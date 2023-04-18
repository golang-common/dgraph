package dgraph

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"reflect"
	"strings"
)

type TypeDef interface {
	Schema() SchemaType
	GetName() string
	GetFields() map[string]Pred
	GetRevPreds() []SchemaPred
	CheckData() error
}

// Type 类型字段
// Name - 类型名称
// Fields - 类型列表
// RevPreds - 指向该类型(Uid)的谓词列表
type Type[T any] struct {
	Name      string          `json:"name"`
	DataModel T               `json:"dataModel,omitempty"`
	Fields    map[string]Pred `json:"fields,omitempty"`
	RevPreds  []SchemaPred    `json:"revPreds,omitempty"`
}

func (t Type[T]) GetName() string {
	return t.Name
}

func (t Type[T]) GetFields() map[string]Pred {
	return t.Fields
}

func (t Type[T]) GetRevPreds() []SchemaPred {
	return t.RevPreds
}

// Schema 将类型转换为操作RDF，用于增加表
func (t Type[T]) Schema() SchemaType {
	var r = SchemaType{Name: t.Name}
	for _, fields := range t.Fields {
		if !fields.Reversed {
			r.Fields = append(r.Fields, SchemaTypeField{Name: fields.Name})
		}
	}
	return r
}

func (t Type[T]) NquadType(uid string) *api.NQuad {
	return &api.NQuad{
		Subject:     uid,
		Predicate:   "dgraph.type",
		ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: t.Name}},
	}
}

func (t Type[T]) Nquad(uid string, data T) ([]*api.NQuad, error) {
	var (
		r   []*api.NQuad
		val = reflect.ValueOf(data)
		typ = val.Type()
	)
	if uid == "" {
		return nil, errors.New("empty uid value")
	}
	for i := 0; i < val.NumField(); i++ {
		subType := typ.Field(i)
		subVal := val.Field(i)
		if subType.Name == Uid {
			continue
		}
		if !subVal.IsValid() || subVal.IsZero() {
			continue
		}
		if subVal.Kind() == reflect.Pointer {
			if subVal.IsNil() {
				continue
			}
			subVal = subVal.Elem()
		}
		nquadList, e := t.fieldNquad(
			uid,
			t.Fields[subType.Name],
			subVal.Interface(),
		)
		if e != nil {
			return nil, e
		}
		r = append(r, nquadList...)
	}
	return r, nil
}

func (t Type[T]) nquadAll(uid string) *api.NQuad {
	return &api.NQuad{
		Subject:     uid,
		Predicate:   StarAll,
		ObjectValue: &api.Value{Val: &api.Value_DefaultVal{DefaultVal: StarAll}},
	}
}

func (t Type[T]) fieldNquad(uid string, pred Pred, data any) ([]*api.NQuad, error) {
	var (
		r   []*api.NQuad
		val = reflect.ValueOf(data)
		typ = val.Type()
	)
	// 如果是切片类型的递归计算
	if typ.Kind() == reflect.Slice {
		for i := 0; i < val.Len(); i++ {
			sub, err := t.fieldNquad(uid, pred, val.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			r = append(r, sub...)
		}
		return r, nil
	}
	// 解析 api.Value 值
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	apival, objid, err := pred.Type.Value(data)
	if err != nil {
		return nil, err
	}
	nquad := &api.NQuad{Subject: uid, Predicate: pred.Name, ObjectId: objid, ObjectValue: apival}
	// 如果值是Uid类型，则解析边属性
	if pred.Type == TypeUid {
		for k, facet := range pred.Facets {
			facetVal := val.FieldByName(k)
			if !facetVal.IsValid() || facetVal.IsZero() {
				continue
			}
			f, err := facet.Facet(facetVal.Interface())
			if err != nil {
				return nil, err
			}
			nquad.Facets = append(nquad.Facets, &f)
		}
	}
	r = append(r, nquad)
	return r, nil
}

// CheckData 检查结构体值类型是否与类型定义匹配
func (t Type[T]) CheckData() error {
	val := reflect.ValueOf(t.DataModel)
	// 检查结构体中的字段是否与传入的字典匹配
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		fieldType := typ.Field(i)
		if fieldType.Name == Uid {
			continue
		}
		db := fieldType.Tag.Get(Db)
		// 忽略边
		if strings.Contains(db, "|") {
			continue
		}
		v, ok := t.Fields[fieldType.Name]
		if !ok {
			return errors.New(fmt.Sprintf("type [%s] check failed,struct field %s not find in predicate map key", typ.Name(), typ.Field(i).Name))
		}
		if db != v.Name {
			return errors.New(fmt.Sprintf("type [%s] check failed, predicate name %s does not match the struct db tag %s", typ.Name(), v.Name, db))
		}
		err := t.checkStructField(v, typ.Field(i))
		if err != nil {
			return err
		}
	}
	return nil
}

func (t Type[T]) checkStructField(pred Pred, field reflect.StructField) error {
	var (
		typ     = field.Type
		matched bool // 谓词类型是否与数据类型匹配
		islist  bool // 谓词是否为列表
	)
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
		islist = true
	}
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Name() {
	case "string":
		matched = pred.Type == "string"
	case "int", "int8", "int16", "int32", "int64":
		matched = pred.Type == "int"
	case "float32", "float64":
		matched = pred.Type == "float"
	case "bool":
		matched = pred.Type == "bool"
	case "time.Time":
		matched = pred.Type == "datetime"
	case "geom.T":
		matched = pred.Type == "geo"
	default:
		if typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		if typ.Kind() == reflect.Struct {
			matched = pred.Type == "uid"
		}
	}
	if islist != pred.List {
		return errors.New(fmt.Sprintf("predicate %s declear its list=%t, but field %s is not a list", pred.Name, pred.List, field.Name))
	}
	if !matched {
		return errors.New(fmt.Sprintf("predicate type %s, is not match field data type %s", pred.Type, field.Type.Name()))
	}
	return nil
}
