package dgraph

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
	"golang.org/x/crypto/bcrypt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type DataOperation int

const (
	ONone      DataOperation = 0b0    // 不进行任何操作
	OEmitempty DataOperation = 0b1    // 忽略空值, 没有此值则不忽略空值
	ONullAll   DataOperation = 0b01   // 对空值赋值ALL
	OAnyAll    DataOperation = 0b001  // 对所有值赋值ALL
	OSetUid    DataOperation = 0b0001 // 设置Uid值，适用于新增操作
)

func NewData[T any](data T, typ Type) *Data[T] {
	return &Data[T]{
		Data: data,
		Type: typ,
	}
}

type Data[T any] struct {
	Data T
	Type Type
}

// QueryFilters TODO: 解析出查询过滤参数
func (d Data[T]) QueryFilters() []string {

	return nil
}

// Nquads 用户新建的数据库操作数，将数据解析为dgraph变更rdf，用户发起数据库请求
func (d Data[T]) Nquads(op DataOperation) ([]*api.NQuad, error) {
	err := d.check()
	if err != nil {
		return nil, err
	}
	val := reflect.ValueOf(d.Data)
	if op&OSetUid != 0 {
		val.Elem().FieldByName(Uid).SetString(d.addUid())
	}
	id := val.FieldByName(Uid).String()
	if id == "" {
		return nil, errors.New("error empty uid")
	}
	val = val.Elem()
	var r []*api.NQuad
	// 如果设置了所有标记，则只传递
	if op&OAnyAll != 0 {
		r = append(r, &api.NQuad{
			Subject:     id,
			Predicate:   StarAll,
			ObjectValue: &api.Value{Val: &api.Value_DefaultVal{DefaultVal: StarAll}},
		})
		return r, nil
	}
	for i := 0; i < val.NumField(); i++ {
		pred := d.Type.Fields[val.Type().Field(i).Name]
		// 跳过Uid值
		if val.Type().Field(i).Name == Uid {
			continue
		}
		// 如果指定了忽略空，则忽略空值
		if op&OEmitempty != 0 && (!val.IsValid() || val.IsZero()) {
			continue
		}
		// 如果指定了为空值指定了ALL，则跳过解析具体值，并返回
		if op&ONullAll != 0 && (!val.IsValid() || val.IsZero()) {
			r = append(r, &api.NQuad{
				Subject:     id,
				Predicate:   pred.Name,
				ObjectValue: &api.Value{Val: &api.Value_DefaultVal{DefaultVal: StarAll}},
			})
			continue
		}
		// 分析值
		nquadList, e := d.fieldNquad(
			id,
			pred,
			val.Field(i).Interface(),
		)
		if e != nil {
			return nil, e
		}
		r = append(r, nquadList...)
	}
	return r, nil
}

// fieldNquad 将值解析为Nquad,传入的值都是data结构的顶层
// 所以顶层结构体可以忽略Facet类型值,但是需要解析子结构的facet值
// 这一步不处理空值，空值在调用侧已经被处理
func (d Data[T]) fieldNquad(uid string, pred Pred, data any) ([]*api.NQuad, error) {
	var (
		r   []*api.NQuad
		val = reflect.ValueOf(data)
		typ = val.Type()
	)
	// 如果是切片类型的递归计算
	if typ.Kind() == reflect.Slice {
		for i := 0; i < typ.Len(); i++ {
			sub, err := d.fieldNquad(uid, pred, val.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			r = append(r, sub...)
		}
		return r, nil
	}
	// 将值解指针
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	// 开始处理具体值,
	switch pred.Type {
	case "default":
		if v, ok := data.(string); ok {
			r = append(r, &api.NQuad{Subject: uid, Predicate: pred.Name, ObjectValue: &api.Value{Val: &api.Value_DefaultVal{DefaultVal: v}}})
		}
	case "string":
		if v, ok := data.(string); ok {
			r = append(r, &api.NQuad{Subject: uid, Predicate: pred.Name, ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: v}}})
		}
	case "password":
		if v, ok := data.(string); ok {
			b, err := bcrypt.GenerateFromPassword([]byte(v), bcrypt.MinCost)
			if err != nil {
				return nil, err
			}
			r = append(r, &api.NQuad{Subject: uid, Predicate: pred.Name, ObjectValue: &api.Value{Val: &api.Value_PasswordVal{PasswordVal: string(b)}}})
		}
	case "int":
		if v, ok := numToInt64(data); ok {
			r = append(r, &api.NQuad{Subject: uid, Predicate: pred.Name, ObjectValue: &api.Value{Val: &api.Value_IntVal{IntVal: v}}})
		}
	case "float":
		if v, ok := data.(float32); ok {
			r = append(r, &api.NQuad{Subject: uid, Predicate: pred.Name, ObjectValue: &api.Value{Val: &api.Value_DoubleVal{DoubleVal: float64(v)}}})
		}
		if v, ok := data.(float64); ok {
			r = append(r, &api.NQuad{Subject: uid, Predicate: pred.Name, ObjectValue: &api.Value{Val: &api.Value_DoubleVal{DoubleVal: v}}})
		}
	case "datetime":
		if v, ok := data.(time.Time); ok {
			timeBinary, err := v.MarshalBinary()
			if err != nil {
				return nil, err
			}
			r = append(r, &api.NQuad{Subject: uid, Predicate: pred.Name, ObjectValue: &api.Value{Val: &api.Value_DatetimeVal{DatetimeVal: timeBinary}}})
		}
	case "geo":
		if v, ok := data.(geom.T); ok {
			geomBinary, err := geojson.Marshal(v)
			if err != nil {
				return nil, err
			}
			r = append(r, &api.NQuad{Subject: uid, Predicate: pred.Name, ObjectValue: &api.Value{Val: &api.Value_GeoVal{GeoVal: geomBinary}}})
		}
	case "uid":
		// 处理uid映射
		uidVal := reflect.ValueOf(data)
		subId := uidVal.FieldByName(Uid).String()
		if subId == "" {
			break
		}
		nquad := &api.NQuad{Subject: uid, Predicate: pred.Name, ObjectId: subId}
		// 边属性解析
		for i := 0; i < uidVal.NumField(); i++ {
			dbTag := uidVal.Type().Field(i).Tag.Get(Db)
			if !strings.Contains(dbTag, "|") {
				continue
			}
			if !uidVal.Field(i).IsValid() || uidVal.Field(i).IsZero() {
				continue
			}
			tl := strings.Split(dbTag, "|")
			if len(tl) != 2 {
				continue
			}
			facet, err := d.parseFacet(tl[1], uidVal.Field(i))
			if err != nil {
				return nil, err
			}
			nquad.Facets = append(nquad.Facets, &facet)
		}
	}
	return r, nil
}

// parseFacet 将单个值解析为边
func (d Data[T]) parseFacet(name string, data any) (api.Facet, error) {
	var (
		val = reflect.ValueOf(data)
	)
	switch data.(type) {
	case int, int8, int16, int32, int64:
		var b = make([]byte, 8)
		intVal := val.Int()
		binary.LittleEndian.PutUint64(b, uint64(intVal))
		return api.Facet{
			Key:     name,
			Value:   b,
			ValType: api.Facet_INT,
		}, nil
	case uint, uint8, uint16, uint32, uint64:
		var b = make([]byte, 8)
		intVal := int64(val.Uint())
		binary.LittleEndian.PutUint64(b, uint64(intVal))
		return api.Facet{
			Key:     name,
			Value:   b,
			ValType: api.Facet_INT,
		}, nil
	case bool:
		return api.Facet{
			Key:     name,
			Value:   []byte(strconv.FormatBool(val.Bool())),
			ValType: api.Facet_BOOL,
		}, nil
	case string:
		return api.Facet{
			Key:     name,
			Value:   []byte(val.String()),
			ValType: api.Facet_STRING,
		}, nil
	case float32, float64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(val.Float()))
		return api.Facet{
			Key:     name,
			Value:   buf,
			ValType: api.Facet_FLOAT,
		}, nil
	case time.Time:
		return api.Facet{
			Key:     name,
			Value:   []byte(data.(time.Time).String()),
			ValType: api.Facet_DATETIME,
		}, nil
	default:
		return api.Facet{}, errors.New("error facet datatype")
	}
}

// addUid 生成随机uid用于增加操作
func (d Data[T]) addUid() string {
	return fmt.Sprintf("_:%d", rand.Int63())
}

// check 检查数据和给定的谓词map是否匹配
func (d Data[T]) check() error {
	val := reflect.ValueOf(d.Data)
	// 必须是指针类型数据，因为后续可能设置adduid
	if val.Kind() != reflect.Pointer {
		return errors.New("data must be a pointer of value")
	}
	// 指针后必须是结构体类型
	if val.Elem().Kind() != reflect.Struct {
		return errors.New("underground data must be a struct of kind")
	}
	// 检查结构体中的字段是否与传入的字典匹配
	typ := val.Elem().Type()
	for i := 0; i < typ.NumField(); i++ {
		db := typ.Field(i).Tag.Get(Db)
		v, ok := d.Type.Fields[typ.Field(i).Name]
		if !ok {
			return errors.New(fmt.Sprintf("struct filed %s not find in predicate map key", typ.Field(i).Name))
		}
		if strings.Split(db, "|")[0] != v.Name {
			return errors.New(fmt.Sprintf("predicate name %s does not match the struct db tag %s", v.Name, db))
		}
		err := d.checkStructField(v, typ.Field(i))
		if err != nil {
			return err
		}
	}
	return nil
}

// checkStructField 检查谓词和结构体类型是否匹配
func (d Data[T]) checkStructField(pred Pred, field reflect.StructField) error {
	var (
		t       = field.Type
		matched bool // 谓词类型是否与数据类型匹配
		islist  bool // 谓词是否为列表
	)
	if t.Kind() == reflect.Slice {
		t = t.Elem()
		islist = true
	}
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch field.Type.Name() {
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
		if field.Type.Kind() == reflect.Struct {
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
