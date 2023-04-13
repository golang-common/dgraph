/**
 * @Author: DPY
 * @Description:
 * @File:  schema
 * @Version: 1.0.0
 * @Date: 2021/11/22 10:49
 */

package dgraph

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"github.com/twpayne/go-geom"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// ComparePreds 比较两个谓词列表
// 返回列表第一个为新列表中存在，旧列表中不存在的谓词
// 返回列表第二个为新旧列表中都存在，但属性发生了变化的谓词
func ComparePreds(new, old []Pred) (notExist []Pred, changed []Pred) {
Loop:
	for _, n := range new {
		var exist bool
		for _, o := range old {
			if n.Name == o.Name {
				exist = true
				isSame := compareTwoPred(n, o)
				if !isSame {
					changed = append(changed, n)
				}
				continue Loop
			}
		}
		if !exist {
			notExist = append(notExist, n)
		}
	}
	return
}

// compareTwoPred 比较两个谓词是否相同，相同返回true，不同返回false
func compareTwoPred(new, old Pred) bool {
	if new.Name != old.Name {
		return false
	}
	if new.Count != old.Count ||
		new.Lang != old.Lang ||
		new.Index != old.Index ||
		new.Reverse != old.Reverse ||
		new.List != old.List ||
		new.Upsert != old.Upsert {
		return false
	}
	if new.Type != old.Type {
		return false
	}
	var tokenMap = make(map[string]struct{})
	for _, nt := range new.Tokens {
		tokenMap[nt] = struct{}{}
	}
	for _, ot := range old.Tokens {
		if _, ok := tokenMap[ot]; ok {
			delete(tokenMap, ot)
			continue
		}
		return false
	}
	if len(tokenMap) > 0 {
		return false
	}
	return true
}

// CompareType 给入一个类型，查找类型在全量类型中是否存在
// 如果存在则第一个变量为true
// 如果存在且字段一致则第二个变量为true
func CompareType(new *Type, olist []Type) (exist bool, same bool) {
	var so Type
	for _, o := range olist {
		if o.Name == new.Name {
			exist = true
			so = o
		}
	}
	if exist {
		var fieldMap = make(map[string]struct{})
		same = true
		for _, fa := range new.Fields {
			fieldMap[fa] = struct{}{}
		}
		for _, fb := range so.Fields {
			if _, ok := fieldMap[fb]; ok {
				delete(fieldMap, fb)
				continue
			}
			same = false
		}
		if len(fieldMap) > 0 {
			same = false
		}
	}
	return
}

// ParseStruct 将结构体解析为 Pred 列表和 Type 指针
// 通常用于初始化数据库表结构
func ParseStruct(data TxnData) ([]Pred, []Type, error) {
	if reflect.TypeOf(data.data).Kind() != reflect.Struct {
		return nil, nil, errors.New("invalid object kind")
	}
	typ := reflect.TypeOf(data.data)
	pl, fl, err := parseStructFields(typ)
	if err != nil {
		return nil, nil, err
	}
	var tp []Type
	for _, tstr := range data.Dtype {
		tp = append(tp, Type{Name: tstr, Fields: fl})
	}
	return pl, tp, nil
}

// parseStructFields 解析结构体
func parseStructFields(typ reflect.Type) ([]Pred, []string, error) {
	var (
		plist []Pred
		flist []string
	)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Anonymous {
			pl, fl, err := parseStructFields(field.Type)
			if err != nil {
				return nil, nil, err
			}
			plist = append(plist, pl...)
			flist = append(flist, fl...)
			continue
		}
		pred, err := predByField(field)
		if err != nil {
			return nil, nil, err
		}
		if pred == nil || pred.isFacet {
			continue
		}
		if pred.reversed {
			flist = append(flist, pred.Name)
			continue
		}
		plist = append(plist, *pred)
		flist = append(flist, pred.Name)
	}
	flist = checkDupTypeField(flist)
	return plist, flist, nil
}

func checkDupTypeField(fields []string) []string {
	fmap := make(map[string]string)
	for _, v := range fields {
		if _, ok := fmap[v]; !ok {
			fmap[v] = v
		}
	}
	var r []string
	for _, v := range fmap {
		r = append(r, v)
	}
	return r
}

// predByField 通过结构体field解析出谓词
func predByField(field reflect.StructField) (*Pred, error) {
	var ok bool
	r := new(Pred)
	tag := field.Tag
	r.Name, ok = tag.Lookup(TagDb)
	if !ok || r.Name == uid || r.Name == dtype {
		return nil, nil
	}
	if facetIndex := strings.Index(r.Name, "|"); facetIndex != -1 {
		r.facetName = r.Name[facetIndex+1:]
		r.Name = r.Name[:facetIndex]
		r.isFacet = true
		return r, nil
	}
	// 通过名称是否包含"@"判断是否包含谓词是否包含多语言
	if langIndex := strings.Index(r.Name, "@"); langIndex != -1 {
		r.langVar = r.Name[langIndex+1:]
		r.Lang = true
		r.Name = r.Name[:langIndex]
	}
	// 如果是反谓词，则不进行谓词索引解析操作，但需要添加到类型
	if strings.HasPrefix(r.Name, "~") {
		r.reversed = true
		return r, nil
	}

	//r.Type, ok = tag.Lookup(TagPred)
	//if !ok {
	//	return nil, errors.New(fmt.Sprintf("field %s do not have pred tag", field.Name))
	//}
	//判断谓词类型是否合法
	//switch r.Type {
	//case "default", "uid", "int", "float", "string", "bool", "datetime", "geo", "password":
	//	break
	//default:
	//	return nil, errors.New(fmt.Sprintf("field %s invalid pred type %s", field.Name, r.Type))
	//}
	// 获取谓词索引
	//indexVal := tag.Get("index")
	//indexList := strings.Split(indexVal, ",")
	//// 判断获取到的谓词索引是否合法并填充谓词
	//if len(indexList) > 0 {
	//	for _, v := range indexList {
	//		switch v {
	//		case "count":
	//			r.Count = true
	//		case "list":
	//			r.List = true
	//		case "lang":
	//			r.Lang = true
	//		case "reverse":
	//			r.Reverse = true
	//		case "upsert":
	//			r.Upsert = true
	//		case "default", "int", "float", "bool", "geo", "year", "month", "day", "hour", "hash", "exact", "term", "fulltext", "trigram":
	//			r.Index = true
	//			r.Tokens = append(r.Tokens, v)
	//		}
	//	}
	//}
	return r, nil
}

// Schema 数据结构
type Schema struct {
	Preds []Pred `json:"schema"`
	Types []Type `json:"types"`
}

// SkipSysSchema 忽略dgraph系统自身schema
func (s *Schema) SkipSysSchema() *Schema {
	var (
		r     Schema
		preds []Pred
		types []Type
	)
	for _, p := range s.Preds {
		if strings.HasPrefix(p.Name, "dgraph.") {
			continue
		}
		preds = append(preds, p)
	}
	for _, v := range s.Types {
		if strings.HasPrefix(v.Name, "dgraph.") {
			continue
		}
		types = append(types, v)
	}
	r.Preds = preds
	r.Types = types
	return &r
}

type TypeField struct {
	Pred    Pred
	Unique  bool
	NotNull bool
}

type Type struct {
	Name   string   `json:"name"`
	Fields []string `json:"fields,omitempty"`
}

func (t *Type) UnmarshalJSON(b []byte) error {
	var a struct {
		Name   string `json:"name"`
		Fields []struct {
			Name string `json:"name"`
		} `json:"fields,omitempty"`
	}
	err := json.Unmarshal(b, &a)
	if err != nil {
		return err
	}
	t.Name = a.Name
	for _, v := range a.Fields {
		t.Fields = append(t.Fields, v.Name)
	}
	return nil
}

//type Field struct {
//	Name string `json:"name"`
//}

func (t *Type) Schema() string {
	var (
		preds []string
		r     string
	)
	for _, p := range t.Fields {
		if strings.HasPrefix(p, "~") {
			p = fmt.Sprintf("<%s>", p)
		}
		preds = append(preds, p)
	}
	if len(preds) > 0 {
		r = fmt.Sprintf("type %s{\n\t%s\n}", t.Name, strings.Join(preds, "\n\t"))
	}
	return r
}

// Pred 谓词/边
type Pred struct {
	Name      string   `json:"predicate"`
	Type      string   `json:"type"`
	Index     bool     `json:"index"`
	Tokens    []string `json:"tokenizer"`
	Reverse   bool     `json:"reverse"`
	Count     bool     `json:"count"`
	List      bool     `json:"list"`
	Upsert    bool     `json:"upsert"`
	Lang      bool     `json:"lang"`
	LangVar   string   // 语言种类
	Facet     bool     // 是否为谓词属性
	FacetName string   // 谓词属性
	Reversed  bool     // 否指示为一个反向谓词
}

func (p Pred) String() string {
	return p.Name
}

// Rdf 转换为rdf格式
func (p Pred) Rdf() string {
	var (
		model   = `$name: $type $indices .`
		ptype   = p.Type
		indices []string
	)
	if p.Index && len(p.Name) > 0 {
		indices = append(indices, fmt.Sprintf("@index(%s)", strings.Join(p.Tokens, ",")))
	}
	if p.Reverse {
		indices = append(indices, "@reverse")
	}
	if p.Count {
		indices = append(indices, "@count")
	}
	if p.List {
		ptype = fmt.Sprintf("[%s]", ptype)
	}
	if p.Upsert {
		indices = append(indices, "@upsert")
	}
	if p.Lang {
		indices = append(indices, "@lang")
	}

	replacer := strings.NewReplacer(
		"$name", p.Name,
		"$type", ptype,
		"$indices", strings.Join(indices, " "),
	)
	return replacer.Replace(model)
}

func (p Pred) ToNquad(subject string, value reflect.Value) (*PredNquad, error) {
	var (
		r PredNquad
	)
	if p.Name == "" {
		return nil, errors.New("pred to nquad failed,empty predicate name")
	}
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	if !value.IsValid() || value.IsZero() || strings.HasPrefix(p.Name, "~") {
		return nil, nil
	}
	// 解析数组类型谓词
	if p.List && value.Kind() == reflect.Slice {
		var err error
		for i := 0; i < value.Len(); i++ {
			var sub *PredNquad
			sub, err = p.ToNquad(subject, value.Index(i))
			if err != nil {
				return nil, err
			}
			if sub == nil {
				continue
			}
			r.Sets = append(r.Sets, sub.Sets...)
			r.Dels = append(r.Dels, sub.Dels...)
			r.Facet = append(r.Facet, sub.Facet...)
		}
		if len(r.Sets) > 0 {
			r.Dels = append(r.Dels, &api.NQuad{
				Subject:     subject,
				Predicate:   p.Name,
				ObjectValue: StarNqVal,
			})
		}
		return &r, nil
	}
	// 处理一个谓词属性
	if p.isFacet && p.facetName != "" {
		facet, err := p.ToFacets(value)
		if err != nil {
			return nil, err
		}
		r.Facet = append(r.Facet, facet)
		return &r, nil
	}
	// 解析谓词值
	switch p.Type {
	case "string":
		if value.Kind() == reflect.String {
			r.Sets = append(r.Sets, &api.NQuad{
				Subject:     subject,
				Predicate:   p.Name,
				ObjectValue: nqStringVal(value.String()),
				Lang:        p.langVar,
			})
			return &r, nil
		}
	case "password":
		if value.Kind() == reflect.String {
			r.Sets = append(r.Sets, &api.NQuad{
				Subject:     subject,
				Predicate:   p.Name,
				ObjectValue: nqPasswdVal(value.String()),
			})
			return &r, nil
		}
	case "default":
		if value.Kind() == reflect.String {
			r.Sets = append(r.Sets, &api.NQuad{
				Subject:     subject,
				Predicate:   p.Name,
				ObjectValue: nqDefaultVal(value.String()),
			})
			return &r, nil
		}
	case "int":
		if value.Kind() == reflect.Int ||
			value.Kind() == reflect.Int8 ||
			value.Kind() == reflect.Int16 ||
			value.Kind() == reflect.Int32 ||
			value.Kind() == reflect.Int64 {
			r.Sets = append(r.Sets, &api.NQuad{
				Subject:     subject,
				Predicate:   p.Name,
				ObjectValue: nqIntVal(value.Int()),
			})
			return &r, nil
		}
	case "float":
		if value.Kind() == reflect.Float32 ||
			value.Kind() == reflect.Float64 {
			r.Sets = append(r.Sets, &api.NQuad{
				Subject:     subject,
				Predicate:   p.Name,
				ObjectValue: nqFloatVal(value.Float()),
			})
			return &r, nil
		}
	case "bool":
		if value.Kind() == reflect.Bool {
			r.Sets = append(r.Sets, &api.NQuad{
				Subject:     subject,
				Predicate:   p.Name,
				ObjectValue: nqBoolVal(value.Bool()),
			})
			return &r, nil
		}
	case "datetime":
		if value.Type() == reflect.TypeOf(time.Time{}) {
			r.Sets = append(r.Sets, &api.NQuad{
				Subject:     subject,
				Predicate:   p.Name,
				ObjectValue: nqTimeVal(value.Interface().(time.Time)),
			})
			return &r, nil
		}
	case "geo":
		if v, ok := value.Interface().(geom.T); ok {
			r.Sets = append(r.Sets, &api.NQuad{
				Subject:     subject,
				Predicate:   p.Name,
				ObjectValue: nqGeoVal(v),
			})
			return &r, nil
		}
	case "uid":
		if value.Kind() == reflect.Struct {
			uidf := value.FieldByName(uidField)
			sSubject := uidf.String()
			if sSubject != "" {
				nquad := &api.NQuad{
					Subject:   subject,
					Predicate: p.Name,
					ObjectId:  sSubject,
				}
				// 解析链接谓词属性
				for i := 0; i < value.NumField(); i++ {
					fv := value.Field(i)
					ft := value.Type().Field(i)
					subPred, err := predByField(ft)
					if err != nil {
						return nil, err
					}
					if subPred == nil {
						continue
					}
					sub, err := subPred.ToNquad(sSubject, fv)
					if err != nil {
						return nil, err
					}
					if sub == nil {
						continue
					}
					if sub.Facet != nil {
						nquad.Facets = append(nquad.Facets, sub.Facet...)
					}
					if len(sub.Sets) > 0 {
						r.Sets = append(r.Sets, sub.Sets...)
					}
					if len(sub.Dels) > 0 {
						r.Dels = append(r.Dels, sub.Dels...)
					}
				}
				r.Sets = append(r.Sets, nquad)
				return &r, nil
			}
			return &r, nil
		}
	}
	return nil, errors.New(fmt.Sprintf("db claimed its type is %s,but real type is %s", p.Type, value.Kind().String()))
}

func (p Pred) ToFacets(value reflect.Value) (*api.Facet, error) {
	var r *api.Facet
	if p.facetName == "" {
		return nil, errors.New("pred to facets failed,empty facet name")
	}
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	if !value.IsValid() || value.IsZero() {
		return nil, nil
	}

	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var b = make([]byte, 8)
		intVal := value.Int()
		binary.LittleEndian.PutUint64(b, uint64(intVal))
		r = &api.Facet{
			Key:     p.facetName,
			Value:   b,
			ValType: api.Facet_INT,
		}
		return r, nil
	case reflect.Bool:
		r = &api.Facet{
			Key:     p.facetName,
			Value:   []byte(strconv.FormatBool(value.Bool())),
			ValType: api.Facet_BOOL,
		}
		return r, nil
	case reflect.String:
		r = &api.Facet{
			Key:   p.facetName,
			Value: []byte(value.String()),
		}
		return r, nil
	case reflect.Float32, reflect.Float64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(value.Float()))
		r = &api.Facet{
			Key:     p.facetName,
			Value:   buf,
			ValType: api.Facet_FLOAT,
		}
		return r, nil
	case reflect.Struct:
		ifVal := value.Interface()
		if timeVal, ok := ifVal.(time.Time); ok {
			r = &api.Facet{
				Key:     p.facetName,
				Value:   []byte(timeVal.String()),
				ValType: api.Facet_DATETIME,
			}
			return r, nil
		}
	}
	return nil, errors.New("pred to facets failed, wrong facet datatype")
}

type PredNquad struct {
	Sets  []*api.NQuad
	Dels  []*api.NQuad
	Facet []*api.Facet
}

func ReformPredicate(obj interface{}, predName string) string {
	if obj == nil {
		return predName
	}
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	typ := val.Type()
	if typ.Kind() != reflect.Struct {
		return predName
	}
	for i := 0; i < typ.NumField(); i++ {
		subField := typ.Field(i)
		subTag := subField.Tag
		dbSubTag := subTag.Get(Db)
		jsonSubl := strings.Split(subTag.Get(Json), ",")
		var jsonSubTag string
		if len(jsonSubl) > 0 {
			jsonSubTag = jsonSubl[0]
		}
		if predName == dbSubTag {
			return dbSubTag
		}
		if predName == jsonSubTag && jsonSubTag != "" && dbSubTag != "" {
			return dbSubTag
		}
	}
	return predName
}
