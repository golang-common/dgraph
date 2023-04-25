package dgraph

import (
	"encoding/binary"
	"fmt"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
	"golang.org/x/crypto/bcrypt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Pred 谓词/边
// Facets - 谓词对应的边，key为结构体字段名
type Pred struct {
	SchemaPred
	Facets   map[string]Facet
	LangType string // 语言种类
	Reversed bool   // 否指示为一个反向谓词
	Pri      bool   // 主键约束(唯一 + 非空)
	Unique   bool   // 唯一约束
	NotNull  bool   // 非空约束
}

func (p Pred) String() string {
	return p.Name
}

func (p Pred) Schema() SchemaPred {
	return p.SchemaPred
}

func (p Pred) Nquad(uid string, data any) ([]*api.NQuad, error) {
	var (
		r   []*api.NQuad
		val = reflect.ValueOf(data)
		typ = val.Type()
	)
	if typ.Kind() == reflect.Slice {
		for i := 0; i < typ.Len(); i++ {
			subVal := val.Index(i)
			if !subVal.IsValid() || subVal.IsZero() {
				continue
			}
			subNquad, err := p.singleNquad(uid, val.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			r = append(r, subNquad)
		}
		return r, nil
	}
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	if !val.IsValid() || val.IsZero() {
		return r, nil
	}
	subNquad, err := p.singleNquad(uid, val.Interface())
	if err != nil {
		return nil, err
	}
	r = append(r, subNquad)
	return r, nil
}

// QueryFilter 解析结构体单个值(去切片后)的过滤和边,start 参数表示是否为入口解析
func (p Pred) QueryFilter(data any) PredFilter {
	var r PredFilter
	val, ok := checkAndElem(reflect.ValueOf(data))
	if !ok {
		return r
	}
	switch p.Type {
	case TypeString:
		if p.List && val.Kind() == reflect.Slice && val.Len() > 0 {
			r.MainFilter = fmt.Sprintf(`eq(%s,["%s"])`, p.Name, strings.Join(data.([]string), `","`))
		}
		if val.Kind() == reflect.String {
			v := data.(string)
			if v != "" {
				r.MainFilter = fmt.Sprintf(`eq(%s,"%s")`, p.Name, v)
			}
		}
	case TypeInt:
		if p.List && val.Kind() == reflect.Slice && val.Len() > 0 {
			var rl []string
			for i := 0; i < val.Len(); i++ {
				sub := val.Index(i)
				if val.CanInt() {
					rl = append(rl, fmt.Sprintf("%d", sub.Int()))
				}
			}
			if len(rl) > 0 {
				r.MainFilter = fmt.Sprintf(`eq(%s,[%s])`, p.Name, strings.Join(rl, ","))
			}
		} else if val.CanInt() {
			r.MainFilter = fmt.Sprintf("eq(%s,%d)", p.Name, val.Int())
		}
	case TypeFloat:
		if p.List && val.Kind() == reflect.Slice && val.Len() > 0 {
			var rl []string
			for i := 0; i < val.Len(); i++ {
				sub := val.Index(i)
				if val.CanFloat() {
					rl = append(rl, fmt.Sprintf("%f", sub.Float()))
				}
			}
			if len(rl) > 0 {
				r.MainFilter = fmt.Sprintf(`eq(%s,[%s])`, p.Name, strings.Join(rl, ","))
			}
		} else if val.CanFloat() {
			r.MainFilter = fmt.Sprintf("eq(%s,%f)", p.Name, val.Float())
		}
	case TypeBool:
		if val.Kind() == reflect.Bool {
			r.MainFilter = fmt.Sprintf("eq(%s,%t)", p.Name, val.Bool())
		}
	case TypeUid:
		if p.List && val.Kind() == reflect.Slice && val.Type().Elem().Kind() == reflect.Struct && val.Len() > 0 {
			val = val.Index(0) // 如果是列表则只查询第一个元素
		}
		if val.Kind() != reflect.Struct {
			break
		}
		// 主UID过滤项
		if subId := val.FieldByName(Uid).String(); subId != "" {
			r.MainFilter = fmt.Sprintf("uid_in(%s,%s)", p.Name, subId)
		}
		// 边解析
		var facetList []string
		for k, v := range p.Facets {
			r.HasFacet = true
			if facetVal := v.QVal(val.FieldByName(k).Interface()); facetVal != "" {
				facetList = append(facetList, facetVal)
			}
		}
		if len(facetList) > 0 {
			r.FacetFilter = strings.Join(facetList, " AND ")
		}
		// 子谓词解析,仅支持基础类型
		var subFilterList []string
		for i := 0; i < val.Type().NumField(); i++ {
			var (
				sok bool
				sub = val.Field(i)
				tag = val.Type().Field(i).Tag.Get(Db)
			)
			sub, sok = checkAndElem(reflect.ValueOf(data))
			if !sok {
				continue
			}
			if tag == "" || strings.Contains(tag, "|") {
				continue
			}
			sval := sub.Interface()
			switch sval.(type) {
			case string:
				subFilterList = append(subFilterList, fmt.Sprintf(`eq(%s,%s)`, tag, sval.(string)))
			case []string:
				subFilterList = append(subFilterList, fmt.Sprintf(`eq(%s,["%s"])`, tag, strings.Join(sval.([]string), `","`)))
			case int, float64:
				subFilterList = append(subFilterList, fmt.Sprintf(`eq(%s,%v)`, tag, sval))
			case []int, []float64:
				subFilterList = append(subFilterList, fmt.Sprintf(`eq(%s,%v)`, tag,
					strings.Join(strings.Fields(fmt.Sprint(sval)), ",")))
			}
		}
		if len(subFilterList) > 0 {
			r.SubFilter = strings.Join(subFilterList, " AND ")
		}
	}
	return r
}

// singleNquad 解析单个值
func (p Pred) singleNquad(uid string, data any) (*api.NQuad, error) {
	switch p.Type {
	case "default":
		if v, ok := data.(string); ok {
			return &api.NQuad{Subject: uid, Predicate: p.Name, ObjectValue: &api.Value{Val: &api.Value_DefaultVal{DefaultVal: v}}}, nil
		}
		return nil, fmt.Errorf("error default value %v", data)
	case "string":
		if v, ok := data.(string); ok {
			return &api.NQuad{Subject: uid, Predicate: p.Name, ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: v}}}, nil
		}
		return nil, fmt.Errorf("error string value %v", data)
	case "password":
		if v, ok := data.(string); ok {
			b, err := bcrypt.GenerateFromPassword([]byte(v), bcrypt.MinCost)
			if err != nil {
				return nil, err
			}
			return &api.NQuad{Subject: uid, Predicate: p.Name, ObjectValue: &api.Value{Val: &api.Value_PasswordVal{PasswordVal: string(b)}}}, nil
		}
		return nil, fmt.Errorf("error password value %v", data)
	case "int":
		if v, ok := numToInt64(data); ok {
			return &api.NQuad{Subject: uid, Predicate: p.Name, ObjectValue: &api.Value{Val: &api.Value_IntVal{IntVal: v}}}, nil
		}
		return nil, fmt.Errorf("error int value %v", data)
	case "float":
		if v, ok := data.(float32); ok {
			return &api.NQuad{Subject: uid, Predicate: p.Name, ObjectValue: &api.Value{Val: &api.Value_DoubleVal{DoubleVal: float64(v)}}}, nil
		}
		if v, ok := data.(float64); ok {
			return &api.NQuad{Subject: uid, Predicate: p.Name, ObjectValue: &api.Value{Val: &api.Value_DoubleVal{DoubleVal: v}}}, nil
		}
		return nil, fmt.Errorf("error float value %v", data)
	case "datetime":
		if v, ok := data.(time.Time); ok {
			timeBinary, err := v.MarshalBinary()
			if err != nil {
				return nil, err
			}
			return &api.NQuad{Subject: uid, Predicate: p.Name, ObjectValue: &api.Value{Val: &api.Value_DatetimeVal{DatetimeVal: timeBinary}}}, nil
		}
		return nil, fmt.Errorf("error datetime value %v", data)
	case "geo":
		if v, ok := data.(geom.T); ok {
			geomBinary, err := geojson.Marshal(v)
			if err != nil {
				return nil, err
			}
			return &api.NQuad{Subject: uid, Predicate: p.Name, ObjectValue: &api.Value{Val: &api.Value_GeoVal{GeoVal: geomBinary}}}, nil
		}
		return nil, fmt.Errorf("error geo value %v", data)
	case "uid":
		// 处理uid映射
		var n api.NQuad
		uidVal := reflect.ValueOf(data)
		if uidVal.Kind() == reflect.Struct {
			subId := uidVal.FieldByName(Uid).String()
			if subId == "" {
				return nil, fmt.Errorf("error uid value %v", data)
			}
			n = api.NQuad{Subject: uid, Predicate: p.Name, ObjectId: subId}
		}
		for k, facet := range p.Facets {
			subval := uidVal.FieldByName(k)
			if !subval.IsValid() || subval.IsZero() || subval.IsNil() {
				continue
			}
			f, err := facet.Facet(subval.Interface())
			if err != nil {
				return nil, err
			}
			n.Facets = append(n.Facets, &f)
		}
		return &n, nil
	default:
		return nil, fmt.Errorf("error predicate type %s", p.Type)
	}
}

type Facet struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func (f Facet) QVal(data any) string {
	val := reflect.ValueOf(data)
	if !val.IsValid() || val.IsZero() {
		return ""
	}
	switch f.Type {
	case "string", "datetime":
		return fmt.Sprintf(`eq(%s,"%s")`, f.Name, val.String())
	case "bool":
		return fmt.Sprintf(`eq(%s,%t)`, f.Name, val.Bool())
	case "int":
		return fmt.Sprintf(`eq(%s,%d)`, f.Name, val.Int())
	case "float":
		return fmt.Sprintf(`eq(%s,%f)`, f.Name, val.Float())
	}
	return ""
}

func (f Facet) Facet(data any) (api.Facet, error) {
	val := reflect.ValueOf(data)
	switch data.(type) {
	case int, int8, int16, int32, int64:
		var b = make([]byte, 8)
		intVal := val.Int()
		binary.LittleEndian.PutUint64(b, uint64(intVal))
		return api.Facet{
			Key:     f.Name,
			Value:   b,
			ValType: api.Facet_INT,
		}, nil
	case uint, uint8, uint16, uint32, uint64:
		var b = make([]byte, 8)
		intVal := int64(val.Uint())
		binary.LittleEndian.PutUint64(b, uint64(intVal))
		return api.Facet{
			Key:     f.Name,
			Value:   b,
			ValType: api.Facet_INT,
		}, nil
	case bool:
		return api.Facet{
			Key:     f.Name,
			Value:   []byte(strconv.FormatBool(val.Bool())),
			ValType: api.Facet_BOOL,
		}, nil
	case string:
		return api.Facet{
			Key:     f.Name,
			Value:   []byte(val.String()),
			ValType: api.Facet_STRING,
		}, nil
	case float32, float64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(val.Float()))
		return api.Facet{
			Key:     f.Name,
			Value:   buf,
			ValType: api.Facet_FLOAT,
		}, nil
	case time.Time:
		return api.Facet{
			Key:     f.Name,
			Value:   []byte(data.(time.Time).String()),
			ValType: api.Facet_DATETIME,
		}, nil
	default:
		return api.Facet{}, fmt.Errorf("error facet datatype %s", val.Type())
	}
}

type PredFilter struct {
	MainFilter  string
	FacetFilter string
	HasFacet    bool
	SubFilter   string
}
