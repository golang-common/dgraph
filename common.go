package dgraph

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
	"golang.org/x/crypto/bcrypt"
	"reflect"
	"strings"
	"time"
)

const (
	Db      = `db`
	Json    = `json`
	Uid     = "Uid"
	StarAll = `_STAR_ALL`

	TypeString   PredType = "string"
	TypeDefault  PredType = "default"
	TypePassword PredType = "password"
	TypeBool     PredType = "bool"
	TypeInt      PredType = "int"
	TypeFloat    PredType = "float"
	TypeDatetime PredType = "datetime"
	TypeGeo      PredType = "geo"
	TypeUid      PredType = "uid"
)

type PredType string

func (p PredType) String() string {
	return string(p)
}

// QueryValue 查询时使用的字符串格式
func (p PredType) QueryValue(data any) (string, error) {
	val := reflect.ValueOf(data)
	if !val.IsValid() || val.IsZero() {
		return "", nil
	}
	if val.Kind() == reflect.Slice {
		var qlist []string
		for i := 0; i < val.Len(); i++ {
			q, err := p.QueryValue(val.Index(i).Interface())
			if err != nil {
				return "", err
			}
			if q != "" {
				qlist = append(qlist, q)
			}
		}
		if len(qlist) > 0 {
			return fmt.Sprintf(`[%s]`, strings.Join(qlist, ",")), nil
		}
		return "", nil
	}
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	switch p {
	case TypeString:
		if v, ok := data.(string); ok {
			return fmt.Sprintf(`"%s"`, v), nil
		}
	case TypeInt:
		if v, ok := numToInt64(data); ok {
			return fmt.Sprintf("%d", v), nil
		}
	case TypeBool:
		if v, ok := data.(bool); ok {
			return fmt.Sprintf("%t", v), nil
		}
	case TypeFloat:
		if v, ok := data.(float32); ok {
			return fmt.Sprintf("%f", v), nil
		}
		if v, ok := data.(float64); ok {
			return fmt.Sprintf("%f", v), nil
		}
	case TypeDatetime:
		if v, ok := data.(time.Time); ok {
			return v.String(), nil
		}
	}
	return "", fmt.Errorf("unsupport predicate type %s, datatype %s", p, reflect.TypeOf(data))
}

// Value 将 data 转换为dgraph底层数据结构，用于变更请求
// 其中 data 为结构体中的字段单值(不含切片，切片已在外部遍历)
func (p PredType) Value(data any) (*api.Value, string, error) {
	switch p {
	case TypeString:
		if v, ok := data.(string); ok {
			return &api.Value{Val: &api.Value_StrVal{StrVal: v}}, "", nil
		}
	case TypeDefault:
		if v, ok := data.(string); ok {
			return &api.Value{Val: &api.Value_DefaultVal{DefaultVal: v}}, "", nil
		}
	case TypePassword:
		if v, ok := data.(string); ok {
			b, err := bcrypt.GenerateFromPassword([]byte(v), bcrypt.MinCost)
			if err != nil {
				return nil, "", err
			}
			return &api.Value{Val: &api.Value_PasswordVal{PasswordVal: string(b)}}, "", nil
		}
	case TypeBool:
		if v, ok := data.(bool); ok {
			return &api.Value{Val: &api.Value_BoolVal{BoolVal: v}}, "", nil
		}
	case TypeInt:
		if v, ok := numToInt64(data); ok {
			return &api.Value{Val: &api.Value_IntVal{IntVal: v}}, "", nil
		}
	case TypeFloat:
		if v, ok := data.(float32); ok {
			return &api.Value{Val: &api.Value_DoubleVal{DoubleVal: float64(v)}}, "", nil
		}
		if v, ok := data.(float64); ok {
			return &api.Value{Val: &api.Value_DoubleVal{DoubleVal: v}}, "", nil
		}
	case TypeDatetime:
		if v, ok := data.(time.Time); ok {
			timeBinary, err := v.MarshalBinary()
			if err != nil {
				return nil, "", err
			}
			return &api.Value{Val: &api.Value_DatetimeVal{DatetimeVal: timeBinary}}, "", nil
		}
	case TypeGeo:
		if v, ok := data.(geom.T); ok {
			geomBinary, err := geojson.Marshal(v)
			if err != nil {
				return nil, "", err
			}
			return &api.Value{Val: &api.Value_GeoVal{GeoVal: geomBinary}}, "", nil
		}
	case TypeUid:
		val := reflect.ValueOf(data)
		subId := val.FieldByName(Uid).String()
		if subId == "" {
			return nil, "", errors.New("empty uid value")
		}
		return nil, subId, nil
	}
	return nil, "", fmt.Errorf("error predicate type %s, datatype %s", p, reflect.TypeOf(data))
}

func numToInt64(num any) (int64, bool) {
	switch num.(type) {
	case int:
		return int64(num.(int)), true
	case int8:
		return int64(num.(int8)), true
	case int16:
		return int64(num.(int16)), true
	case int32:
		return int64(num.(int32)), true
	case int64:
		return num.(int64), true
	case uint:
		return int64(num.(uint)), true
	case uint8:
		return int64(num.(uint8)), true
	case uint16:
		return int64(num.(uint16)), true
	case uint32:
		return int64(num.(uint32)), true
	case uint64:
		return int64(num.(uint64)), true
	default:
		return 0, false
	}
}

func floatToFloat64(f any) (float64, bool) {
	switch f.(type) {
	case float32:
		return float64(f.(float32)), true
	case float64:
		return f.(float64), true
	default:
		return 0, false
	}
}

func checkAndElem(value reflect.Value) (reflect.Value, bool) {
	if !value.IsValid() || value.IsZero() {
		return value, false
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return value, false
		}
		value = value.Elem()
	}
	return value, true
}
