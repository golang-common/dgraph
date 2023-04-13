/**
 * @Author: DPY
 * @Description: 变更快捷操作
 * @File:  nquad
 * @Version: 1.0.0
 * @Date: 2021/12/2 09:23
 */

package dgraph

import (
	"errors"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
	"golang.org/x/crypto/bcrypt"
	"reflect"
	"time"
)

const (
	StarAll  = `_STAR_ALL`
	typePred = `dgraph.type`
	uidField = `Uid`
	uid      = "uid"
	dtype    = "dgraph.type"
)

var StarNqVal = &api.Value{Val: &api.Value_DefaultVal{DefaultVal: StarAll}}

// SetNquad 设置值
// 该方法支持单层变更，不支持向下修改，如:传入User结构，不能修改User->Group内的属性，只能修改User所属组
// obj结构中的uid必须非空，如果是增加操作使用者需要自行构建"_:[id]"形式的新增字段
func SetNquad(data *TxnData, op int) ([]*api.NQuad, []*api.NQuad, error) {
	predNquad, subject, _, err := objToNquads(data.data)
	if err != nil {
		return nil, nil, err
	}
	var setl, dell []*api.NQuad
	for _, pnquad := range predNquad {
		setl = append(setl, pnquad.Sets...)
		// 更新操作可能删除值
		if op == MuUpdate {
			dell = append(dell, pnquad.Dels...)
		}
	}
	// 更新操作不会更新dgraph.type
	if len(setl) > 0 && (op == MuInsert) {
		for _, dt := range data.Dtype {
			setl = append(setl, &api.NQuad{
				Subject:     subject,
				Predicate:   typePred,
				ObjectValue: &api.Value{Val: &api.Value_StrVal{StrVal: dt}},
			})
		}
	}
	// 更新操作时处理前端传来的显式置空
	jsonRecord := data.JsonRecord()
	if jsonRecord != nil && len(jsonRecord) > 0 && op == MuUpdate {
		for _, record := range jsonRecord {
			if record.Name != "" && record.Set && (record.Invalid || record.IsZero) {
				dell = append(dell, &api.NQuad{
					Subject:     subject,
					Predicate:   ReformPredicate(data.data, record.Name),
					ObjectValue: StarNqVal,
				})
			}
		}
	}
	return setl, dell, nil
}

// DelNquad obj结构体解析为dgraph删除语句
// 结构中必须包含uid字段，否则无法明确删除对象
// 如果仅包含uid，则删除该节点下的所有值（包括有向连接）
// 如果包含了uid以外的值，则仅删除给入值的部分
// 目前无法明确的表达需要清空结构中的某个字段值（因为无法明确前端的空值），需要结合del和set操作同时处理
// 即先删除所有值，然后再插入需要的值
// 或者明确的给入该字段中的所有值
// 删除操作不需要解析边属性（边属性无法显示删除），删除边的同时边属性也不复存在，或者替换操作也会覆盖该边属性
//func DelNquad(data *TxnData) ([]*api.NQuad, error) {
//	predNquad, subject, _, err := objToNquads(data.data)
//	if err != nil {
//		return nil, err
//	}
//	var nql []*api.NQuad
//	for _, pnquad := range predNquad {
//		nql = append(nql, pnquad.Sets...)
//	}
//	// 如果没有解析出任何值，则删除节点所有值,仅保留节点本身
//	// 注意，该行为不会删除指向该节点的其它节点边，所以外部还需要另外处理
//	if len(nql) == 0 {
//		nql = append(nql, &api.NQuad{
//			Subject:     subject,
//			Predicate:   StarAll,
//			ObjectValue: StarNqVal,
//		})
//	}
//	return nql, nil
//}

// objToNquads 将对象结构体转换为变更列表
// 返回值1 = 变更列表
// 返回值2 = subject，即变更对象uid值，该值必须非空
// 返回值3 = dgraph type，类型值，非空
// 返回值4 = 错误
func objToNquads(obj any) ([]PredNquad, string, string, error) {
	var (
		val = reflect.ValueOf(obj)
		err error
	)
	parser := new(nquadParser)
	val, err = parser.checkObj(val)
	if err != nil {
		return nil, "", "", err
	}
	if parser.isZero(val) {
		return nil, "", "", errors.New("empty object value, nothing to change")
	}
	dType := parser.getDtype(val)
	if dType == "" {
		return nil, "", "", errors.New("failed to parse dgraph type")
	}
	parser.dtype = dType
	subj := parser.getSubject(val)
	if subj == "" {
		return nil, "", "", errors.New("failed to get obj subject(uid)")
	}
	parser.subject = subj
	err = parser.parseNquads(val)
	if err != nil {
		return nil, "", "", err
	}
	nql := parser.toNquadList()
	return nql, subj, dType, nil
}

type nquadParser struct {
	subject string                // 对象uid值
	dtype   string                // 对象类型
	valMap  map[string]*PredNquad // 对象谓词值字典
}

func (n *nquadParser) checkObj(value reflect.Value) (reflect.Value, error) {
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return reflect.ValueOf(nil), errors.New(`set nquad failed, obj must be kind of "struct"`)
	}
	n.valMap = make(map[string]*PredNquad)
	return value, nil
}

func (n *nquadParser) isZero(value reflect.Value) bool {
	if !value.IsValid() || value.IsZero() {
		return true
	}
	return false
}

func (n *nquadParser) getDtype(value reflect.Value) string {
	return value.Type().Name()
}

func (n *nquadParser) getSubject(val reflect.Value) string {
	field := val.FieldByName(uidField)
	if field.String() != "" {
		return field.String()
	}
	return ""
}

func (n *nquadParser) parseNquads(value reflect.Value) error {
	var err error
	value, err = n.checkObj(value)
	if err != nil {
		return err
	}
	if n.isZero(value) {
		return nil
	}
	typ := value.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldVal := value.Field(i)
		if n.isZero(fieldVal) {
			continue
		}
		if field.Anonymous {
			err = n.parseNquads(fieldVal)
			if err != nil {
				return err
			}
			continue
		}

		pred, err := predByField(field)
		if err != nil {
			return err
		}
		if pred == nil || pred.Name == "" {
			continue
		}
		nquads, err := pred.ToNquad(n.subject, fieldVal)
		if err != nil {
			return err
		}
		n.valMap[pred.Name+pred.langVar] = nquads
	}
	return nil
}

func (n *nquadParser) toNquadList() []PredNquad {
	var r []PredNquad
	for _, v := range n.valMap {
		r = append(r, *v)
	}
	return r
}

func getDbTag(p reflect.StructField) string {
	p.Tag.Get("db")
}

func nqDefaultVal(val string) *api.Value {
	return &api.Value{Val: &api.Value_DefaultVal{DefaultVal: val}}
}

func nqIntVal(val int64) *api.Value {
	return &api.Value{Val: &api.Value_IntVal{IntVal: val}}
}

func nqFloatVal(val float64) *api.Value {
	return &api.Value{Val: &api.Value_DoubleVal{DoubleVal: val}}
}

func nqStringVal(val string) *api.Value {
	return &api.Value{Val: &api.Value_StrVal{StrVal: val}}
}

func nqBoolVal(val bool) *api.Value {
	return &api.Value{Val: &api.Value_BoolVal{BoolVal: val}}
}

func nqTimeVal(val time.Time) *api.Value {
	s, _ := val.MarshalBinary()
	return &api.Value{Val: &api.Value_DatetimeVal{DatetimeVal: s}}
}

func nqGeoVal(val geom.T) *api.Value {
	b, _ := geojson.Marshal(val)
	return &api.Value{Val: &api.Value_GeoVal{GeoVal: b}}
}

func nqPasswdVal(val string) *api.Value {
	b, err := bcrypt.GenerateFromPassword([]byte(val), bcrypt.MinCost)
	if err != nil {
		return nil
	}
	return &api.Value{Val: &api.Value_PasswordVal{PasswordVal: string(b)}}
}
