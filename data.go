package dgraph

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"github.com/twpayne/go-geom"
	"math/rand"
	"reflect"
	"time"
)

type Dgraph[T any] struct {
	Data T
}

// ToNquad 将数据解析为dgraph变更rdf，用户发起数据库请求
func (d Dgraph[T]) ToNquad(isAdd ...bool) ([]*api.NQuad, error) {
	err := d.check()
	if err != nil {
		return nil, err
	}
	val := reflect.ValueOf(d.Data)
	if len(isAdd) > 0 && isAdd[0] {
		val.Elem().FieldByName(Uid).SetString(d.addUid())
	}
	id := val.FieldByName(Uid).String()
	if id == "" {
		return nil, errors.New("error empty uid")
	}
	val = val.Elem()
	typ := val.Type()
	var r []*api.NQuad
	for i := 0; i < typ.NumField(); i++ {
		nq, err := d.fieldNquad(id, typ.Field(i).Tag.Get(Db), val.Field(i).Interface())
		if err != nil {
			return nil, err
		}
		r = append(r, nq...)
	}
	return r, nil
}

func (d Dgraph[T]) check() error {
	val := reflect.ValueOf(d.Data)
	if val.Kind() != reflect.Pointer {
		return errors.New("data must be a pointer of value")
	}
	if val.Elem().Kind() != reflect.Struct {
		return errors.New("underground data must be a struct of kind")
	}
	return nil
}

func (d Dgraph[T]) fieldNquad(uid, tag string, data any) ([]*api.NQuad, error) {
	var r []*api.NQuad
	val := reflect.ValueOf(data)
	if !val.IsValid() || val.IsZero() {
		return nil, nil
	}
	// 解析基础数据值
	switch data.(type) {
	case string:
	case int, int8, int16, int32, int64:
	case float32, float64:
	case bool:
	case time.Time:
	case geom.T:
	}
}

func (d Dgraph[T]) addUid() string {
	return fmt.Sprintf("_:%d", rand.Int63())
}
