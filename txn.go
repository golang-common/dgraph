package dgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/dgo/v210"
)

type Txn struct {
	*dgo.Txn
}

// Schema 获取dgraph所有谓词和类型
func (d *Txn) Schema() (Schema, error) {
	resp, err := d.Txn.Query(context.Background(), `schema{}`)
	if err != nil {
		return Schema{}, err
	}
	r := Schema{}
	err = json.Unmarshal(resp.Json, &r)
	if err != nil {
		return Schema{}, err
	}
	r = r.SkipSysSchema()
	return r, err
}

// SchemaPred 查找特定谓词结构,如果不存在则报错
func (d *Txn) SchemaPred(name string) (SchemaPred, error) {
	var res Schema
	q := fmt.Sprintf(`schema(pred: %s){}`, name)
	resp, err := d.Txn.Query(context.Background(), q)
	if err != nil {
		return SchemaPred{}, err
	}
	err = json.Unmarshal(resp.Json, &res)
	if err != nil {
		return SchemaPred{}, err
	}
	if len(res.Preds) == 0 {
		return SchemaPred{}, nil
	}
	p := res.Preds[0]
	return p, nil
}

// SchemaType 查找特定类型,如果不存在则报错
func (d *Txn) SchemaType(name string) (SchemaType, error) {
	var res Schema
	resp, err := d.Txn.Query(context.Background(), fmt.Sprintf(`schema(type: %s){}`, name))
	if err != nil {
		return SchemaType{}, err
	}
	err = json.Unmarshal(resp.Json, &res)
	if err != nil {
		return SchemaType{}, err
	}
	if len(res.Types) == 0 {
		return SchemaType{}, nil
	}
	p := res.Types[0]
	return p, nil
}
