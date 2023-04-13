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

// GetSchema 获取dgraph所有谓词和类型
func (d *Txn) GetSchema() (*Schema, error) {
	const q = `schema{}`
	var res = new(Schema)
	resp, err := d.Txn.Query(context.Background(), q)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(resp.Json, res)
	if err != nil {
		return nil, err
	}
	r := res.SkipSysSchema()
	return r, err
}

// FindPred 查找特定谓词结构,如果不存在则报错
func (d *Txn) FindPred(pred string) (*Pred, error) {
	var res Schema
	q := fmt.Sprintf(`schema(pred: %s){}`, pred)
	resp, err := d.Txn.Query(context.Background(), q)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(resp.Json, &res)
	if err != nil {
		return nil, err
	}
	if len(res.Preds) == 0 {
		return nil, nil
	}
	p := res.Preds[0]
	return &p, nil
}

// FindType 查找特定类型,如果不存在则报错
func (d *Txn) FindType(tp string) (*Type, error) {
	var res Schema
	resp, err := d.Txn.Query(context.Background(), fmt.Sprintf(`schema(type: %s){}`, tp))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(resp.Json, &res)
	if err != nil {
		return nil, err
	}
	if len(res.Types) == 0 {
		return nil, nil
	}
	p := res.Types[0]
	return &p, nil
}
