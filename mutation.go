/**
 * @Author: DPY
 * @Description: 变更抽象
 * @File:  mutation.go
 * @Version: 1.0.0
 * @Date: 2022/2/11 15:57
 */

package dgraph

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/dgo/v210/protos/api"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Request struct {
	vars    map[string]string
	varlist []string
	qlist   []string
	Mul     []*Mutation
}

// Marshal 构建带条件查询的变更
func (c *Request) Marshal(commitNow bool) (*api.Request, error) {
	var (
		query    string
		vartitle string
		subquery string
	)
	if len(c.varlist) > 0 {
		vartitle = fmt.Sprintf("(%s)", strings.Join(c.varlist, ","))
	}
	if len(c.qlist) > 0 {
		subquery = fmt.Sprintf("{\n\t%s\n}", strings.Join(c.qlist, "\n\t"))
	}
	if subquery != "" {
		query = fmt.Sprintf("query Me%s%s", vartitle, subquery)
	}
	var mul []*api.Mutation
	for _, m := range c.Mul {
		if mm := m.Marshal(); mm != nil {
			mm.CommitNow = commitNow
			mul = append(mul, mm)
		}
	}
	if len(mul) == 0 {
		return nil, errors.New("no mutations has been set")
	}
	req := new(api.Request)
	req.Query = query
	req.Vars = c.vars
	req.Mutations = mul
	req.CommitNow = commitNow
	return req, nil
}

func (c *Request) AddVar(name string, val interface{}) {
	if v := reflect.ValueOf(val); !v.IsValid() || v.IsZero() {
		return
	}
	if c.vars == nil {
		c.vars = make(map[string]string)
	}
	if _, ok := c.vars[name]; !ok {
		switch val.(type) {
		case int:
			valString := strconv.Itoa(val.(int))
			c.vars[name] = valString
			c.varlist = append(c.varlist, fmt.Sprintf("%s:%s", name, "int"))
		case string:
			c.vars[name] = val.(string)
			c.varlist = append(c.varlist, fmt.Sprintf("%s:%s", name, "string"))
		case []string:
			v := fmt.Sprintf(`["%s"]`, strings.Join(val.([]string), `","`))
			c.vars[name] = v
			c.varlist = append(c.varlist, fmt.Sprintf("%s:%s", name, "string"))
		case bool:
			c.vars[name] = fmt.Sprintf("%t", val.(bool))
			c.varlist = append(c.varlist, fmt.Sprintf("%s:%s", name, "bool"))
		case float32:
			c.vars[name] = fmt.Sprintf("%f", val.(float32))
			c.varlist = append(c.varlist, fmt.Sprintf("%s:%s", name, "float"))
		case float64:
			c.vars[name] = fmt.Sprintf("%f", val.(float64))
			c.varlist = append(c.varlist, fmt.Sprintf("%s:%s", name, "float"))
		case time.Time:
			c.vars[name] = val.(time.Time).String()
			c.varlist = append(c.varlist, fmt.Sprintf("%s:%s", name, "string"))
		}
	}
}

func (c *Request) AddQuery(q string) {
	c.qlist = append(c.qlist, q)
}

func (c *Request) NewMutation() *Mutation {
	mu := new(Mutation)
	c.Mul = append(c.Mul, mu)
	return mu
}

type Mutation struct {
	CondList  []string
	Set       []*api.NQuad
	Del       []*api.NQuad
	SetNquads []byte
	DelNquads []byte
}

func (c *Mutation) Marshal() *api.Mutation {
	var cond string
	if c.Set == nil && c.Del == nil && c.DelNquads == nil && c.SetNquads == nil {
		return nil
	}
	if len(c.CondList) > 0 {
		cond = fmt.Sprintf("@if(%s)", strings.Join(c.CondList, " AND "))
	}
	return &api.Mutation{Cond: cond, Del: c.Del, Set: c.Set, DelNquads: c.DelNquads, SetNquads: c.SetNquads}
}

func (c *Mutation) AddCond(cond string) {
	c.CondList = append(c.CondList, cond)
}
