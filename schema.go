/**
 * @Author: DPY
 * @Description:
 * @File:  schema
 * @Version: 1.0.0
 * @Date: 2021/11/22 10:49
 */

package dgraph

import (
	"fmt"
	"strings"
)

// Schema 数据结构
type Schema struct {
	Preds []SchemaPred `json:"schema" schema:"schema"`
	Types []SchemaType `json:"types" schema:"types"`
}

// SkipSysSchema 忽略dgraph系统自身schema
func (s Schema) SkipSysSchema() Schema {
	var (
		r     Schema
		preds []SchemaPred
		types []SchemaType
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
	return r
}

// ComparePreds 比较传入的preds，返回原preds中不存在的和发生了变更的谓词
func (s Schema) ComparePreds(preds []SchemaPred) ([]SchemaPred, error) {
	var r []SchemaPred
Loop:
	for _, newPred := range preds {
		var exist bool
		for _, oldPred := range s.Preds {
			if newPred.Name == oldPred.Name {
				exist = true
				same := s.compareTwoPred(newPred, oldPred)
				if !same {
					r = append(r, newPred)
				}
				continue Loop
			}
		}
		if !exist {
			r = append(r, newPred)
		}
	}
	return r, nil
}

// compareTwoPred 比较两个谓词，返回是否相同
func (s Schema) compareTwoPred(new, old SchemaPred) bool {
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

// CompareTypes 比较类型列表是否一致，返回不一致的类型
func (s Schema) CompareTypes(types []SchemaType) ([]SchemaType, error) {
	var r []SchemaType
Loop:
	for _, newType := range types {
		var exist bool
		for _, oldType := range s.Types {
			if newType.Name == oldType.Name {
				exist = true
				same := s.compareTwoType(newType, oldType)
				if !same {
					r = append(r, newType)
				}
				continue Loop
			}
		}
		if !exist {
			r = append(r, newType)
		}
	}
	return r, nil
}

// compareTwoType 比较两个类型是否一致
func (s Schema) compareTwoType(new, old SchemaType) bool {
Loop:
	for _, newfield := range new.Fields {
		for _, oldfield := range old.Fields {
			if newfield.Name == oldfield.Name {
				continue Loop
			}
		}
		return false
	}
	return true
}

// SchemaPred 谓词数据结构
type SchemaPred struct {
	Name    string   `json:"predicate" schema:"predicate"`
	Type    PredType `json:"type" schema:"type"`
	Index   bool     `json:"index" schema:"index"`
	Tokens  []string `json:"tokenizer" schema:"tokenizer"`
	Reverse bool     `json:"reverse" schema:"reverse"`
	Count   bool     `json:"count" schema:"count"`
	List    bool     `json:"list" schema:"list"`
	Upsert  bool     `json:"upsert" schema:"upsert"`
	Lang    bool     `json:"lang" schema:"lang"`
}

func (s SchemaPred) Rdf() string {
	var (
		model   = `$name: $type $indices .`
		ptype   = s.Type.String()
		indices []string
	)
	if s.Index && len(s.Name) > 0 {
		indices = append(indices, fmt.Sprintf("@index(%s)", strings.Join(s.Tokens, ",")))
	}
	if s.Reverse {
		indices = append(indices, "@reverse")
	}
	if s.Count {
		indices = append(indices, "@count")
	}
	if s.List {
		ptype = fmt.Sprintf("[%s]", ptype)
	}
	if s.Upsert {
		indices = append(indices, "@upsert")
	}
	if s.Lang {
		indices = append(indices, "@lang")
	}

	replacer := strings.NewReplacer(
		"$name", s.Name,
		"$type", ptype,
		"$indices", strings.Join(indices, " "),
	)
	return replacer.Replace(model)
}

type SchemaType struct {
	Name   string            `json:"name"`
	Fields []SchemaTypeField `json:"fields,omitempty"`
}

type SchemaTypeField struct {
	Name string `json:"name,omitempty"`
}

func (s SchemaType) Rdf() string {
	var predNameList []string
	for _, field := range s.Fields {
		predName := field.Name
		if strings.HasPrefix(predName, "~") {
			predName = fmt.Sprintf("<%s>", predName)
		}
		predNameList = append(predNameList, predName)
	}
	rdf := fmt.Sprintf("type %s{\n\t%s\n}", s.Name, strings.Join(predNameList, "\n\t"))
	return rdf
}
