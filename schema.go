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

// SchemaPred 谓词数据结构
type SchemaPred struct {
	Name    string   `json:"predicate" schema:"predicate"`
	Type    string   `json:"type" schema:"type"`
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
		ptype   = s.Type
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
