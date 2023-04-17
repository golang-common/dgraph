package dgraph

// Type 类型字段
// Name - 类型名称
// Fields - 类型列表
// RevPreds - 指向该类型(Uid)的谓词列表
type Type struct {
	Name     string          `json:"name"`
	Fields   map[string]Pred `json:"fields,omitempty"`
	RevPreds []SchemaPred    `json:"revPreds,omitempty"`
}

// Schema 将类型转换为操作RDF，用于增加表
func (t Type) Schema() SchemaType {
	var r = SchemaType{Name: t.Name}
	for _, fields := range t.Fields {
		if !fields.Reversed {
			r.Fields = append(r.Fields, SchemaTypeField{Name: fields.Name})
		}
	}
	return r
}
