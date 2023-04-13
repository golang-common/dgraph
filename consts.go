package dgraph

type Constraint string

const (
	Db   = `db`
	Json = `json`
	Uid  = "Uid"

	None    Constraint = "none"
	Unique  Constraint = "unique"
	NotNull Constraint = "notnull"
)
