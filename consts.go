package dgraph

type Constraint string

const (
	Db      = `db`
	Json    = `json`
	Uid     = "Uid"
	StarAll = `_STAR_ALL`

	None    Constraint = "none"
	Unique  Constraint = "unique"
	NotNull Constraint = "notnull"
)

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
