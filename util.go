package dgraph

import (
	"fmt"
	"math/rand"
)

func AddUid() string {
	return fmt.Sprintf("_:%d", rand.Int63())
}
