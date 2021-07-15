package global

import (
	"sync"

	"github.com/geometry-labs/icon-blocks/crud"
)

const Version = "v0.1.0"

type Global struct {
	Transactions *crud.TransactionModel
}

var globalInstance *Global
var globalOnce sync.Once

func GetGlobal() *Global {
	globalOnce.Do(func() {
		globalInstance = &Global{
			Transactions: crud.GetTransacstionModel(),
		}
	})
	return globalInstance
}

var ShutdownChan = make(chan int)
