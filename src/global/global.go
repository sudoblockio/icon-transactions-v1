package global

const Version = "v0.1.0"

//type Global struct {
//	Transactions *crud.TransactionModelMongo
//}

//var globalInstance *Global
//var globalOnce sync.Once
//
//func GetGlobal() *Global {
//	globalOnce.Do(func() {
//		globalInstance = &Global{
//			Transactions: crud.GetTransactionModelMongo(),
//		}
//	})
//	return globalInstance
//}

var ShutdownChan = make(chan int)
