package loader

import (
	"github.com/geometry-labs/icon-blocks/global"
	"github.com/geometry-labs/icon-blocks/models"
	"go.uber.org/zap"
)

func StartTransactionLoader() {
	go TransactionLoader()
}

func TransactionLoader() {
	var transaction *models.Transaction
	postgresLoaderChan := global.GetGlobal().Transactions.GetWriteChan()
	for {
		transaction = <-postgresLoaderChan
		global.GetGlobal().Transactions.RetryCreate(transaction) // inserted here !!
		zap.S().Debug("Loader Transaction: Loaded in postgres table Transactions, Block Number", transaction.BlockNumber)
	}
}
