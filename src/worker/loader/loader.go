package loader

import (
	"github.com/geometry-labs/icon-transactions/global"
	"github.com/geometry-labs/icon-transactions/models"
	"go.uber.org/zap"
)

func StartTransactionLoader() {
	go transactionLoader()
}

func transactionLoader() {
	var transaction *models.Transaction
	postgresLoaderChan := global.GetGlobal().Transactions.GetWriteChan()
	for {
		transaction = <-postgresLoaderChan
		global.GetGlobal().Transactions.RetryCreate(transaction) // inserted here !!
		zap.S().Debug("Loader Transaction: Loaded in postgres table Transactions, Block Number", transaction.BlockNumber)
	}
}
