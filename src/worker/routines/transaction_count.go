package routines

import (
	"time"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/models"
	"github.com/geometry-labs/icon-transactions/redis"
)

func StartTransactionCountRoutine() {

	// routine every day
	go transactionCountRoutine(3600 * time.Second)
}

func transactionCountRoutine(duration time.Duration) {

	// Loop every duration
	for {

		/////////////
		// Regular //
		/////////////

		// Count
		count, err := crud.GetTransactionModel().SelectCountRegular()
		if err != nil {
			// Postgres error
			zap.S().Warn(err)
			continue
		}

		// Update Redis
		countKey := "icon_transactions_transaction_count_regular"
		err = redis.GetRedisClient().SetCount(countKey, count)
		if err != nil {
			// Redis error
			zap.S().Warn(err)
			continue
		}

		// Update Postgres
		transactionCount := &models.TransactionCount{
			Type:  "regular",
			Count: uint64(count),
		}
		err = crud.GetTransactionCountModel().UpsertOne(transactionCount)

		//////////////
		// Internal //
		//////////////

		// Count
		count, err = crud.GetTransactionModel().SelectCountInternal()
		if err != nil {
			// Postgres error
			zap.S().Warn(err)
			continue
		}

		// Update Redis
		countKey = "icon_transactions_transaction_count_internal"
		err = redis.GetRedisClient().SetCount(countKey, count)
		if err != nil {
			// Redis error
			zap.S().Warn(err)
			continue
		}

		// Update Postgres
		transactionCount = &models.TransactionCount{
			Type:  "internal",
			Count: uint64(count),
		}
		err = crud.GetTransactionCountModel().UpsertOne(transactionCount)

		////////////////////
		// Token Transfer //
		////////////////////

		// Count
		count, err = crud.GetTokenTransferModel().SelectCount()
		if err != nil {
			// Postgres error
			zap.S().Warn(err)
			continue
		}

		// Update Redis
		countKey = "icon_transactions_transaction_count_token_transfer"
		err = redis.GetRedisClient().SetCount(countKey, count)
		if err != nil {
			// Redis error
			zap.S().Warn(err)
			continue
		}

		// Update Postgres
		transactionCount = &models.TransactionCount{
			Type:  "token_transfer",
			Count: uint64(count),
		}
		err = crud.GetTransactionCountModel().UpsertOne(transactionCount)

		zap.S().Info("Completed routine, sleeping...")
		time.Sleep(duration)
	}
}
