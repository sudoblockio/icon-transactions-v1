package routines

import (
	"errors"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/models"
)

func StartTransactionCountByAddressIndexRoutine() {

	// routine every day
	go transactionCountByAddressIndexRoutine(3600 * time.Second)
}

func transactionCountByAddressIndexRoutine(duration time.Duration) {

	// Loop every duration
	for {

		// Loop through all addresses
		skip := 0
		limit := 1000
		for {
			transactions, err := crud.GetTransactionModel().SelectMany(limit, skip, "", "", "")
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Sleep
				zap.S().Info("Routine=TransactionCountByAddressIndex", " - No records found, sleeping...")
				break
			} else if err != nil {
				zap.S().Fatal(err.Error())
			}
			if len(*transactions) == 0 {
				// Sleep
				break
			}

			zap.S().Info("Routine=TransactionCountByAddressIndex", " - Processing ", len(*transactions), " transactions...")
			for _, t := range *transactions {

				if t.Type == "log" {
					// internal transaction
					continue
				}

				transactionCountByAddressIndexFromAddress := &models.TransactionCountByAddressIndex{
					TransactionHash: t.Hash,
					Address:         t.FromAddress,
					BlockNumber:     t.BlockNumber,
				}

				transactionCountByAddressIndexToAddress := &models.TransactionCountByAddressIndex{
					TransactionHash: t.Hash,
					Address:         t.ToAddress,
					BlockNumber:     t.BlockNumber,
				}

				crud.GetTransactionCountByAddressIndexModel().UpsertOne(transactionCountByAddressIndexFromAddress)
				crud.GetTransactionCountByAddressIndexModel().UpsertOne(transactionCountByAddressIndexToAddress)
			}

			skip += limit
		}

		zap.S().Info("Completed routine, sleeping...")
		// time.Sleep(duration)
		break
	}
}
