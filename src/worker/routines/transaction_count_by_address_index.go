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
		limit := 1000
		for {
			transactionCountByAddressIndices, err := crud.GetTransactionCountByAddressIndexModel().SelectMissingBlockNumbers(limit)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Sleep
				zap.S().Info("Routine=TransactionCountByAddressIndex", " - No records found, sleeping...")
				break
			} else if err != nil {
				zap.S().Fatal(err.Error())
			}
			if len(*transactionCountByAddressIndices) == 0 {
				// Sleep
				break
			}

			zap.S().Info("Routine=TransactionCountByAddressIndex", " - Processing ", len(*transactionCountByAddressIndices), " transactions...")
			for _, t := range *transactionCountByAddressIndices {

				transaction, err := crud.GetTransactionModel().SelectOne(t.TransactionHash, -1)
				if err != nil {
					continue
				}

				transactionCountByAddressIndex := &models.TransactionCountByAddressIndex{
					TransactionHash: t.TransactionHash,
					Address:         t.Address,
					BlockNumber:     transaction.BlockNumber,
				}

				crud.GetTransactionCountByAddressIndexModel().UpsertOne(transactionCountByAddressIndex)
			}
		}

		zap.S().Info("Completed routine, sleeping...")
		// time.Sleep(duration)
		break
	}
}
