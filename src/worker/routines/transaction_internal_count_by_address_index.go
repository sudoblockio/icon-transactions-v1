package routines

import (
	"errors"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/models"
)

func StartTransactionInternalCountByAddressIndexRoutine() {

	// routine every day
	go transactionInternalCountByAddressIndexRoutine(3600 * time.Second)
}

func transactionInternalCountByAddressIndexRoutine(duration time.Duration) {

	// Loop every duration
	for {

		// Loop through all addresses
		limit := 1000
		for {
			transactionInternalCountByAddressIndices, err := crud.GetTransactionInternalCountByAddressIndexModel().SelectMissingBlockNumbers(limit)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Sleep
				zap.S().Info("Routine=TransactionInternalCountByAddressIndex", " - No records found, sleeping...")
				break
			} else if err != nil {
				zap.S().Fatal(err.Error())
			}
			if len(*transactionInternalCountByAddressIndices) == 0 {
				// Sleep
				break
			}

			zap.S().Info("Routine=TransactionInternalCountByAddressIndex", " - Processing ", len(*transactionInternalCountByAddressIndices), " transactionInternalCountByAddressIndices...")
			for _, t := range *transactionInternalCountByAddressIndices {

				transactionInternal, err := crud.GetTransactionModel().SelectOne(t.TransactionHash, int32(t.LogIndex))
				if err != nil {
					continue
				}

				transactionInternalCountByAddressIndex := &models.TransactionInternalCountByAddressIndex{
					TransactionHash: t.TransactionHash,
					LogIndex:        uint64(t.LogIndex),
					Address:         t.Address,
					BlockNumber:     transactionInternal.BlockNumber,
				}

				crud.GetTransactionInternalCountByAddressIndexModel().UpsertOne(transactionInternalCountByAddressIndex)
			}
		}

		zap.S().Info("Completed routine, sleeping...")
		// time.Sleep(duration)
		break
	}
}
