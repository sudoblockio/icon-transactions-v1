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
		skip := 0
		limit := 1000
		for {
			transactions, err := crud.GetTransactionModel().SelectMany(limit, skip, "", "", "")
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Sleep
				zap.S().Info("Routine=TransactionInternalCountByAddressIndex", " - No records found, sleeping...")
				break
			} else if err != nil {
				zap.S().Fatal(err.Error())
			}
			if len(*transactions) == 0 {
				// Sleep
				break
			}

			zap.S().Info("Routine=TransactionInternalCountByAddressIndex", " - Processing ", len(*transactions), " transactions...")
			for _, t := range *transactions {

				if t.Type == "transaction" {
					// regular transaction
					continue
				}

				transactionInternalCountByAddressIndexFromAddress := &models.TransactionInternalCountByAddressIndex{
					TransactionHash: t.Hash,
					LogIndex:        uint64(t.LogIndex),
					Address:         t.FromAddress,
					BlockNumber:     t.BlockNumber,
				}

				transactionInternalCountByAddressIndexToAddress := &models.TransactionInternalCountByAddressIndex{
					TransactionHash: t.Hash,
					LogIndex:        uint64(t.LogIndex),
					Address:         t.ToAddress,
					BlockNumber:     t.BlockNumber,
				}

				crud.GetTransactionInternalCountByAddressIndexModel().UpsertOne(transactionInternalCountByAddressIndexFromAddress)
				crud.GetTransactionInternalCountByAddressIndexModel().UpsertOne(transactionInternalCountByAddressIndexToAddress)
			}

			skip += limit
		}

		zap.S().Info("Completed routine, sleeping...")
		// time.Sleep(duration)
		break
	}
}
