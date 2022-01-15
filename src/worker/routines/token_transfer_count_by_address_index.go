package routines

import (
	"errors"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/models"
)

func StartTokenTransferCountByAddressIndexRoutine() {

	// routine every day
	go tokenTransferCountByAddressIndexRoutine(3600 * time.Second)
}

func tokenTransferCountByAddressIndexRoutine(duration time.Duration) {

	// Loop every duration
	for {

		// Loop through all addresses
		skip := 0
		limit := 1000
		for {
			transactions, err := crud.GetTokenTransferModel().SelectMany(limit, skip, "", "", 0, "")
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Sleep
				zap.S().Info("Routine=TokenTransferCountByAddressIndex", " - No records found, sleeping...")
				break
			} else if err != nil {
				zap.S().Fatal(err.Error())
			}
			if len(*transactions) == 0 {
				// Sleep
				break
			}

			zap.S().Info("Routine=TokenTransferCountByAddressIndex", " - Processing ", len(*transactions), " transactions...")
			for _, t := range *transactions {

				tokenTransferCountByAddressIndexFromAddress := &models.TokenTransferCountByAddressIndex{
					TransactionHash: t.TransactionHash,
					LogIndex:        uint64(t.LogIndex),
					Address:         t.FromAddress,
					BlockNumber:     t.BlockNumber,
				}

				tokenTransferCountByAddressIndexToAddress := &models.TokenTransferCountByAddressIndex{
					TransactionHash: t.TransactionHash,
					LogIndex:        uint64(t.LogIndex),
					Address:         t.ToAddress,
					BlockNumber:     t.BlockNumber,
				}

				crud.GetTokenTransferCountByAddressIndexModel().UpsertOne(tokenTransferCountByAddressIndexFromAddress)
				crud.GetTokenTransferCountByAddressIndexModel().UpsertOne(tokenTransferCountByAddressIndexToAddress)
			}

			skip += limit
		}

		zap.S().Info("Completed routine, sleeping...")
		// time.Sleep(duration)
		break
	}
}
