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
		limit := 1000
		for {
			tokenTransferCountByAddressIndices, err := crud.GetTokenTransferCountByAddressIndexModel().SelectMissingBlockNumbers(limit)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Sleep
				zap.S().Info("Routine=TokenTransferCountByAddressIndex", " - No records found, sleeping...")
				break
			} else if err != nil {
				zap.S().Fatal(err.Error())
			}
			if len(*tokenTransferCountByAddressIndices) == 0 {
				// Sleep
				break
			}

			zap.S().Info("Routine=TokenTransferCountByAddressIndex", " - Processing ", len(*tokenTransferCountByAddressIndices), " tokenTransferCountByAddressIndices...")
			for _, t := range *tokenTransferCountByAddressIndices {

				tokenTransfer, err := crud.GetTokenTransferModel().SelectOne(t.TransactionHash, int32(t.LogIndex))
				if err != nil {
					continue
				}

				tokenTransferCountByAddressIndex := &models.TokenTransferCountByAddressIndex{
					TransactionHash: t.TransactionHash,
					LogIndex:        uint64(t.LogIndex),
					Address:         t.Address,
					BlockNumber:     tokenTransfer.BlockNumber,
				}

				crud.GetTokenTransferCountByAddressIndexModel().UpsertOne(tokenTransferCountByAddressIndex)
			}
		}

		zap.S().Info("Completed routine, sleeping...")
		// time.Sleep(duration)
		break
	}
}
