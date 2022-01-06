package routines

import (
	"errors"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/models"
	"github.com/geometry-labs/icon-transactions/redis"
)

func StartTokenTransferCountByTokenContractRoutine() {

	// routine every day
	go tokenTransferCountByTokenContractRoutine(3600 * time.Second)
}

func tokenTransferCountByTokenContractRoutine(duration time.Duration) {

	// Loop every duration
	for {

		// Loop through all addresses
		skip := 0
		limit := 1000
		for {
			tokenTransfers, err := crud.GetTokenTransferModel().SelectManyDistinctTokenContracts(limit, skip)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Sleep
				zap.S().Info("Routine=TokenTransferCountByTokenContract", " - No records found, sleeping...")
				break
			} else if err != nil {
				zap.S().Fatal(err.Error())
			}
			if len(*tokenTransfers) == 0 {
				// Sleep
				break
			}

			zap.S().Info("Routine=TokenTransferCountByTokenContract", " - Processing ", len(*tokenTransfers), " addresses...")
			for _, t := range *tokenTransfers {

				///////////
				// Count //
				///////////
				count, err := crud.GetTokenTransferModel().CountByTokenContract(t.TokenContractAddress)
				if err != nil {
					// Postgres error
					zap.S().Warn(err)
					continue
				}

				//////////////////
				// Update Redis //
				//////////////////
				countKey := "icon_transactions_token_transfer_count_by_token_contract_" + t.TokenContractAddress
				err = redis.GetRedisClient().SetCount(countKey, count)
				if err != nil {
					// Redis error
					zap.S().Warn(err)
					continue
				}

				/////////////////////
				// Update Postgres //
				/////////////////////
				tokenTransferCountByTokenContract := &models.TokenTransferCountByTokenContract{
					TokenContract: t.TokenContractAddress,
					Count:         uint64(count),
				}
				err = crud.GetTokenTransferCountByTokenContractModel().UpsertOne(tokenTransferCountByTokenContract)
			}

			skip += limit
		}

		zap.S().Info("Completed routine, sleeping...")
		time.Sleep(duration)
	}
}
