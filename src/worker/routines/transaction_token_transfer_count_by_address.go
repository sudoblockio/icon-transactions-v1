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

func StartTokenTransferCountByAddressRoutine() {

	// routine every day
	go tokenTransferCountByAddressRoutine(3600 * time.Second)
}

func tokenTransferCountByAddressRoutine(duration time.Duration) {

	// Loop every duration
	for {

		// Loop through all addresses
		skip := 0
		limit := 100
		for {
			addresses, err := crud.GetTokenTransferCountByAddressModel().SelectMany(limit, skip)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Sleep
				zap.S().Info("Routine=TokenTransferCountByAddress", " - No records found, sleeping...")
				break
			} else if err != nil {
				zap.S().Fatal(err.Error())
			}
			if len(*addresses) == 0 {
				// Sleep
				break
			}

			zap.S().Info("Routine=TokenTransferCountByAddress", " - Processing ", len(*addresses), " addresses...")
			for _, a := range *addresses {

				///////////
				// Count //
				///////////
				count, err := crud.GetTokenTransferCountByAddressIndexModel().CountByAddress(a.Address)
				if err != nil {
					// Postgres error
					zap.S().Warn(err)
					continue
				}

				//////////////////
				// Update Redis //
				//////////////////
				countKey := "icon_transactions_token_transfer_count_by_address_" + a.Address
				err = redis.GetRedisClient().SetCount(countKey, count)
				if err != nil {
					// Redis error
					zap.S().Warn(err)
					continue
				}

				/////////////////////
				// Update Postgres //
				/////////////////////
				tokenTransferCountByAddress := &models.TokenTransferCountByAddress{
					Address: a.Address,
					Count:   uint64(count),
				}
				err = crud.GetTokenTransferCountByAddressModel().UpsertOne(tokenTransferCountByAddress)
			}

			skip += limit
		}

		zap.S().Info("Completed routine, sleeping...")
		time.Sleep(duration)
	}
}
