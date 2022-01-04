package routines

import (
	"errors"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/models"
)

func StartTokenHolderCountByTokenContractRoutine() {

	// routine every day
	go tokenHolderCountByTokenContractRoutine(3600 * time.Second)
}

func tokenHolderCountByTokenContractRoutine(duration time.Duration) {

	// Loop every duration
	for {

		// Loop through all addresses
		skip := 0
		limit := 100
		for {
			tokenHolders, err := crud.GetTokenHolderModel().SelectMany(limit, skip)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Sleep
				break
			} else if err != nil {
				zap.S().Fatal(err.Error())
			}
			if len(*tokenHolders) == 0 {
				// Sleep
				break
			}

			zap.S().Info("Routine=TokenHolderCountByTokenContract", " - Processing ", len(*tokenHolders), " token holders...")
			for _, t := range *tokenHolders {

				count, err := crud.GetTokenHolderModel().CountByTokenContract(t.TokenContractAddress)
				if err != nil {
					zap.S().Warn("Routine=TokenHolderCountByTokenContract", " - Error counting holders: ", err.Error())
					continue
				}

				tokenHolderCountByTokenContract := &models.TokenHolderCountByTokenContract{
					TokenContractAddress: t.TokenContractAddress,
					Count:                uint64(count),
				}

				// Insert to database
				crud.GetTokenHolderCountByTokenContractModel().LoaderChannel <- tokenHolderCountByTokenContract
			}

			skip += limit
		}

		zap.S().Info("Completed routine, sleeping...")
		time.Sleep(duration)
	}
}
