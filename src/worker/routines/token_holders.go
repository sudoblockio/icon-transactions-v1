package routines

import (
	"errors"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/models"
	"github.com/geometry-labs/icon-transactions/worker/utils"
)

func StartTokenHoldersRoutine() {

	// routine every day
	go tokenHoldersRoutine(3600*time.Second, true)
	go tokenHoldersRoutine(3600*time.Second, false)
}

func tokenHoldersRoutine(duration time.Duration, isFromAddress bool) {

	// Loop every duration
	for {

		// Loop through all addresses
		skip := 0
		limit := 1000
		for {
			var tokenTransfers *[]models.TokenTransfer
			var err error
			if isFromAddress {
				tokenTransfers, err = crud.GetTokenTransferModel().SelectManyDistinctFromAddresses(limit, skip)
			} else {
				tokenTransfers, err = crud.GetTokenTransferModel().SelectManyDistinctToAddresses(limit, skip)
			}
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Sleep
				break
			} else if err != nil {
				zap.S().Fatal(err.Error())
			}
			if len(*tokenTransfers) == 0 {
				// Sleep
				break
			}

			zap.S().Info("Routine=TokenHolders", " - Processing ", len(*tokenTransfers), " token transfers...")
			for _, t := range *tokenTransfers {
				address := ""
				if isFromAddress {
					address = t.FromAddress
				} else {
					address = t.ToAddress
				}

				// Node call
				value, err := utils.IconNodeServiceGetTokenBalance(t.TokenContractAddress, address)
				if err != nil {
					// Icon node error
					zap.S().Warn("Routine=TokenHolder - Error: ", err.Error())
					continue
				}

				// Hex -> float64
				decimalBase, err := utils.IconNodeServiceGetTokenDecimalBase(t.TokenContractAddress)
				if err != nil {
					// Icon node error
					zap.S().Warn("Routine=TokenHolder - Error: ", err.Error())
					continue
				}
				valueDecimal := utils.StringHexToFloat64(value, decimalBase)

				tokenHolder := &models.TokenHolder{
					TokenContractAddress: t.TokenContractAddress,
					HolderAddress:        address,
					Value:                value,
					ValueDecimal:         valueDecimal,
				}

				// Insert to database
				crud.GetTokenHolderModel().LoaderChannel <- tokenHolder
			}

			skip += limit
		}

		zap.S().Info("Completed routine, sleeping...")
		time.Sleep(duration)
	}
}
