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
	go tokenHoldersRoutine(3600 * time.Second)
}

func tokenHoldersRoutine(duration time.Duration) {

	// Loop every duration
	for {

		// Loop through all addresses
		skip := 0
		limit := 100
		for {
			tokenTransfers, err := crud.GetTokenTransferModel().SelectMany(limit, skip, "", "", 0, "")
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

				//////////////////
				// From Address //
				//////////////////

				// Node call
				value, err := utils.IconNodeServiceGetTokenBalance(t.TokenContractAddress, t.FromAddress)
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
					HolderAddress:        t.FromAddress,
					Value:                value,
					ValueDecimal:         valueDecimal,
				}

				// Insert to database
				crud.GetTokenHolderModel().LoaderChannel <- tokenHolder

				////////////////
				// To Address //
				////////////////

				// Node call
				value, err = utils.IconNodeServiceGetTokenBalance(t.TokenContractAddress, t.ToAddress)
				if err != nil {
					// Icon node error
					zap.S().Warn("Routine=TokenHolder - Error: ", err.Error())
					continue
				}

				// Hex -> float64
				decimalBase, err = utils.IconNodeServiceGetTokenDecimalBase(t.TokenContractAddress)
				if err != nil {
					// Icon node error
					zap.S().Warn("Routine=TokenHolder - Error: ", err.Error())
					continue
				}
				valueDecimal = utils.StringHexToFloat64(value, decimalBase)

				tokenHolder = &models.TokenHolder{
					TokenContractAddress: t.TokenContractAddress,
					HolderAddress:        t.ToAddress,
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
