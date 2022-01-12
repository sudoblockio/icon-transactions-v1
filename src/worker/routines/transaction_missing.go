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

func StartTransactionMissingRoutine() {

	// routine every day
	go transactionMissingRoutine(3600 * time.Second)
}

func transactionMissingRoutine(duration time.Duration) {

	// Loop every duration
	for {

		currentBlockNumber := 1

		for {

			transactionHashes, err := utils.IconNodeServiceGetBlockTransactionHashes(currentBlockNumber)
			if err != nil {
				zap.S().Warn(
					"Routine=TransactionMissing",
					" CurrentBlockNumber=", currentBlockNumber,
					" Error=", err.Error(),
					" Sleeping 1 second...",
				)

				time.Sleep(1 * time.Second)
				continue
			}

			for _, txHash := range *transactionHashes {
				tx, err := crud.GetTransactionModel().SelectOne(txHash, -1)

				if errors.Is(err, gorm.ErrRecordNotFound) || tx.Signature == "" {
					transactionMissing := &models.TransactionMissing{
						Hash: txHash,
					}

					crud.GetTransactionMissingModel().LoaderChannel <- transactionMissing
				} else if err != nil {
					zap.S().Warn("Loader=TransactionMissing Number=", currentBlockNumber, " Error=", err.Error(), " - Retrying...")

					time.Sleep(1 * time.Second)
					continue
				}

			}

			if currentBlockNumber > 44000000 {
				break
			}

			if currentBlockNumber%100000 == 0 {
				zap.S().Info("Routine=TransactionMissing, CurrentBlockNumber= ", currentBlockNumber, " - Checked 100,000 blocks...")
			}

			currentBlockNumber++
		}

		zap.S().Info("Completed routine, sleeping...")
		time.Sleep(duration)
	}
}
