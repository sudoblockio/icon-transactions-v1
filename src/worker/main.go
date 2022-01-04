package main

import (
	"log"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/global"
	"github.com/geometry-labs/icon-transactions/kafka"
	"github.com/geometry-labs/icon-transactions/logging"
	"github.com/geometry-labs/icon-transactions/metrics"
	"github.com/geometry-labs/icon-transactions/worker/routines"
	"github.com/geometry-labs/icon-transactions/worker/transformers"
)

func main() {
	config.ReadEnvironment()

	logging.Init()
	log.Printf("Main: Starting logging with level %s", config.Config.LogLevel)

	// Start Prometheus client
	metrics.Start()

	// Feature flags
	if config.Config.OnlyRunAllRoutines == true {
		// Start routines
		routines.StartTransactionCountRoutine()
		routines.StartTransactionCountByAddressRoutine()
		routines.StartTransactionInternalCountByAddressRoutine()
		routines.StartTokenTransferCountByAddressRoutine()
		routines.StartTokenTransferCountByTokenContractRoutine()
		routines.StartTokenHoldersRoutine()
		routines.StartTokenHolderCountByTokenContractRoutine()

		global.WaitShutdownSig()
	}

	// Start kafka consumer
	kafka.StartWorkerConsumers()

	// Start transformers
	transformers.StartTransactionsTransformer()
	transformers.StartLogsTransformer()

	global.WaitShutdownSig()
}
