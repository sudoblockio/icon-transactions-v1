package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/geometry-labs/icon-transactions/worker/transformers"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/global"
	"github.com/geometry-labs/icon-transactions/kafka"
	"github.com/geometry-labs/icon-transactions/logging"
	"github.com/geometry-labs/icon-transactions/metrics"
)

func main() {
	config.ReadEnvironment()

	logging.Init()
	log.Printf("Main: Starting logging with level %s", config.Config.LogLevel)

	// Start Prometheus client
	metrics.MetricsWorkerStart()

	// Start Mongodb loader
	crud.StartTransactionLoader()

	// Start kafka consumer
	kafka.StartWorkerConsumers()

	// Start kafka Producer
	kafka.StartProducers()

	// Start transformers
	transformers.StartTransactionsTransformer()

	// Listen for close sig
	// Register for interupt (Ctrl+C) and SIGTERM (docker)

	//create a notification channel to shutdown
	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		zap.S().Info("Shutting down...")
		global.ShutdownChan <- 1
	}()

	<-global.ShutdownChan
}
