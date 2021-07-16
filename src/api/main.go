package main

import (
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/geometry-labs/icon-transactions/api/healthcheck"
	"github.com/geometry-labs/icon-transactions/api/routes"
	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/global"
	"github.com/geometry-labs/icon-transactions/kafka"
	"github.com/geometry-labs/icon-transactions/logging"
	"github.com/geometry-labs/icon-transactions/metrics"
)

func main() {
	config.ReadEnvironment()

	logging.StartLoggingInit()
	zap.S().Debug("Main: Starting logging with level ", config.Config.LogLevel)

	global.GetGlobal()
	// Start kafka consumers
	// Go routines start in function
	kafka.StartApiConsumers()

	// Start Prometheus client
	// Go routine starts in function
	metrics.MetricsApiStart()

	// Start API server
	// Go routine starts in function
	routes.Start()

	// Start Health server
	// Go routine starts in function
	healthcheck.Start()

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
