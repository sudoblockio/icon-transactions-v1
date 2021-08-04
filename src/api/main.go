package main

import (
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-transactions/api/healthcheck"
	"github.com/geometry-labs/icon-transactions/api/routes"
	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/global"
	"github.com/geometry-labs/icon-transactions/kafka"
	"github.com/geometry-labs/icon-transactions/logging"
	"github.com/geometry-labs/icon-transactions/metrics"
	_ "github.com/geometry-labs/icon-transactions/models" // for swagger docs
)

func main() {
	config.ReadEnvironment()

	logging.Init()
	zap.S().Debug("Main: Starting logging with level ", config.Config.LogLevel)

	// Start kafka consumers
	// Go routines start in function
	kafka.StartAPIConsumers()

	// Start Prometheus client
	// Go routine starts in function
	metrics.MetricsAPIStart()

	// Start API server
	// Go routine starts in function
	routes.Start()

	// Start Health server
	// Go routine starts in function
	healthcheck.Start()

  global.WaitShutdownSig()
}
