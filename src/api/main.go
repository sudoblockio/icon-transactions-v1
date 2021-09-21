package main

import (
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-transactions/api/healthcheck"
	"github.com/geometry-labs/icon-transactions/api/routes"
	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/global"
	"github.com/geometry-labs/icon-transactions/logging"
	"github.com/geometry-labs/icon-transactions/metrics"
	_ "github.com/geometry-labs/icon-transactions/models" // for swagger docs
	"github.com/geometry-labs/icon-transactions/redis"
)

func main() {
	config.ReadEnvironment()

	logging.Init()
	zap.S().Debug("Main: Starting logging with level ", config.Config.LogLevel)

	// Start Prometheus client
	// Go routine starts in function
	metrics.Start()

	// Start Redis Client
	// NOTE: redis is used for websockets
	redis.GetBroadcaster().Start()
	redis.GetRedisClient().StartSubscriber()

	// Start API server
	// Go routine starts in function
	routes.Start()

	// Start Health server
	// Go routine starts in function
	healthcheck.Start()

	global.WaitShutdownSig()
}
