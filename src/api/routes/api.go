package routes

import (
	"encoding/json"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/global"
	"go.uber.org/zap"

	swagger "github.com/arsmn/fiber-swagger/v2"
	fiber "github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"

	_ "github.com/geometry-labs/icon-transactions/api/docs" // import swagger docs
	"github.com/geometry-labs/icon-transactions/api/routes/rest"
	"github.com/geometry-labs/icon-transactions/api/routes/ws"
)

// @title Go api template docs
// @version 2.0
// @description This is a sample server server.
func Start() {

	app := fiber.New()

	// Logging middleware
	app.Use(func(c *fiber.Ctx) error {
		zap.S().Info(c.Method(), " ", c.Path())

		// Go to next middleware:
		return c.Next()
	})

	// CORS Middleware
	app.Use(cors.New(cors.Config{
		AllowOrigins:  config.Config.CORSAllowOrigins,
		AllowHeaders:  config.Config.CORSAllowHeaders,
		AllowMethods:  config.Config.CORSAllowMethods,
		ExposeHeaders: config.Config.CORSExposeHeaders,
	}))

	// Compression Middleware
	app.Use(compress.New(compress.Config{
		// refer to gofiber/fiber/blob/v1.14.6/middleware/compress.go#L17
		Level: compress.Level(config.Config.RestCompressLevel),
	}))

	// Swagger docs
	app.Get(config.Config.RestPrefix+"/transactions/docs/*", swagger.Handler)

	// Add version handlers
	app.Get("/version", handlerVersion)
	app.Get("/metadata", handlerMetadata)

	// Add handlers
	rest.TransactionsAddHandlers(app)
	ws.TransactionsAddHandlers(app)

	go app.Listen(":" + config.Config.Port)
}

// Version
// @Summary Show the status of server.
// @Description get the status of server.
// @Tags Version
// @Accept */*
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /version [get]
func handlerVersion(c *fiber.Ctx) error {
	message := map[string]string{
		"version": global.Version,
	}

	json_message, _ := json.Marshal(message)

	return c.SendString(string(json_message))
}

// Metadata
// @Summary Show the status of server.
// @Description get the status of server.
// @Tags Version
// @Accept */*
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /metadata [get]
func handlerMetadata(c *fiber.Ctx) error {
	message := map[string]string{
		"version":     global.Version,
		"name":        config.Config.Name,
		"description": "a go api template",
	}

	json_message, _ := json.Marshal(message)

	return c.SendString(string(json_message))
}
