package rest

import (
	"encoding/json"

	fiber "github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-transactions/api/service"
	"github.com/geometry-labs/icon-transactions/config"
)

func BlocksAddHandlers(app *fiber.App) {

	prefix := config.Config.RestPrefix + "/transactions"

	app.Get(prefix+"/", handlerGetQuery)
}

// Transactions
// @Summary Get Transactions that match the query
// @Description Get all blocks in the system.
// @Tags root
// @Accept */*
// @Produce json
// @Router /transaction [get]
func handlerGetQuery(c *fiber.Ctx) error {
	params := new(service.TransactionsQuery)
	if err := c.QueryParser(params); err != nil {
		zap.S().Warnf("Transactions Get Handler ERROR: %s", err.Error())

		c.Status(422)
		return c.SendString(`{"error": "could not parse query parameters"}`)
	}

	// Get Transactions
	transactions := params.RunQuery()
	if len(transactions) == 0 {
		// No Content
		c.Status(204)
	} else {
		// Success
		c.Status(200)
	}

	body, _ := json.Marshal(transactions)
	return c.SendString(string(body))
}
