package rest

import (
	"encoding/json"
	"github.com/geometry-labs/icon-transactions/crud"

	fiber "github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-transactions/config"
)

type TransactionsQuery struct {
	Limit int `query:"limit"`
	Skip  int `query:"skip"`

	From string `query:"from"`
	To   string `query:"to"`
	Type string `query:"type"`
}

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
	params := new(TransactionsQuery)
	if err := c.QueryParser(params); err != nil {
		zap.S().Warnf("Transactions Get Handler ERROR: %s", err.Error())

		c.Status(422)
		return c.SendString(`{"error": "could not parse query parameters"}`)
	}

	// Default Params
	if params.Limit <= 0 {
		params.Limit = 1
	}

	// Get Transactions
	transactions := crud.GetTransactionModelMongo().Select(params.Limit, params.Skip, params.From, params.To, params.Type)
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
