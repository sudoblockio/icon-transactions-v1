package rest

import (
	"context"
	"encoding/json"
	"time"

	fiber "github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/crud"
)

type TransactionsQuery struct {
	Limit int64 `query:"limit"`
	Skip  int64 `query:"skip"`

	From string `query:"from"`
	To   string `query:"to"`
	Type string `query:"type"`
}

func TransactionsAddHandlers(app *fiber.App) {

	prefix := config.Config.RestPrefix + "/transactions"

	app.Get(prefix+"/", handlerGetTransactions)
}

// Transactions
// @Summary Get Transactions
// @Description get historical transactions
// @Tags Transactions
// @BasePath /api/v1
// @Accept */*
// @Produce json
// @Param limit query int false "amount of records"
// @Param skip query int false "skip to a record"
// @Param from query string false "find by from address"
// @Param to query string false "find by to address"
// @Param type query string false "find by transaction type"
// @Router /api/v1/transactions [get]
// @Success 200 {object} []models.Transaction
// @Failure 422 {object} map[string]interface{}
func handlerGetTransactions(c *fiber.Ctx) error {
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transactions := crud.GetTransactionModelMongo().Select(
		ctx,
		params.Limit,
		params.Skip,
		params.From,
		params.To,
		params.Type,
	)
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
