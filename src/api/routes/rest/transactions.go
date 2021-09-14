package rest

import (
	"encoding/json"
	"regexp"
	"strconv"

	fiber "github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/crud"
)

type TransactionsQuery struct {
	Limit int `query:"limit"`
	Skip  int `query:"skip"`

	Hash string `query:"hash"`
	From string `query:"from"`
	To   string `query:"to"`
	Type string `query:"type"`
}

func TransactionsAddHandlers(app *fiber.App) {

	prefix := config.Config.RestPrefix + "/transactions"

	app.Get(prefix+"/", handlerGetTransactions)
	app.Get(prefix+"/:hash", handlerGetTransactionDetails)
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
// @Param hash query string false "find by hash"
// @Param from query string false "find by from address"
// @Param to query string false "find by to address"
// @Param type query string false "find by type"
// @Router /api/v1/transactions [get]
// @Success 200 {object} []models.TransactionAPIList
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

	// Check Params
	if params.Limit < 1 || params.Limit > config.Config.MaxPageSize {
		c.Status(422)
		return c.SendString(`{"error": "limit must be greater than 0 and less than 101"}`)
	}

	// NOTE: TEMP casting string types for type field
	if params.Type == "regular" {
		params.Type = "transaction"
	} else if params.Type == "internal" {
		params.Type = "log"
	}

	// Get Transactions
	transactions, count, err := crud.GetTransactionModel().SelectManyAPI(
		params.Limit,
		params.Skip,
		params.Hash,
		params.From,
		params.To,
		params.Type,
	)
	if err != nil {
		zap.S().Warnf("Transactions CRUD ERROR: %s", err.Error())
		c.Status(500)
		return c.SendString(`{"error": "could not retrieve transactions"}`)
	}

	if len(*transactions) == 0 {
		// No Content
		c.Status(204)
	}

	// Set X-TOTAL-COUNT
	if count != -1 {
		// Filters given, count some
		c.Append("X-TOTAL-COUNT", strconv.FormatInt(count, 10))
	} else {
		// No filters given, count all
		// Total count in the transaction_counts table
		counter, err := crud.GetTransactionCountModel().SelectLargestCount()
		if err != nil {
			counter = 0
			zap.S().Warn("Could not retrieve transaction count: ", err.Error())
		}

		c.Append("X-TOTAL-COUNT", strconv.FormatUint(counter, 10))
	}

	body, _ := json.Marshal(&transactions)
	return c.SendString(string(body))
}

// Transaction Details
// @Summary Get Transaction Details
// @Description get details of a transaction
// @Tags Transactions
// @BasePath /api/v1
// @Accept */*
// @Produce json
// @Param hash path string true "transaction hash"
// @Router /api/v1/transactions/{hash} [get]
// @Success 200 {object} models.TransactionAPIDetail
// @Failure 422 {object} map[string]interface{}
func handlerGetTransactionDetails(c *fiber.Ctx) error {
	hash := c.Params("hash")

	if hash == "" {
		c.Status(422)
		return c.SendString(`{"error": "hash required"}`)
	}

	// Is hash?
	isHash, err := regexp.Match("0x([0-9a-fA-F]*)", []byte(hash))
	if err != nil {
		c.Status(422)
		return c.SendString(`{"error": "invalid hash"}`)
	}
	if isHash == true {
		// ID is Hash
		transaction, err := crud.GetTransactionModel().SelectOneAPI(hash, -1)
		if err != nil {
			c.Status(404)
			return c.SendString(`{"error": "no transaction found"}`)
		}

		body, _ := json.Marshal(&transaction)
		return c.SendString(string(body))
	}

	c.Status(422)
	return c.SendString(`{"error": "invalid hash"}`)
}
