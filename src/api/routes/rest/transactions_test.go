//+build integration

package rest

import (
	"encoding/json"
	"github.com/geometry-labs/icon-transactions/crud"
	"io/ioutil"
	"net/http/httptest"
	"testing"

	fiber "github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/models"
)

func init() {
	config.ReadEnvironment()
}

func TestHandlerGetBlocks(t *testing.T) {
	assert := assert.New(t)

	// Insert block fixtures
	tx := &models.Transaction{}
	crud.GetTransactionModelMongo().RetryCreate(tx)

	app := fiber.New()

	app.Get("/", handlerGetQuery)

	resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	// Read body
	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	var txs []models.Transaction
	err = json.Unmarshal(bytes, &txs)
	assert.Equal(nil, err)

	// Verify body
	assert.NotEqual(0, len(txs[0].Hash))
}
