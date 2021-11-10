package tests

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Address test
func TestTransactionsEndpointAddress(t *testing.T) {
	assert := assert.New(t)

	transactionsServiceURL := os.Getenv("TRANSACTIONS_SERVICE_URL")
	if transactionsServiceURL == "" {
		transactionsServiceURL = "http://localhost:8000"
	}
	transactionsServiceRestPrefx := os.Getenv("TRANSACTIONS_SERVICE_REST_PREFIX")
	if transactionsServiceRestPrefx == "" {
		transactionsServiceRestPrefx = "/api/v1"
	}

	// Get latest transaction
	resp, err := http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions?limit=1")
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	// Test headers
	assert.NotEqual("0", resp.Header.Get("X-TOTAL-COUNT"))

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap := make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))

	// Get testable address
	transactionFromAddress := bodyMap[0].(map[string]interface{})["from_address"].(string)

	// Test number
	resp, err = http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions/address/" + transactionFromAddress)
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	// Test headers
	assert.NotEqual("0", resp.Header.Get("X-TOTAL-COUNT"))

	bytes, err = ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap = make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))
}
