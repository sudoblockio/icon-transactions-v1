package tests

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// List test
func TestTransactionsTokenTransferEndpointList(t *testing.T) {
	assert := assert.New(t)

	transactionsServiceURL := os.Getenv("TRANSACTIONS_SERVICE_URL")
	if transactionsServiceURL == "" {
		transactionsServiceURL = "http://localhost:8000"
	}
	transactionsServiceRestPrefx := os.Getenv("TRANSACTIONS_SERVICE_REST_PREFIX")
	if transactionsServiceRestPrefx == "" {
		transactionsServiceRestPrefx = "/api/v1"
	}

	resp, err := http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions/token-transfers")
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap := make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))
}

// List limit and skip test
func TestTransactionsTokenTransferEndpointListLimitSkip(t *testing.T) {
	assert := assert.New(t)

	transactionsServiceURL := os.Getenv("TRANSACTIONS_SERVICE_URL")
	if transactionsServiceURL == "" {
		transactionsServiceURL = "http://localhost:8000"
	}
	transactionsServiceRestPrefx := os.Getenv("TRANSACTIONS_SERVICE_REST_PREFIX")
	if transactionsServiceRestPrefx == "" {
		transactionsServiceRestPrefx = "/api/v1"
	}

	resp, err := http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions/token-transfers?limit=20&skip=20")
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap := make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))
}

// List number test
func TestTransactionsTokenTransferEndpointListBlockNumber(t *testing.T) {
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
	resp, err := http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions/token-transfers?limit=1")
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap := make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))

	// Get testable number
	transactionNumber := strconv.FormatUint(uint64(bodyMap[0].(map[string]interface{})["block_number"].(float64)), 10)

	// Test number
	resp, err = http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions/token-transfers?block_number=" + transactionNumber)
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	bytes, err = ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap = make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))
}

// List from address test
func TestTransactionsTokenTransferEndpointListFromAddress(t *testing.T) {
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
	resp, err := http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions/token-transfers?limit=1")
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap := make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))

	// Get testable address
	transactionFromAddress := bodyMap[0].(map[string]interface{})["from_address"].(string)

	// Test number
	resp, err = http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions/token-transfers?from=" + transactionFromAddress)
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	bytes, err = ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap = make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))
}

// List to address test
func TestTransactionsTokenTransferEndpointListToAddress(t *testing.T) {
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
	resp, err := http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions/token-transfers?limit=1")
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap := make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))

	// Get testable address
	transactionToAddress := bodyMap[0].(map[string]interface{})["to_address"].(string)

	// Test number
	resp, err = http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions/token-transfers?to=" + transactionToAddress)
	assert.Equal(nil, err)
	assert.Equal(200, resp.StatusCode)

	defer resp.Body.Close()

	bytes, err = ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap = make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))
}
