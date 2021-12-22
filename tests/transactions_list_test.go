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
func TestTransactionsEndpointList(t *testing.T) {
	assert := assert.New(t)

	transactionsServiceURL := os.Getenv("TRANSACTIONS_SERVICE_URL")
	if transactionsServiceURL == "" {
		transactionsServiceURL = "http://localhost:8000"
	}
	transactionsServiceRestPrefx := os.Getenv("TRANSACTIONS_SERVICE_REST_PREFIX")
	if transactionsServiceRestPrefx == "" {
		transactionsServiceRestPrefx = "/api/v1"
	}

	resp, err := http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions")
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
func TestTransactionsEndpointListLimitSkip(t *testing.T) {
	assert := assert.New(t)

	transactionsServiceURL := os.Getenv("TRANSACTIONS_SERVICE_URL")
	if transactionsServiceURL == "" {
		transactionsServiceURL = "http://localhost:8000"
	}
	transactionsServiceRestPrefx := os.Getenv("TRANSACTIONS_SERVICE_REST_PREFIX")
	if transactionsServiceRestPrefx == "" {
		transactionsServiceRestPrefx = "/api/v1"
	}

	resp, err := http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions?limit=20&skip=20")
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
func TestTransactionsEndpointListBlockNumber(t *testing.T) {
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

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap := make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))

	// Get testable number
	transactionNumber := strconv.FormatUint(uint64(bodyMap[0].(map[string]interface{})["block_number"].(float64)), 10)

	// Test number
	resp, err = http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions?block_number=" + transactionNumber)
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
func TestTransactionsEndpointListFromAddress(t *testing.T) {
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

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap := make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))

	// Get testable address
	transactionFromAddress := bodyMap[0].(map[string]interface{})["from_address"].(string)

	// Test number
	resp, err = http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions?from=" + transactionFromAddress)
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
func TestTransactionsEndpointListToAddress(t *testing.T) {
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

	bytes, err := ioutil.ReadAll(resp.Body)
	assert.Equal(nil, err)

	bodyMap := make([]interface{}, 0)
	err = json.Unmarshal(bytes, &bodyMap)
	assert.Equal(nil, err)
	assert.NotEqual(0, len(bodyMap))

	// Get testable address
	transactionToAddress := bodyMap[0].(map[string]interface{})["to_address"].(string)

	// Test number
	resp, err = http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions?to=" + transactionToAddress)
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

// List to type test
func TestTransactionsEndpointListType(t *testing.T) {
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
	resp, err := http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions?type=regular")
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

// List method test
func TestTransactionsEndpointListMethod(t *testing.T) {
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
	transactionMethod := ""
	skip := 0
	for {
		// Keep interating skip until non-null method
		resp, err := http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions?limit=1&skip=" + strconv.Itoa(skip))
		assert.Equal(nil, err)
		assert.Equal(200, resp.StatusCode)

		defer resp.Body.Close()

		bytes, err := ioutil.ReadAll(resp.Body)
		assert.Equal(nil, err)

		bodyMap := make([]interface{}, 0)
		err = json.Unmarshal(bytes, &bodyMap)
		assert.Equal(nil, err)
		assert.NotEqual(0, len(bodyMap))

		// Get testable method
		transactionMethod = bodyMap[0].(map[string]interface{})["method"].(string)
		if transactionMethod != "" {
			break
		}

		skip++
	}

	// Test method
	resp, err := http.Get(transactionsServiceURL + transactionsServiceRestPrefx + "/transactions?method=" + transactionMethod)
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
