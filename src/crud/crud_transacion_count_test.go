//+build unit

package crud

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/logging"
	"github.com/geometry-labs/icon-transactions/models"
)

func init() {
	// Read env
	// Defaults should work
	config.ReadEnvironment()

	// Set up logging
	logging.Init()
}

func TestGetTransactionCountModel(t *testing.T) {
	assert := assert.New(t)

	transactionCountModel := GetTransactionCountModel()
	assert.NotEqual(nil, transactionCountModel)
}

func TestTransactionCountModelInsert(t *testing.T) {
	assert := assert.New(t)

	transactionCountModel := GetTransactionCountModel()
	assert.NotEqual(nil, transactionCountModel)

	transactionCountFixture := &models.TransactionCount{
		Count: 1,
		Id:    10,
	}

	// Clear entry
	transactionCountModel.Delete(*transactionCountFixture)

	insertErr := transactionCountModel.Insert(transactionCountFixture)
	assert.Equal(nil, insertErr)
}

func TestTransactionCountModelSelect(t *testing.T) {
	assert := assert.New(t)

	transactionCountModel := GetTransactionCountModel()
	assert.NotEqual(nil, transactionCountModel)

	// Load fixture
	transactionCountFixture := &models.TransactionCount{
		Count: 1,
		Id:    10,
	}

	// Clear entry
	transactionCountModel.Delete(*transactionCountFixture)

	insertErr := transactionCountModel.Insert(transactionCountFixture)
	assert.Equal(nil, insertErr)

	// Select TransactionCount
	result, err := transactionCountModel.Select()
	assert.Equal(transactionCountFixture.Count, result.Count)
	assert.Equal(transactionCountFixture.Id, result.Id)
	assert.Equal(nil, err)
}

func TestTransactionCountModelUpdate(t *testing.T) {
	assert := assert.New(t)

	transactionCountModel := GetTransactionCountModel()
	assert.NotEqual(nil, transactionCountModel)

	// Load fixture
	transactionCountFixture := &models.TransactionCount{
		Count: 1,
		Id:    10,
	}

	// Clear entry
	transactionCountModel.Delete(*transactionCountFixture)

	insertErr := transactionCountModel.Insert(transactionCountFixture)
	assert.Equal(nil, insertErr)

	// Select TransactionCount
	result, err := transactionCountModel.Select()
	assert.Equal(transactionCountFixture.Count, result.Count)
	assert.Equal(transactionCountFixture.Id, result.Id)
	assert.Equal(nil, err)

	// Update TransactionCount
	transactionCountFixture = &models.TransactionCount{
		Count: 10,
		Id:    10,
	}
	insertErr = transactionCountModel.Update(transactionCountFixture)
	assert.Equal(nil, insertErr)

	// Select TransactionCount
	result, err = transactionCountModel.Select()
	assert.Equal(transactionCountFixture.Count, result.Count)
	assert.Equal(transactionCountFixture.Id, result.Id)
	assert.Equal(nil, err)
}

func TestTransactionCountModelLoader(t *testing.T) {
	assert := assert.New(t)

	transactionCountModel := GetTransactionCountModel()
	assert.NotEqual(nil, transactionCountModel)

	// Load fixture
	transactionCountFixture := &models.TransactionCount{
		Count: 1,
		Id:    10,
	}

	// Clear entry
	transactionCountModel.Delete(*transactionCountFixture)

	// Start loader
	StartTransactionCountLoader()

	// Write to loader channel
	go func() {
		for {
			transactionCountModel.WriteChan <- transactionCountFixture
			time.Sleep(1)
		}
	}()

	// Wait for inserts
	time.Sleep(5)
}
