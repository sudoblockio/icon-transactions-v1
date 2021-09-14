//+build unit

package crud

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/fixtures"
	"github.com/geometry-labs/icon-transactions/logging"
)

func init() {
	// Read env
	// Defaults should work
	config.ReadEnvironment()

	// Set up logging
	logging.Init()
}

func TestGetTransactionModel(t *testing.T) {
	assert := assert.New(t)

	transactionModel := GetTransactionModel()
	assert.NotEqual(nil, transactionModel)
}

func TestTransactionModelInsert(t *testing.T) {
	assert := assert.New(t)

	transactionModel := GetTransactionModel()
	assert.NotEqual(nil, transactionModel)

	// Load fixtures
	transactionFixtures := fixtures.LoadTransactionFixtures()

	for _, tx := range transactionFixtures {

		insertErr := transactionModel.Insert(tx)
		assert.Equal(nil, insertErr)
	}
}

func TestTransactionModelSelectMany(t *testing.T) {
	assert := assert.New(t)

	transactionModel := GetTransactionModel()
	assert.NotEqual(nil, transactionModel)

	// Load fixtures
	transactionFixtures := fixtures.LoadTransactionFixtures()

	for _, tx := range transactionFixtures {

		insertErr := transactionModel.Insert(tx)
		assert.Equal(nil, insertErr)
	}

	// SelectMany all transactions
	transactions, _, err := transactionModel.SelectMany(len(transactionFixtures), 0, "", "", "")
	assert.Equal(len(transactionFixtures), len(transactions))
	assert.Equal(nil, err)

	// Test limit
	transactions, _, err = transactionModel.SelectMany(1, 0, "", "", "")
	assert.Equal(1, len(transactions))
	assert.Equal(nil, err)

	// Test skip
	transactions, _, err = transactionModel.SelectMany(1, 1, "", "", "")
	assert.Equal(1, len(transactions))
	assert.Equal(nil, err)

	// Test hash
	transactions, _, err = transactionModel.SelectMany(1, 0, "0x18094ca8e7f5cc52511c36a8c85f56c8788b8802025a8cbcd84fb0f5b5ea7d82", "", "")
	assert.Equal(1, len(transactions))
	assert.Equal(nil, err)

	// Test from
	transactions, _, err = transactionModel.SelectMany(1, 0, "", "hx02e6bf5860b7d7744ec5050545d10d37c72ac2ef", "")
	assert.Equal(1, len(transactions))
	assert.Equal(nil, err)

	// Test to
	transactions, _, err = transactionModel.SelectMany(1, 0, "", "", "cx38fd2687b202caf4bd1bda55223578f39dbb6561")
	assert.Equal(1, len(transactions))
	assert.Equal(nil, err)
}

func TestTransactionModelSelectOne(t *testing.T) {
	assert := assert.New(t)

	transactionModel := GetTransactionModel()
	assert.NotEqual(nil, transactionModel)

	migrateErr := transactionModel.Migrate()
	assert.Equal(nil, migrateErr)

	// Load fixtures
	transactionFixtures := fixtures.LoadTransactionFixtures()
	for _, transaction := range transactionFixtures {
		insertErr := transactionModel.Insert(transaction)
		assert.Equal(nil, insertErr)
	}

	// Test hash
	transaction, err := transactionModel.SelectOne("0x18094ca8e7f5cc52511c36a8c85f56c8788b8802025a8cbcd84fb0f5b5ea7d82", -1)
	assert.Equal(nil, err)
	assert.NotEqual(nil, transaction)
}

func TestTransactionModelLoader(t *testing.T) {
	assert := assert.New(t)

	transactionModel := GetTransactionModel()
	assert.NotEqual(nil, transactionModel)

	// Load fixtures
	transactionFixtures := fixtures.LoadTransactionFixtures()

	// Start loader
	go StartTransactionLoader()

	// Write to loader channel
	go func() {
		for _, fixture := range transactionFixtures {
			transactionModel.WriteChan <- fixture
		}
	}()

	// Wait for inserts
	time.Sleep(5)

	// Select all transactions
	transactions, _, err := transactionModel.SelectMany(len(transactionFixtures), 0, "", "", "")
	assert.Equal(len(transactionFixtures), len(transactions))
	assert.Equal(nil, err)
}
