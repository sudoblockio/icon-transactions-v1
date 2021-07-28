package crud

import (
	"testing"
	"time"
  "context"

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

  ctx, cancel := context.WithCancel(context.Background())
  defer cancel()

	for _, tx := range transactionFixtures {

		insertErr := transactionModel.Insert(ctx, tx)
		assert.Equal(nil, insertErr)
	}
}

func TestTransactionModelSelect(t *testing.T) {
	assert := assert.New(t)

	transactionModel := GetTransactionModel()
	assert.NotEqual(nil, transactionModel)

	// Load fixtures
	transactionFixtures := fixtures.LoadTransactionFixtures()

  ctx, cancel := context.WithCancel(context.Background())
  defer cancel()

	for _, tx := range transactionFixtures {

		insertErr := transactionModel.Insert(ctx, tx)
		assert.Equal(nil, insertErr)
	}

	// Select all transactions
	transactions, err := transactionModel.Select(ctx, int64(len(transactionFixtures)), 0, "", "")
	assert.Equal(len(transactionFixtures), len(transactions))
  assert.Equal(nil, err)

	// Test limit
	transactions, err = transactionModel.Select(ctx, 1, 0, "", "")
	assert.Equal(1, len(transactions))
  assert.Equal(nil, err)

	// Test skip
	transactions, err = transactionModel.Select(ctx, 1, 1, "", "")
	assert.Equal(1, len(transactions))
  assert.Equal(nil, err)

	// Test from
	transactions, err = transactionModel.Select(ctx, 1, 0, "hx02e6bf5860b7d7744ec5050545d10d37c72ac2ef", "")
	assert.Equal(1, len(transactions))
  assert.Equal(nil, err)

	// Test to
	transactions, err = transactionModel.Select(ctx, 1, 0, "", "cx38fd2687b202caf4bd1bda55223578f39dbb6561")
	assert.Equal(1, len(transactions))
  assert.Equal(nil, err)
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

  ctx, cancel := context.WithCancel(context.Background())
  defer cancel()

	// Select all transactions
	transactions, err := transactionModel.Select(ctx, int64(len(transactionFixtures)), 0, "", "")
	assert.Equal(len(transactionFixtures), len(transactions))
  assert.Equal(nil, err)
}
