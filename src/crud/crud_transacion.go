package crud

import (
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/models"
)

// TransactionModel - type for transaction table model
type TransactionModel struct {
	db        *gorm.DB
	model     *models.Transaction
	modelORM  *models.TransactionORM
	WriteChan chan *models.Transaction
}

var transactionModel *TransactionModel
var transactionModelOnce sync.Once

// GetTransactionModel - create and/or return the transactions table model
func GetTransactionModel() *TransactionModel {
	transactionModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionModel = &TransactionModel{
			db:        dbConn,
			model:     &models.Transaction{},
			WriteChan: make(chan *models.Transaction, 1),
		}

		err := transactionModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return transactionModel
}

// Migrate - migrate transactions table
func (m *TransactionModel) Migrate() error {
	// Only using TransactionRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert transaction into table
func (m *TransactionModel) Insert(transaction *models.Transaction) error {

	err := backoff.Retry(func() error {
		query := m.db.Create(transaction)
		if query.Error != nil && !strings.Contains(query.Error.Error(), "duplicate key value violates unique constraint") {
			zap.S().Warn("POSTGRES Insert Error : ", query.Error.Error())
			return query.Error
		}

		return nil
	}, backoff.NewExponentialBackOff())

	return err
}

// SelectMany - select from transactions table
// Returns: models, total count (if filters), error (if present)
func (m *TransactionModel) SelectMany(
	limit int,
	skip int,
	hash string,
	from string,
	to string,
) ([]models.TransactionAPI, int64, error) {
	db := m.db
	computeCount := false

	// Set table
	db = db.Model(&[]models.Transaction{})

	// Latest transactions first
	db = db.Order("block_number desc")

	// Hash
	if hash != "" {
		computeCount = true
		db = db.Where("hash = ?", hash)
	}

	// from
	if from != "" {
		computeCount = true
		db = db.Where("from_address = ?", from)
	}

	// to
	if to != "" {
		computeCount = true
		db = db.Where("to_address = ?", to)
	}

	// Count, if needed
	count := int64(-1)
	if computeCount {
		db.Count(&count)
	}

	// Limit is required and defaulted to 1
	// Note: Count before setting limit
	db = db.Limit(limit)

	// Skip
	// Note: Count before setting skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	transactions := []models.TransactionAPI{}
	db = db.Find(&transactions)

	return transactions, count, db.Error
}

// SelectOne - select from transactions table
func (m *TransactionModel) SelectOne(
	hash string,
) (models.Transaction, error) {
	db := m.db

	// Hash
	if hash != "" {
		db = db.Where("hash = ?", hash)
	}

	// No internal transactions
	db = db.Where("type = ?", "transaction")

	transaction := models.Transaction{}
	db = db.First(&transaction)

	return transaction, db.Error
}

// StartTransactionLoader starts loader
func StartTransactionLoader() {
	go func() {
		postgresLoaderChan := GetTransactionModel().WriteChan

		for {
			// Read transaction
			newTransaction := <-postgresLoaderChan

			// Load transaction to database
			GetTransactionModel().Insert(newTransaction)

			// Check current state
			for {
				// Wait for postgres to set state before processing more messages

				checkTransaction, err := GetTransactionModel().SelectOne(newTransaction.Hash)
				if err != nil {
					zap.S().Warn("State check error: ", err.Error())
					zap.S().Warn("Waiting 100ms...")
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// check all fields
				if checkTransaction.Type == newTransaction.Type &&
					checkTransaction.Version == newTransaction.Version &&
					checkTransaction.FromAddress == newTransaction.FromAddress &&
					checkTransaction.ToAddress == newTransaction.ToAddress &&
					checkTransaction.Value == newTransaction.Value &&
					checkTransaction.StepLimit == newTransaction.StepLimit &&
					checkTransaction.Timestamp == newTransaction.Timestamp &&
					checkTransaction.BlockTimestamp == newTransaction.BlockTimestamp &&
					checkTransaction.Nid == newTransaction.Nid &&
					checkTransaction.Nonce == newTransaction.Nonce &&
					checkTransaction.Hash == newTransaction.Hash &&
					checkTransaction.TransactionIndex == newTransaction.TransactionIndex &&
					checkTransaction.BlockHash == newTransaction.BlockHash &&
					checkTransaction.BlockNumber == newTransaction.BlockNumber &&
					checkTransaction.Fee == newTransaction.Fee &&
					checkTransaction.Signature == newTransaction.Signature &&
					checkTransaction.DataType == newTransaction.DataType &&
					checkTransaction.Data == newTransaction.Data &&
					checkTransaction.ReceiptCumulativeStepUsed == newTransaction.ReceiptCumulativeStepUsed &&
					checkTransaction.ReceiptStepUsed == newTransaction.ReceiptStepUsed &&
					checkTransaction.ReceiptScoreAddress == newTransaction.ReceiptScoreAddress &&
					checkTransaction.ReceiptLogs == newTransaction.ReceiptLogs &&
					checkTransaction.ReceiptStatus == newTransaction.ReceiptStatus &&
					checkTransaction.ItemId == newTransaction.ItemId &&
					checkTransaction.ItemTimestamp == newTransaction.ItemTimestamp {
					// Success
					break
				} else {
					// Wait
					zap.S().Warn("Models did not match")
					zap.S().Warn("Waiting 100ms...")
					time.Sleep(100 * time.Millisecond)
					continue
				}
			}

			zap.S().Debugf("Loader Transaction: Loaded in postgres table Transactions, Block Number: %d", newTransaction.BlockNumber)
		}
	}()
}
