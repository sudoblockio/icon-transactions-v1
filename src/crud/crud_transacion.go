package crud

import (
	"strings"
	"sync"

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

// Select - select from transactions table
func (m *TransactionModel) Select(
	limit int,
	skip int,
  hash string,
  from string,
  to string,
) []models.Transaction {
	db := m.db

	// Latest transactions first
	db = db.Order("block_number desc")

	// Limit is required and defaulted to 1
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	// Hash
	if hash != "" {
		db = db.Where("hash = ?", hash)
	}

	// from
	if from != "" {
		db = db.Where("from_address = ?", from)
	}

	// to
	if hash != "" {
		db = db.Where("to_address = ?", to)
	}

	transactions := []models.Transaction{}
	db.Find(&transactions)

	return transactions
}

// StartTransactionLoader starts loader
func StartTransactionLoader() {
	go func() {
		var transaction *models.Transaction
		postgresLoaderChan := GetTransactionModel().WriteChan

		for {
			// Read transaction
			transaction = <-postgresLoaderChan

			// Load transaction to database
			GetTransactionModel().Insert(transaction)

      zap.S().Debugf("Loader Transaction: Loaded in postgres table Transactions, Block Number: %d", transaction.BlockNumber)
		}
	}()
}
