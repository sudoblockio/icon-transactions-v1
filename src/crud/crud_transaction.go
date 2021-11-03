package crud

import (
	"reflect"
	"strings"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-transactions/models"
)

// TransactionModel - type for transaction table model
type TransactionModel struct {
	db            *gorm.DB
	model         *models.Transaction
	modelORM      *models.TransactionORM
	LoaderChannel chan *models.Transaction
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
			db:            dbConn,
			model:         &models.Transaction{},
			modelORM:      &models.TransactionORM{},
			LoaderChannel: make(chan *models.Transaction, 1),
		}

		err := transactionModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionModel: Unable migrate postgres table: ", err.Error())
		}

		StartTransactionLoader()
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

// UpdateOne - update one from transactions table
func (m *TransactionModel) UpdateOne(
	transaction *models.Transaction,
) error {
	db := m.db

	// Set table
	db = db.Model(&models.Transaction{})

	// Hash
	db = db.Where("hash = ?", transaction.Hash)

	// Log Index
	db = db.Where("log_index = ?", transaction.LogIndex)

	db = db.Save(transaction)

	return db.Error
}

// SelectMany - select from transactions table
// Returns: models, total count (if filters), error (if present)
func (m *TransactionModel) SelectMany(
	limit int,
	skip int,
	hash string,
	from string,
	to string,
) (*[]models.Transaction, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.Transaction{})

	// Latest transactions first
	db = db.Order("block_number desc")

	// Hash
	if hash != "" {
		db = db.Where("hash = ?", hash)
	}

	// from
	if from != "" {
		db = db.Where("from_address = ?", from)
	}

	// to
	if to != "" {
		db = db.Where("to_address = ?", to)
	}

	// Limit
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	transactions := &[]models.Transaction{}
	db = db.Find(transactions)

	return transactions, db.Error
}

// SelectManyAPI - select from transactions table
// Returns: models, total count (if filters), error (if present)
func (m *TransactionModel) SelectManyAPI(
	limit int,
	skip int,
	from string,
	to string,
	_type string,
	blockNumber int,
	method string,
) (*[]models.TransactionAPIList, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.Transaction{})

	// Latest transactions first
	db = db.Order("block_number desc")

	// from
	if from != "" {
		db = db.Where("from_address = ?", from)
	}

	// to
	if to != "" {
		db = db.Where("to_address = ?", to)
	}

	// type
	if _type != "" {
		db = db.Where("type = ?", _type)
	}

	// block number
	if blockNumber != 0 {
		db = db.Where("block_number = ?", blockNumber)
	}

	// method
	if method != "" {
		db = db.Where("method = ?", method)
	}

	// Limit is required and defaulted to 1
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	transactions := &[]models.TransactionAPIList{}
	db = db.Find(transactions)

	return transactions, db.Error
}

// SelectManyByAddressAPI - select from transactions table
// Returns: models, total count (if filters), error (if present)
func (m *TransactionModel) SelectManyByAddressAPI(
	limit int,
	skip int,
	address string,
) (*[]models.TransactionAPIList, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.Transaction{})

	// Where clause
	whereClause := `WHERE type='transaction' AND (from_address = '` + address + `' OR to_address = '` + address + `'` + `)`

	// Order By Clause
	orderByClause := "ORDER BY a.block_number DESC"

	db.Raw(formatSelectStatementForceCTE(
		reflect.ValueOf(models.TransactionAPIList{}), // modelValueOf
		reflect.TypeOf(models.TransactionAPIList{}),  // modelTypeOf
		m.modelORM.TableName(),                       // tableName
		whereClause,                                  // whereClause
		orderByClause,                                // orderByClause
		limit,                                        // limit
		skip,                                         // skip
	))

	transactions := &[]models.TransactionAPIList{}
	db = db.Find(transactions)

	return transactions, db.Error
}

// SelectManyInternalAPI- select many internal transaction table
// Returns: models, total count (if filters), error (if present)
func (m *TransactionModel) SelectManyInternalAPI(
	limit int,
	skip int,
	hash string,
) (*[]models.TransactionInternalAPIList, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.Transaction{})

	// Latest transactions first
	db = db.Order("block_number desc")

	// Hash
	if hash != "" {
		db = db.Where("hash = ?", hash)
	}

	// Internal transactions only
	db = db.Where("type = ?", "log")

	// Limit is required and defaulted to 1
	// Note: Count before setting limit
	db = db.Limit(limit)

	// Skip
	// Note: Count before setting skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	transactions := &[]models.TransactionInternalAPIList{}
	db = db.Find(transactions)

	return transactions, db.Error
}

// SelectManyInternalByAddressAPI - select from internal transactions table
// Returns: models, total count (if filters), error (if present)
func (m *TransactionModel) SelectManyInternalByAddressAPI(
	limit int,
	skip int,
	address string,
) (*[]models.TransactionInternalAPIList, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.Transaction{})

	// Latest transactions first
	db = db.Order("block_number desc")

	// Internal transactions only
	db = db.Where("type = ?", "log")

	// From address or to address
	db = db.Where("from_address = ? OR to_address = ?", address, address)

	// Limit is required and defaulted to 1
	// Note: Count before setting limit
	db = db.Limit(limit)

	// Skip
	// Note: Count before setting skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	transactions := &[]models.TransactionInternalAPIList{}
	db = db.Find(transactions)

	return transactions, db.Error
}

// SelectOne - select from transactions table
func (m *TransactionModel) SelectOne(
	hash string,
	logIndex int32, // Used for internal transactions
) (*models.Transaction, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.Transaction{})

	// Hash
	db = db.Where("hash = ?", hash)

	// Log Index
	db = db.Where("log_index = ?", logIndex)

	// Type, always transaction
	db = db.Where("type= ?", "transaction")

	transaction := &models.Transaction{}
	db = db.First(transaction)

	return transaction, db.Error
}

// SelectOne - select from transactions table
func (m *TransactionModel) SelectOneAPI(
	hash string,
	logIndex int32, // Used for internal transactions
) (*models.TransactionAPIDetail, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.Transaction{})

	// Hash
	db = db.Where("hash = ?", hash)

	// Log Index
	db = db.Where("log_index = ?", logIndex)

	transaction := &models.TransactionAPIDetail{}
	db = db.First(transaction)

	return transaction, db.Error
}

func (m *TransactionModel) UpsertOne(
	transaction *models.Transaction,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*transaction),
		reflect.TypeOf(*transaction),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "hash"}, {Name: "log_index"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(transaction)

	return db.Error
}

// StartTransactionLoader starts loader
func StartTransactionLoader() {
	go func() {
		postgresLoaderChan := GetTransactionModel().LoaderChannel

		for {
			// Read transaction
			newTransaction := <-postgresLoaderChan

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetTransactionModel().UpsertOne(newTransaction)
			zap.S().Debug("Loader=Transaction, Hash=", newTransaction.Hash, " LogIndex=", newTransaction.LogIndex, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Info("Loader=Transaction, Hash=", newTransaction.Hash, " LogIndex=", newTransaction.LogIndex, " - FATAL")
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
