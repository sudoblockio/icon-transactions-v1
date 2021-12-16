package crud

import (
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/models"
)

// TransactionCountByAddressIndexModel - type for address table model
type TransactionCountByAddressIndexModel struct {
	db            *gorm.DB
	model         *models.TransactionCountByAddressIndex
	modelORM      *models.TransactionCountByAddressIndexORM
	LoaderChannel chan *models.TransactionCountByAddressIndex
}

var transactionCountByAddressIndexModel *TransactionCountByAddressIndexModel
var transactionCountByAddressIndexModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetTransactionCountByAddressIndexModel() *TransactionCountByAddressIndexModel {
	transactionCountByAddressIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionCountByAddressIndexModel = &TransactionCountByAddressIndexModel{
			db:            dbConn,
			model:         &models.TransactionCountByAddressIndex{},
			LoaderChannel: make(chan *models.TransactionCountByAddressIndex, 1),
		}

		err := transactionCountByAddressIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionCountByAddressIndexModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return transactionCountByAddressIndexModel
}

// Migrate - migrate transactionCountByAddressIndexs table
func (m *TransactionCountByAddressIndexModel) Migrate() error {
	// Only using TransactionCountByAddressIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// CountByAddress - Count transactionCountByIndex by address
func (m *TransactionCountByAddressIndexModel) SelectTransactionHashesByAddress(
	limit int,
	skip int,
	address string,
) (*[]string, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCountByAddressIndex{})

	// Latest transactions first
	db = db.Order("block_number desc")

	// Address
	db = db.Where("address = ?", address)

	// Select
	db = db.Select("transaction_hash")

	// Limit is required and defaulted to 1
	// Note: Count before setting limit
	db = db.Limit(limit)

	// Skip
	// Note: Count before setting skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	transactionHashes := &[]string{}
	db = db.Find(transactionHashes)

	return transactionHashes, db.Error
}

// CountByAddress - Count transactionCountByIndex by address
// NOTE this function may take very long for some addresses
func (m *TransactionCountByAddressIndexModel) CountByAddress(address string) (int64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCountByAddressIndex{})

	// Address
	db = db.Where("address = ?", address)

	// Count
	var count int64
	db = db.Count(&count)

	return count, db.Error
}

// Insert - Insert transactionCountByIndex into table
func (m *TransactionCountByAddressIndexModel) Insert(transactionCountByAddressIndex *models.TransactionCountByAddressIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCountByAddressIndex{})

	db = db.Create(transactionCountByAddressIndex)

	return db.Error
}
