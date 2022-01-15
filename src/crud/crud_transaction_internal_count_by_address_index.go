package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-transactions/models"
)

// TransactionInternalCountByAddressIndexModel - type for address table model
type TransactionInternalCountByAddressIndexModel struct {
	db            *gorm.DB
	model         *models.TransactionInternalCountByAddressIndex
	modelORM      *models.TransactionInternalCountByAddressIndexORM
	LoaderChannel chan *models.TransactionInternalCountByAddressIndex
}

var transactionInternalCountByAddressIndexModel *TransactionInternalCountByAddressIndexModel
var transactionInternalCountByAddressIndexModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetTransactionInternalCountByAddressIndexModel() *TransactionInternalCountByAddressIndexModel {
	transactionInternalCountByAddressIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionInternalCountByAddressIndexModel = &TransactionInternalCountByAddressIndexModel{
			db:            dbConn,
			model:         &models.TransactionInternalCountByAddressIndex{},
			LoaderChannel: make(chan *models.TransactionInternalCountByAddressIndex, 1),
		}

		err := transactionInternalCountByAddressIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionInternalCountByAddressIndexModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return transactionInternalCountByAddressIndexModel
}

// Migrate - migrate transactionInternalCountByAddressIndexs table
func (m *TransactionInternalCountByAddressIndexModel) Migrate() error {
	// Only using TransactionInternalCountByAddressIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// CountByAddress - Count transactionCountByIndex by address
// NOTE this function may take very long for some addresses
func (m *TransactionInternalCountByAddressIndexModel) CountByAddress(address string) (int64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionInternalCountByAddressIndex{})

	// Address
	db = db.Where("address = ?", address)

	// Count
	var count int64
	db = db.Count(&count)

	return count, db.Error
}

// Insert - Insert transactionCountByIndex into table
func (m *TransactionInternalCountByAddressIndexModel) Insert(transactionInternalCountByAddressIndex *models.TransactionInternalCountByAddressIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionInternalCountByAddressIndex{})

	db = db.Create(transactionInternalCountByAddressIndex)

	return db.Error
}

func (m *TransactionInternalCountByAddressIndexModel) UpsertOne(
	transactionInternalCountByAddressIndex *models.TransactionInternalCountByAddressIndex,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*transactionInternalCountByAddressIndex),
		reflect.TypeOf(*transactionInternalCountByAddressIndex),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}, {Name: "log_index"}, {Name: "address"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(transactionInternalCountByAddressIndex)

	return db.Error
}
