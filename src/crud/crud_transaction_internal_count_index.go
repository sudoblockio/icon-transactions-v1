package crud

import (
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/models"
)

// TransactionInternalCountIndexModel - type for address table model
type TransactionInternalCountIndexModel struct {
	db            *gorm.DB
	model         *models.TransactionInternalCountIndex
	modelORM      *models.TransactionInternalCountIndexORM
	LoaderChannel chan *models.TransactionInternalCountIndex
}

var transactionInternalCountIndexModel *TransactionInternalCountIndexModel
var transactionInternalCountIndexModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetTransactionInternalCountIndexModel() *TransactionInternalCountIndexModel {
	transactionInternalCountIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionInternalCountIndexModel = &TransactionInternalCountIndexModel{
			db:            dbConn,
			model:         &models.TransactionInternalCountIndex{},
			LoaderChannel: make(chan *models.TransactionInternalCountIndex, 1),
		}

		err := transactionInternalCountIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionInternalCountIndexModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return transactionInternalCountIndexModel
}

// Migrate - migrate transactionInternalCountIndexs table
func (m *TransactionInternalCountIndexModel) Migrate() error {
	// Only using TransactionInternalCountIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert transactionCountByIndex into table
func (m *TransactionInternalCountIndexModel) Insert(transactionInternalCountIndex *models.TransactionInternalCountIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionInternalCountIndex{})

	db = db.Create(transactionInternalCountIndex)

	return db.Error
}

// Select - select from transactionInternalCountIndexs table
func (m *TransactionInternalCountIndexModel) SelectOne(transactionHash string) (*models.TransactionInternalCountIndex, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionInternalCountIndex{})

	// Address
	db = db.Where("transaction_hash = ?", transactionHash)

	transactionInternalCountIndex := &models.TransactionInternalCountIndex{}
	db = db.First(transactionInternalCountIndex)

	return transactionInternalCountIndex, db.Error
}
