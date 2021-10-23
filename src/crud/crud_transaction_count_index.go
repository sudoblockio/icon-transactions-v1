package crud

import (
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/models"
)

// TransactionCountIndexModel - type for address table model
type TransactionCountIndexModel struct {
	db            *gorm.DB
	model         *models.TransactionCountIndex
	modelORM      *models.TransactionCountIndexORM
	LoaderChannel chan *models.TransactionCountIndex
}

var transactionCountIndexModel *TransactionCountIndexModel
var transactionCountIndexModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetTransactionCountIndexModel() *TransactionCountIndexModel {
	transactionCountIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionCountIndexModel = &TransactionCountIndexModel{
			db:            dbConn,
			model:         &models.TransactionCountIndex{},
			LoaderChannel: make(chan *models.TransactionCountIndex, 1),
		}

		err := transactionCountIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionCountIndexModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return transactionCountIndexModel
}

// Migrate - migrate transactionCountIndexs table
func (m *TransactionCountIndexModel) Migrate() error {
	// Only using TransactionCountIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert transactionCountByIndex into table
func (m *TransactionCountIndexModel) Insert(transactionCountIndex *models.TransactionCountIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCountIndex{})

	db = db.Create(transactionCountIndex)

	return db.Error
}

// Select - select from transactionCountIndexs table
func (m *TransactionCountIndexModel) SelectOne(transactionHash string) (*models.TransactionCountIndex, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCountIndex{})

	// Address
	db = db.Where("transaction_hash = ?", transactionHash)

	transactionCountIndex := &models.TransactionCountIndex{}
	db = db.First(transactionCountIndex)

	return transactionCountIndex, db.Error
}