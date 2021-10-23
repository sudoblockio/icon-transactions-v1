package crud

import (
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/models"
)

// TransactionTokenTransferCountIndexModel - type for address table model
type TransactionTokenTransferCountIndexModel struct {
	db            *gorm.DB
	model         *models.TransactionTokenTransferCountIndex
	modelORM      *models.TransactionTokenTransferCountIndexORM
	LoaderChannel chan *models.TransactionTokenTransferCountIndex
}

var transactionTokenTransaferCountIndexModel *TransactionTokenTransferCountIndexModel
var transactionTokenTransaferCountIndexModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetTransactionTokenTransferCountIndexModel() *TransactionTokenTransferCountIndexModel {
	transactionTokenTransaferCountIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionTokenTransaferCountIndexModel = &TransactionTokenTransferCountIndexModel{
			db:            dbConn,
			model:         &models.TransactionTokenTransferCountIndex{},
			LoaderChannel: make(chan *models.TransactionTokenTransferCountIndex, 1),
		}

		err := transactionTokenTransaferCountIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionTokenTransferCountIndexModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return transactionTokenTransaferCountIndexModel
}

// Migrate - migrate transactionTokenTransaferCountIndexs table
func (m *TransactionTokenTransferCountIndexModel) Migrate() error {
	// Only using TransactionTokenTransferCountIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert transactionCountByIndex into table
func (m *TransactionTokenTransferCountIndexModel) Insert(transactionTokenTransaferCountIndex *models.TransactionTokenTransferCountIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionTokenTransferCountIndex{})

	db = db.Create(transactionTokenTransaferCountIndex)

	return db.Error
}
