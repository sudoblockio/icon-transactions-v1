package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/models"
)

// TransactionCountModel - type for log table model
type TransactionCountModel struct {
	db            *gorm.DB
	model         *models.TransactionCount
	modelORM      *models.TransactionCountORM
	LoaderChannel chan *models.TransactionCount
}

var transactionCountModel *TransactionCountModel
var transactionCountModelOnce sync.Once

// GetLogModel - create and/or return the logs table model
func GetTransactionCountModel() *TransactionCountModel {
	transactionCountModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionCountModel = &TransactionCountModel{
			db:            dbConn,
			model:         &models.TransactionCount{},
			LoaderChannel: make(chan *models.TransactionCount, 1),
		}

		err := transactionCountModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionCountModel: Unable migrate postgres table: ", err.Error())
		}

		StartTransactionCountLoader()
	})

	return transactionCountModel
}

// Migrate - migrate transactionCounts table
func (m *TransactionCountModel) Migrate() error {
	// Only using TransactionCountRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert transactionCount into table
func (m *TransactionCountModel) Insert(transactionCount *models.TransactionCount) error {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCount{})

	db = db.Create(transactionCount)

	return db.Error
}

// Select - select from transactionCounts table
func (m *TransactionCountModel) SelectOne(transactionHash string, logIndex int32) (*models.TransactionCount, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCount{})

	// Transaction Hash
	db = db.Where("transaction_hash = ?", transactionHash)

	// Log Index
	db = db.Where("log_index = ?", logIndex)

	transactionCount := &models.TransactionCount{}
	db = db.First(transactionCount)

	return transactionCount, db.Error
}

func (m *TransactionCountModel) SelectLargestCount() (uint64, error) {

	db := m.db
	//computeCount := false

	// Set table
	db = db.Model(&models.TransactionCount{})

	// Get max id
	count := uint64(0)
	row := db.Select("max(id)").Row()
	row.Scan(&count)

	return count, db.Error
}

// StartTransactionCountLoader starts loader
func StartTransactionCountLoader() {
	go func() {

		for {
			// Read transactionCount
			newTransactionCount := <-GetTransactionCountModel().LoaderChannel

			// Insert
			_, err := GetTransactionCountModel().SelectOne(
				newTransactionCount.TransactionHash,
				newTransactionCount.LogIndex,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Insert
				err = GetTransactionCountModel().Insert(newTransactionCount)
				if err != nil {
					zap.S().Fatal(err.Error())
				}

				zap.S().Debug("Loader=TransactionCount, TransactionHash=", newTransactionCount.TransactionHash, " LogIndex=", newTransactionCount.LogIndex, " - Insert")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
