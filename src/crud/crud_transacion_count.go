package crud

import (
	"strings"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/models"
)

// TransactionCountModel - type for transaction table model
type TransactionCountModel struct {
	db        *gorm.DB
	model     *models.TransactionCount
	modelORM  *models.TransactionCountORM
	WriteChan chan *models.TransactionCount
}

var transactionCountModel *TransactionCountModel
var transactionCountModelOnce sync.Once

// GetTransactionModel - create and/or return the transactions table model
func GetTransactionCountModel() *TransactionCountModel {
	transactionCountModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionCountModel = &TransactionCountModel{
			db:        dbConn,
			model:     &models.TransactionCount{},
			WriteChan: make(chan *models.TransactionCount, 1),
		}

		err := transactionCountModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionCountModel: Unable migrate postgres table: ", err.Error())
		}
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

	err := backoff.Retry(func() error {
		query := m.db.Create(transactionCount)

		if query.Error != nil && !strings.Contains(query.Error.Error(), "duplicate key value violates unique constraint") {
			zap.S().Warn("POSTGRES Insert Error : ", query.Error.Error())
			return query.Error
		}

		return nil
	}, backoff.NewExponentialBackOff())

	return err
}

// Update - Update transactionCount
func (m *TransactionCountModel) Update(transactionCount *models.TransactionCount) error {

	err := backoff.Retry(func() error {
		query := m.db.Model(&models.TransactionCount{}).Where("id = ?", transactionCount.Id).Update("count", transactionCount.Count)

		if query.Error != nil && !strings.Contains(query.Error.Error(), "duplicate key value violates unique constraint") {
			zap.S().Warn("POSTGRES Insert Error : ", query.Error.Error())
			return query.Error
		}

		return nil
	}, backoff.NewExponentialBackOff())

	return err
}

// Select - select from transactionCounts table
func (m *TransactionCountModel) Select() (models.TransactionCount, error) {
	db := m.db

	transactionCount := models.TransactionCount{}
	db = db.First(&transactionCount)

	return transactionCount, db.Error
}

// Delete - delete from transactionCounts table
func (m *TransactionCountModel) Delete(transactionCount models.TransactionCount) error {
	db := m.db

	db = db.Delete(&transactionCount)

	return db.Error
}

// StartTransactionCountLoader starts loader
func StartTransactionCountLoader() {
	go func() {
		var transactionCount *models.TransactionCount
		postgresLoaderChan := GetTransactionCountModel().WriteChan

		for {
			// Read transactionCount
			transactionCount = <-postgresLoaderChan

			// Load transactionCount to database
			curCount, err := GetTransactionCountModel().Select()
			if err == nil {
				transactionCount.Count = transactionCount.Count + curCount.Count
				GetTransactionCountModel().Update(transactionCount)
			} else {
				GetTransactionCountModel().Insert(transactionCount)
			}

		}
	}()
}
