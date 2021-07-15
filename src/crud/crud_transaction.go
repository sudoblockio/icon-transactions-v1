package crud

import (
	"github.com/cenkalti/backoff/v4"
	"github.com/geometry-labs/icon-blocks/models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"strings"
	"sync"
)

type TransactionModel struct {
	db        *gorm.DB
	model     *models.Transaction
	modelORM  *models.Transaction
	writeChan chan *models.Transaction
}

var blockModelInstance *TransactionModel
var blockModelOnce sync.Once

func GetTransacstionModel() *TransactionModel {
	blockModelOnce.Do(func() {
		blockModelInstance = &TransactionModel{
			db:        GetPostgresConn().conn,
			model:     &models.Transaction{},
			writeChan: make(chan *models.Transaction, 1),
		}

		err := blockModelInstance.Migrate()
		if err != nil {
			zap.S().Error("TransactionModel: Unable create postgres table: Transactions")
		}
	})
	return blockModelInstance
}

func (m *TransactionModel) GetDB() *gorm.DB {
	return m.db
}

func (m *TransactionModel) GetModel() *models.Transaction {
	return m.model
}

func (m *TransactionModel) GetWriteChan() chan *models.Transaction {
	return m.writeChan
}

func (m *TransactionModel) Migrate() error {
	// Only using BlockRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

func (m *TransactionModel) create(block *models.Transaction) (*gorm.DB, error) {
	tx := m.db.Create(block)
	return tx, tx.Error
}

func (m *TransactionModel) RetryCreate(block *models.Transaction) (*gorm.DB, error) {
	var transaction *gorm.DB
	operation := func() error {
		tx, err := m.create(block)
		if err != nil && !strings.Contains(err.Error(), "duplicate key value violates unique constraint") {
			zap.S().Info("POSTGRES RetryCreate Error : ", err.Error())
		} else {
			transaction = tx
			return nil
		}
		return err
	}
	neb := backoff.NewExponentialBackOff()
	err := backoff.Retry(operation, neb)
	return transaction, err
}

func (m *TransactionModel) FindOne(conds ...interface{}) (*models.Transaction, *gorm.DB) {
	block := &models.Transaction{}
	tx := m.db.Find(block, conds...)
	return block, tx
}

func (m *TransactionModel) FindAll(conds ...interface{}) (*[]models.Transaction, *gorm.DB) {
	blocks := &[]models.Transaction{}
	tx := m.db.Scopes().Find(blocks, conds...)
	return blocks, tx
}
