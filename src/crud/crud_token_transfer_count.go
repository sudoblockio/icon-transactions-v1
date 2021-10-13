package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/models"
)

// TokenTransferCountModel - type for log table model
type TokenTransferCountModel struct {
	db            *gorm.DB
	model         *models.TokenTransferCount
	modelORM      *models.TokenTransferCountORM
	LoaderChannel chan *models.TokenTransferCount
}

var tokenTransferCountModel *TokenTransferCountModel
var tokenTransferCountModelOnce sync.Once

// GetLogModel - create and/or return the logs table model
func GetTokenTransferCountModel() *TokenTransferCountModel {
	tokenTransferCountModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		tokenTransferCountModel = &TokenTransferCountModel{
			db:            dbConn,
			model:         &models.TokenTransferCount{},
			LoaderChannel: make(chan *models.TokenTransferCount, 1),
		}

		err := tokenTransferCountModel.Migrate()
		if err != nil {
			zap.S().Fatal("TokenTransferCountModel: Unable migrate postgres table: ", err.Error())
		}

		StartTokenTransferCountLoader()
	})

	return tokenTransferCountModel
}

// Migrate - migrate tokenTransferCounts table
func (m *TokenTransferCountModel) Migrate() error {
	// Only using TokenTransferCountRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert tokenTransferCount into table
func (m *TokenTransferCountModel) Insert(tokenTransferCount *models.TokenTransferCount) error {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCount{})

	db = db.Create(tokenTransferCount)

	return db.Error
}

// Select - select from tokenTransferCounts table
func (m *TokenTransferCountModel) SelectOne(transactionHash string, logIndex int32) (*models.TokenTransferCount, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCount{})

	// Transaction Hash
	db = db.Where("transaction_hash = ?", transactionHash)

	// Log Index
	db = db.Where("log_index = ?", logIndex)

	tokenTransferCount := &models.TokenTransferCount{}
	db = db.First(tokenTransferCount)

	return tokenTransferCount, db.Error
}

func (m *TokenTransferCountModel) SelectLargestCount() (uint64, error) {

	db := m.db
	//computeCount := false

	// Set table
	db = db.Model(&models.TokenTransferCount{})

	// Get max id
	count := uint64(0)
	row := db.Select("max(id)").Row()
	row.Scan(&count)

	return count, db.Error
}

// StartTokenTransferCountLoader starts loader
func StartTokenTransferCountLoader() {
	go func() {

		for {
			// Read tokenTransferCount
			newTokenTransferCount := <-GetTokenTransferCountModel().LoaderChannel

			// Insert
			_, err := GetTokenTransferCountModel().SelectOne(
				newTokenTransferCount.TransactionHash,
				newTokenTransferCount.LogIndex,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Insert
				err = GetTokenTransferCountModel().Insert(newTokenTransferCount)
				if err != nil {
					zap.S().Warn(err.Error())
				}

				zap.S().Debug("Loader=TokenTransferCount, TokenTransferHash=", newTokenTransferCount.TransactionHash, " LogIndex=", newTokenTransferCount.LogIndex, " - Insert")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
