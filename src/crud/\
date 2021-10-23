package crud

import (
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/models"
)

// TokenTransferCountByAddressIndexModel - type for address table model
type TokenTransferCountByAddressIndexModel struct {
	db            *gorm.DB
	model         *models.TokenTransferCountByAddressIndex
	modelORM      *models.TokenTransferCountByAddressIndexORM
	LoaderChannel chan *models.TokenTransferCountByAddressIndex
}

var tokenTransferCountByAddressIndexModel *TokenTransferCountByAddressIndexModel
var tokenTransferCountByAddressIndexModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetTokenTransferCountByAddressIndexModel() *TokenTransferCountByAddressIndexModel {
	tokenTransferCountByAddressIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		tokenTransferCountByAddressIndexModel = &TokenTransferCountByAddressIndexModel{
			db:            dbConn,
			model:         &models.TokenTransferCountByAddressIndex{},
			LoaderChannel: make(chan *models.TokenTransferCountByAddressIndex, 1),
		}

		err := tokenTransferCountByAddressIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("TokenTransferCountByAddressIndexModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return tokenTransferCountByAddressIndexModel
}

// Migrate - migrate tokenTransferCountByAddressIndexs table
func (m *TokenTransferCountByAddressIndexModel) Migrate() error {
	// Only using TokenTransferCountByAddressIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert transactionCountByIndex into table
func (m *TokenTransferCountByAddressIndexModel) Insert(tokenTransferCountByAddressIndex *models.TokenTransferCountByAddressIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCountByAddressIndex{})

	db = db.Create(tokenTransferCountByAddressIndex)

	return db.Error
}
