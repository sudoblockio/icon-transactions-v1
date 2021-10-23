package crud

import (
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/models"
)

// TokenTransferCountByTokenContractIndexModel - type for address table model
type TokenTransferCountByTokenContractIndexModel struct {
	db            *gorm.DB
	model         *models.TokenTransferCountByTokenContractIndex
	modelORM      *models.TokenTransferCountByTokenContractIndexORM
	LoaderChannel chan *models.TokenTransferCountByTokenContractIndex
}

var tokenTransferCountByTokenContractIndexModel *TokenTransferCountByTokenContractIndexModel
var tokenTransferCountByTokenContractIndexModelOnce sync.Once

// GetTokenContractModel - create and/or return the addresss table model
func GetTokenTransferCountByTokenContractIndexModel() *TokenTransferCountByTokenContractIndexModel {
	tokenTransferCountByTokenContractIndexModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		tokenTransferCountByTokenContractIndexModel = &TokenTransferCountByTokenContractIndexModel{
			db:            dbConn,
			model:         &models.TokenTransferCountByTokenContractIndex{},
			LoaderChannel: make(chan *models.TokenTransferCountByTokenContractIndex, 1),
		}

		err := tokenTransferCountByTokenContractIndexModel.Migrate()
		if err != nil {
			zap.S().Fatal("TokenTransferCountByTokenContractIndexModel: Unable migrate postgres table: ", err.Error())
		}
	})

	return tokenTransferCountByTokenContractIndexModel
}

// Migrate - migrate tokenTransferCountByTokenContractIndexs table
func (m *TokenTransferCountByTokenContractIndexModel) Migrate() error {
	// Only using TokenTransferCountByTokenContractIndexRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Insert - Insert transactionCountByIndex into table
func (m *TokenTransferCountByTokenContractIndexModel) Insert(tokenTransferCountByTokenContractIndex *models.TokenTransferCountByTokenContractIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCountByTokenContractIndex{})

	db = db.Create(tokenTransferCountByTokenContractIndex)

	return db.Error
}
