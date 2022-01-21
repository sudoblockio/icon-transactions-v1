package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

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

func (m *TokenTransferCountByAddressIndexModel) SelectMissingBlockNumbers(
	limit int,
) (*[]models.TokenTransferCountByAddressIndex, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.TokenTransferCountByAddressIndex{})

	db = db.Where("block_number IS NULL")

	// Limit
	db = db.Limit(limit)

	tokenTransferCountByAddressIndices := &[]models.TokenTransferCountByAddressIndex{}
	db = db.Find(tokenTransferCountByAddressIndices)

	return tokenTransferCountByAddressIndices, db.Error
}

// CountByAddress - Count transactionCountByIndex by address
// NOTE this function may take very long for some addresses
func (m *TokenTransferCountByAddressIndexModel) CountByAddress(address string) (int64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCountByAddressIndex{})

	// Address
	db = db.Where("address = ?", address)

	// Count
	var count int64
	db = db.Count(&count)

	return count, db.Error
}

// Insert - Insert transactionCountByIndex into table
func (m *TokenTransferCountByAddressIndexModel) Insert(tokenTransferCountByAddressIndex *models.TokenTransferCountByAddressIndex) error {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCountByAddressIndex{})

	db = db.Create(tokenTransferCountByAddressIndex)

	return db.Error
}

func (m *TokenTransferCountByAddressIndexModel) UpsertOne(
	tokenTransferCountByAddressIndex *models.TokenTransferCountByAddressIndex,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*tokenTransferCountByAddressIndex),
		reflect.TypeOf(*tokenTransferCountByAddressIndex),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}, {Name: "log_index"}, {Name: "address"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(tokenTransferCountByAddressIndex)

	return db.Error
}
