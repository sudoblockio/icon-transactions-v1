package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-transactions/models"
)

// TokenHolderModel - type for tokenHolder table model
type TokenHolderModel struct {
	db            *gorm.DB
	model         *models.TokenHolder
	modelORM      *models.TokenHolderORM
	LoaderChannel chan *models.TokenHolder
}

var tokenHolderModel *TokenHolderModel
var tokenHolderModelOnce sync.Once

// GetTokenHolderModel - create and/or return the tokenHolders table model
func GetTokenHolderModel() *TokenHolderModel {
	tokenHolderModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		tokenHolderModel = &TokenHolderModel{
			db:            dbConn,
			model:         &models.TokenHolder{},
			LoaderChannel: make(chan *models.TokenHolder, 1),
		}

		err := tokenHolderModel.Migrate()
		if err != nil {
			zap.S().Fatal("TokenHolderModel: Unable migrate postgres table: ", err.Error())
		}

		StartTokenHolderLoader()
	})

	return tokenHolderModel
}

// Migrate - migrate tokenHolders table
func (m *TokenHolderModel) Migrate() error {
	// Only using TokenHolderRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// SelectMany - select from token_transfers table
// Returns: models, error (if present)
func (m *TokenHolderModel) SelectManyByTokenContract(
	limit int,
	skip int,
	tokenContractAddress string,
) (*[]models.TokenHolder, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.TokenHolder{})

	db = db.Order("value_decimal desc")

	// Token Contract Address
	db = db.Where("token_contract_address = ?", tokenContractAddress)

	// Limit is required and defaulted to 1
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	tokenHolders := &[]models.TokenHolder{}
	db = db.Find(tokenHolders)

	return tokenHolders, db.Error
}

func (m *TokenHolderModel) UpsertOne(
	tokenHolder *models.TokenHolder,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*tokenHolder),
		reflect.TypeOf(*tokenHolder),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "token_contract_address"}, {Name: "holder_address"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(tokenHolder)

	return db.Error
}

// StartTokenHolderLoader starts loader
func StartTokenHolderLoader() {
	go func() {
		postgresLoaderChan := GetTokenHolderModel().LoaderChannel

		for {
			// Read tokenHolder
			newTokenHolder := <-postgresLoaderChan

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetTokenHolderModel().UpsertOne(newTokenHolder)
			zap.S().Debug("Loader=TokenHolder, TokenContractAddress=", newTokenHolder.TokenContractAddress, " HolderAddress=", newTokenHolder.HolderAddress, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Fatal("Loader=TokenHolder, TokenContractAddress=", newTokenHolder.TokenContractAddress, " HolderAddress=", newTokenHolder.HolderAddress, " - Error: ", err.Error())
			}
		}
	}()
}
