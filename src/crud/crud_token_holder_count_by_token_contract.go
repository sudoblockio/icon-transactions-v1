package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-transactions/models"
)

// TokenHolderCountByTokenContractModel - type for tokenHolderCountByTokenContract table model
type TokenHolderCountByTokenContractModel struct {
	db            *gorm.DB
	model         *models.TokenHolderCountByTokenContract
	modelORM      *models.TokenHolderCountByTokenContractORM
	LoaderChannel chan *models.TokenHolderCountByTokenContract
}

var tokenHolderCountByTokenContractModel *TokenHolderCountByTokenContractModel
var tokenHolderCountByTokenContractModelOnce sync.Once

// GetTokenHolderCountByTokenContractModel - create and/or return the tokenHolderCountByTokenContracts table model
func GetTokenHolderCountByTokenContractModel() *TokenHolderCountByTokenContractModel {
	tokenHolderCountByTokenContractModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		tokenHolderCountByTokenContractModel = &TokenHolderCountByTokenContractModel{
			db:            dbConn,
			model:         &models.TokenHolderCountByTokenContract{},
			LoaderChannel: make(chan *models.TokenHolderCountByTokenContract, 1),
		}

		err := tokenHolderCountByTokenContractModel.Migrate()
		if err != nil {
			zap.S().Fatal("TokenHolderCountByTokenContractModel: Unable migrate postgres table: ", err.Error())
		}

		StartTokenHolderCountByTokenContractLoader()
	})

	return tokenHolderCountByTokenContractModel
}

// Migrate - migrate tokenHolderCountByTokenContracts table
func (m *TokenHolderCountByTokenContractModel) Migrate() error {
	// Only using TokenHolderCountByTokenContractRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// SelectMany - select from token_transfers table
// Returns: models, error (if present)
func (m *TokenHolderCountByTokenContractModel) SelectOne(
	tokenContractAddress string,
) (*models.TokenHolderCountByTokenContract, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.TokenHolderCountByTokenContract{})

	// Token Contract Address
	db = db.Where("token_contract_address = ?", tokenContractAddress)

	tokenHolderCountByTokenContract := &models.TokenHolderCountByTokenContract{}
	db = db.First(tokenHolderCountByTokenContract)

	return tokenHolderCountByTokenContract, db.Error
}

// Select - select from transactionCountByAddresss table
func (m *TokenHolderCountByTokenContractModel) SelectCount(tokenContractAddress string) (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TokenHolderCountByTokenContract{})

	// Address
	db = db.Where("token_contract_address = ?", tokenContractAddress)

	tokenHolderCountByTokenContract := &models.TokenHolderCountByTokenContract{}
	db = db.First(tokenHolderCountByTokenContract)

	count := uint64(0)
	if tokenHolderCountByTokenContract != nil {
		count = tokenHolderCountByTokenContract.Count
	}

	return count, db.Error
}

func (m *TokenHolderCountByTokenContractModel) UpsertOne(
	tokenHolderCountByTokenContract *models.TokenHolderCountByTokenContract,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*tokenHolderCountByTokenContract),
		reflect.TypeOf(*tokenHolderCountByTokenContract),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "token_contract_address"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(tokenHolderCountByTokenContract)

	return db.Error
}

// StartTokenHolderCountByTokenContractLoader starts loader
func StartTokenHolderCountByTokenContractLoader() {
	go func() {
		postgresLoaderChan := GetTokenHolderCountByTokenContractModel().LoaderChannel

		for {
			// Read tokenHolderCountByTokenContract
			newTokenHolderCountByTokenContract := <-postgresLoaderChan

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetTokenHolderCountByTokenContractModel().UpsertOne(newTokenHolderCountByTokenContract)
			zap.S().Debug("Loader=TokenHolderCountByTokenContract, TokenContractAddress=", newTokenHolderCountByTokenContract.TokenContractAddress, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Fatal("Loader=TokenHolderCountByTokenContract, TokenContractAddress=", newTokenHolderCountByTokenContract.TokenContractAddress, " - Error: ", err.Error())
			}
		}
	}()
}
