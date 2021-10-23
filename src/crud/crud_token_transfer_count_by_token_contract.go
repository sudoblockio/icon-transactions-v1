package crud

import (
	"errors"
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-transactions/models"
	"github.com/geometry-labs/icon-transactions/redis"
)

// TokenTransferCountByTokenContractModel - type for token_contract table model
type TokenTransferCountByTokenContractModel struct {
	db            *gorm.DB
	model         *models.TokenTransferCountByTokenContract
	modelORM      *models.TokenTransferCountByTokenContractORM
	LoaderChannel chan *models.TokenTransferCountByTokenContract
}

var tokenTransferCountByTokenContractModel *TokenTransferCountByTokenContractModel
var tokenTransferCountByTokenContractModelOnce sync.Once

// GetTokenContractModel - create and/or return the token_contract table model
func GetTokenTransferCountByTokenContractModel() *TokenTransferCountByTokenContractModel {
	tokenTransferCountByTokenContractModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		tokenTransferCountByTokenContractModel = &TokenTransferCountByTokenContractModel{
			db:            dbConn,
			model:         &models.TokenTransferCountByTokenContract{},
			LoaderChannel: make(chan *models.TokenTransferCountByTokenContract, 1),
		}

		err := tokenTransferCountByTokenContractModel.Migrate()
		if err != nil {
			zap.S().Fatal("TokenTransferCountByTokenContractModel: Unable migrate postgres table: ", err.Error())
		}

		StartTokenTransferCountByTokenContractLoader()
	})

	return tokenTransferCountByTokenContractModel
}

// Migrate - migrate tokenTransferCountByTokenContracts table
func (m *TokenTransferCountByTokenContractModel) Migrate() error {
	// Only using TokenTransferCountByTokenContractRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Select - select from tokenTransferCountByTokenContracts table
func (m *TokenTransferCountByTokenContractModel) SelectOne(tokenContract string) (*models.TokenTransferCountByTokenContract, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCountByTokenContract{})

	// TokenContract
	db = db.Where("token_contract = ?", tokenContract)

	tokenTransferCountByTokenContract := &models.TokenTransferCountByTokenContract{}
	db = db.First(tokenTransferCountByTokenContract)

	return tokenTransferCountByTokenContract, db.Error
}

// Select - select from tokenTransferCountByTokenContracts table
func (m *TokenTransferCountByTokenContractModel) SelectCount(tokenContract string) (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCountByTokenContract{})

	// TokenContract
	db = db.Where("token_contract = ?", tokenContract)

	tokenTransferCountByTokenContract := &models.TokenTransferCountByTokenContract{}
	db = db.First(tokenTransferCountByTokenContract)

	count := uint64(0)
	if tokenTransferCountByTokenContract != nil {
		count = tokenTransferCountByTokenContract.Count
	}

	return count, db.Error
}

func (m *TokenTransferCountByTokenContractModel) UpsertOne(
	tokenTransferCountByTokenContract *models.TokenTransferCountByTokenContract,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*tokenTransferCountByTokenContract),
		reflect.TypeOf(*tokenTransferCountByTokenContract),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "token_contract"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(tokenTransferCountByTokenContract)

	return db.Error
}

// StartTokenTransferCountByTokenContractLoader starts loader
func StartTokenTransferCountByTokenContractLoader() {
	go func() {
		postgresLoaderChan := GetTokenTransferCountByTokenContractModel().LoaderChannel

		for {
			// Read transaction
			newTokenTransferCountByTokenContract := <-postgresLoaderChan

			//////////////////////////
			// Get count from redis //
			//////////////////////////
			countKey := "token_transfer_count_by_token_contract_" + newTokenTransferCountByTokenContract.TokenContract

			count, err := redis.GetRedisClient().GetCount(countKey)
			if err != nil {
				zap.S().Fatal("Loader=Transaction, Hash=", newTokenTransferCountByTokenContract.TransactionHash, " TokenContract=", newTokenTransferCountByTokenContract.TokenContract, " - Error: ", err.Error())
			}

			// No count set yet
			// Get from database
			if count == -1 {
				curTokenTransferCountByTokenContract, err := GetTokenTransferCountByTokenContractModel().SelectOne(newTokenTransferCountByTokenContract.TokenContract)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					count = 0
				} else if err != nil {
					zap.S().Fatal(
						"Loader=Transaction,",
						"Hash=", newTokenTransferCountByTokenContract.TransactionHash,
						" TokenContract=", newTokenTransferCountByTokenContract.TokenContract,
						" - Error: ", err.Error())
				} else {
					count = int64(curTokenTransferCountByTokenContract.Count)
				}

				// Set count
				err = redis.GetRedisClient().SetCount(countKey, int64(count))
				if err != nil {
					// Redis error
					zap.S().Fatal("Loader=Transaction, Hash=", newTokenTransferCountByTokenContract.TransactionHash, " TokenContract=", newTokenTransferCountByTokenContract.TokenContract, " - Error: ", err.Error())
				}
			}

			//////////////////////
			// Load to postgres //
			//////////////////////

			// Add transaction to indexed
			newTokenTransferCountByTokenContractIndex := &models.TokenTransferCountByTokenContractIndex{
				TransactionHash: newTokenTransferCountByTokenContract.TransactionHash,
				LogIndex:        newTokenTransferCountByTokenContract.LogIndex,
			}
			err = GetTokenTransferCountByTokenContractIndexModel().Insert(newTokenTransferCountByTokenContractIndex)
			if err != nil {
				// Record already exists, continue
				continue
			}

			// Increment records
			count, err = redis.GetRedisClient().IncCount(countKey)
			if err != nil {
				// Redis error
				zap.S().Fatal("Loader=Transaction, Hash=", newTokenTransferCountByTokenContract.TransactionHash, " TokenContract=", newTokenTransferCountByTokenContract.TokenContract, " - Error: ", err.Error())
			}
			newTokenTransferCountByTokenContract.Count = uint64(count)

			err = GetTokenTransferCountByTokenContractModel().UpsertOne(newTokenTransferCountByTokenContract)
			zap.S().Debug("Loader=Transaction, Hash=", newTokenTransferCountByTokenContract.TransactionHash, " TokenContract=", newTokenTransferCountByTokenContract.TokenContract, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Fatal("Loader=Transaction, Hash=", newTokenTransferCountByTokenContract.TransactionHash, " TokenContract=", newTokenTransferCountByTokenContract.TokenContract, " - Error: ", err.Error())
			}
		}
	}()
}
