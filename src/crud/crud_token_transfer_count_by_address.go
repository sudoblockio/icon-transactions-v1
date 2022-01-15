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

// TokenTransferCountByAddressModel - type for address table model
type TokenTransferCountByAddressModel struct {
	db            *gorm.DB
	model         *models.TokenTransferCountByAddress
	modelORM      *models.TokenTransferCountByAddressORM
	LoaderChannel chan *models.TokenTransferCountByAddress
}

var tokenTransferCountByAddressModel *TokenTransferCountByAddressModel
var tokenTransferCountByAddressModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetTokenTransferCountByAddressModel() *TokenTransferCountByAddressModel {
	tokenTransferCountByAddressModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		tokenTransferCountByAddressModel = &TokenTransferCountByAddressModel{
			db:            dbConn,
			model:         &models.TokenTransferCountByAddress{},
			LoaderChannel: make(chan *models.TokenTransferCountByAddress, 1),
		}

		err := tokenTransferCountByAddressModel.Migrate()
		if err != nil {
			zap.S().Fatal("TokenTransferCountByAddressModel: Unable migrate postgres table: ", err.Error())
		}

		StartTokenTransferCountByAddressLoader()
	})

	return tokenTransferCountByAddressModel
}

// Migrate - migrate tokenTransferCountByAddresss table
func (m *TokenTransferCountByAddressModel) Migrate() error {
	// Only using TokenTransferCountByAddressRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Select - select from tokenTransferCountByAddresss table
func (m *TokenTransferCountByAddressModel) SelectOne(address string) (*models.TokenTransferCountByAddress, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCountByAddress{})

	// Address
	db = db.Where("address = ?", address)

	tokenTransferCountByAddress := &models.TokenTransferCountByAddress{}
	db = db.First(tokenTransferCountByAddress)

	return tokenTransferCountByAddress, db.Error
}

// SelectMany - select from transactionCountByAddresss table
func (m *TokenTransferCountByAddressModel) SelectMany(limit int, skip int) (*[]models.TokenTransferCountByAddress, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.TokenTransferCountByAddress{})

	// Limit
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	tokenTransferCountByAddresses := &[]models.TokenTransferCountByAddress{}
	db = db.Find(tokenTransferCountByAddresses)

	return tokenTransferCountByAddresses, db.Error
}

// Select - select from tokenTransferCountByAddresss table
func (m *TokenTransferCountByAddressModel) SelectCount(address string) (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCountByAddress{})

	// Address
	db = db.Where("address = ?", address)

	tokenTransferCountByAddress := &models.TokenTransferCountByAddress{}
	db = db.First(tokenTransferCountByAddress)

	count := uint64(0)
	if tokenTransferCountByAddress != nil {
		count = tokenTransferCountByAddress.Count
	}

	return count, db.Error
}

func (m *TokenTransferCountByAddressModel) UpsertOne(
	tokenTransferCountByAddress *models.TokenTransferCountByAddress,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*tokenTransferCountByAddress),
		reflect.TypeOf(*tokenTransferCountByAddress),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "address"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(tokenTransferCountByAddress)

	return db.Error
}

// StartTokenTransferCountByAddressLoader starts loader
func StartTokenTransferCountByAddressLoader() {
	go func() {
		postgresLoaderChan := GetTokenTransferCountByAddressModel().LoaderChannel

		for {
			// Read transaction
			newTokenTransferCountByAddress := <-postgresLoaderChan

			//////////////////////////
			// Get count from redis //
			//////////////////////////
			countKey := "icon_transactions_token_transfer_count_by_address_" + newTokenTransferCountByAddress.Address

			count, err := redis.GetRedisClient().GetCount(countKey)
			if err != nil {
				zap.S().Fatal("Loader=Transaction, Hash=", newTokenTransferCountByAddress.TransactionHash, " Address=", newTokenTransferCountByAddress.Address, " - Error: ", err.Error())
			}

			// No count set yet
			// Get from database
			if count == -1 {
				curTokenTransferCountByAddress, err := GetTokenTransferCountByAddressModel().SelectOne(newTokenTransferCountByAddress.Address)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					count = 0
				} else if err != nil {
					zap.S().Fatal(
						"Loader=Transaction,",
						"Hash=", newTokenTransferCountByAddress.TransactionHash,
						" Address=", newTokenTransferCountByAddress.Address,
						" - Error: ", err.Error())
				} else {
					count = int64(curTokenTransferCountByAddress.Count)
				}

				// Set count
				err = redis.GetRedisClient().SetCount(countKey, int64(count))
				if err != nil {
					// Redis error
					zap.S().Fatal("Loader=Transaction, Hash=", newTokenTransferCountByAddress.TransactionHash, " Address=", newTokenTransferCountByAddress.Address, " - Error: ", err.Error())
				}
			}

			//////////////////////
			// Load to postgres //
			//////////////////////

			// Add transaction to indexed
			newTokenTransferCountByAddressIndex := &models.TokenTransferCountByAddressIndex{
				TransactionHash: newTokenTransferCountByAddress.TransactionHash,
				LogIndex:        newTokenTransferCountByAddress.LogIndex,
				Address:         newTokenTransferCountByAddress.Address,
				BlockNumber:     newTokenTransferCountByAddress.BlockNumber,
			}
			err = GetTokenTransferCountByAddressIndexModel().UpsertOne(newTokenTransferCountByAddressIndex)
			if err != nil {
				// Record already exists, continue
				continue
			}

			// Increment records
			count, err = redis.GetRedisClient().IncCount(countKey)
			if err != nil {
				// Redis error
				zap.S().Fatal("Loader=Transaction, Hash=", newTokenTransferCountByAddress.TransactionHash, " Address=", newTokenTransferCountByAddress.Address, " - Error: ", err.Error())
			}
			newTokenTransferCountByAddress.Count = uint64(count)

			err = GetTokenTransferCountByAddressModel().UpsertOne(newTokenTransferCountByAddress)
			zap.S().Debug("Loader=Transaction, Hash=", newTokenTransferCountByAddress.TransactionHash, " Address=", newTokenTransferCountByAddress.Address, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Fatal("Loader=Transaction, Hash=", newTokenTransferCountByAddress.TransactionHash, " Address=", newTokenTransferCountByAddress.Address, " - Error: ", err.Error())
			}
		}
	}()
}
