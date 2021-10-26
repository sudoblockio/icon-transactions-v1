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

// TransactionInternalCountByAddressModel - type for address table model
type TransactionInternalCountByAddressModel struct {
	db            *gorm.DB
	model         *models.TransactionInternalCountByAddress
	modelORM      *models.TransactionInternalCountByAddressORM
	LoaderChannel chan *models.TransactionInternalCountByAddress
}

var transactionInternalCountByAddressModel *TransactionInternalCountByAddressModel
var transactionInternalCountByAddressModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetTransactionInternalCountByAddressModel() *TransactionInternalCountByAddressModel {
	transactionInternalCountByAddressModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionInternalCountByAddressModel = &TransactionInternalCountByAddressModel{
			db:            dbConn,
			model:         &models.TransactionInternalCountByAddress{},
			LoaderChannel: make(chan *models.TransactionInternalCountByAddress, 1),
		}

		err := transactionInternalCountByAddressModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionInternalCountByAddressModel: Unable migrate postgres table: ", err.Error())
		}

		StartTransactionInternalCountByAddressLoader()
	})

	return transactionInternalCountByAddressModel
}

// Migrate - migrate transactionInternalCountByAddresss table
func (m *TransactionInternalCountByAddressModel) Migrate() error {
	// Only using TransactionInternalCountByAddressRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Select - select from transactionInternalCountByAddresss table
func (m *TransactionInternalCountByAddressModel) SelectOne(address string) (*models.TransactionInternalCountByAddress, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionInternalCountByAddress{})

	// Address
	db = db.Where("address = ?", address)

	transactionInternalCountByAddress := &models.TransactionInternalCountByAddress{}
	db = db.First(transactionInternalCountByAddress)

	return transactionInternalCountByAddress, db.Error
}

// Select - select from transactionInternalCountByAddresss table
func (m *TransactionInternalCountByAddressModel) SelectCount(address string) (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionInternalCountByAddress{})

	// Address
	db = db.Where("address = ?", address)

	transactionInternalCountByAddress := &models.TransactionInternalCountByAddress{}
	db = db.First(transactionInternalCountByAddress)

	count := uint64(0)
	if transactionInternalCountByAddress != nil {
		count = transactionInternalCountByAddress.Count
	}

	return count, db.Error
}

func (m *TransactionInternalCountByAddressModel) UpsertOne(
	transactionInternalCountByAddress *models.TransactionInternalCountByAddress,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*transactionInternalCountByAddress),
		reflect.TypeOf(*transactionInternalCountByAddress),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "address"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(transactionInternalCountByAddress)

	return db.Error
}

// StartTransactionInternalCountByAddressLoader starts loader
func StartTransactionInternalCountByAddressLoader() {
	go func() {
		postgresLoaderChan := GetTransactionInternalCountByAddressModel().LoaderChannel

		for {
			// Read transaction
			newTransactionInternalCountByAddress := <-postgresLoaderChan

			//////////////////////////
			// Get count from redis //
			//////////////////////////
			countKey := "icon_transactions_transaction_internal_count_by_address_" + newTransactionInternalCountByAddress.Address

			count, err := redis.GetRedisClient().GetCount(countKey)
			if err != nil {
				zap.S().Fatal("Loader=Transaction, Hash=", newTransactionInternalCountByAddress.TransactionHash, " Address=", newTransactionInternalCountByAddress.Address, " - Error: ", err.Error())
			}

			// No count set yet
			// Get from database
			if count == -1 {
				curTransactionInternalCountByAddress, err := GetTransactionInternalCountByAddressModel().SelectOne(newTransactionInternalCountByAddress.Address)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					count = 0
				} else if err != nil {
					zap.S().Fatal(
						"Loader=Transaction,",
						"Hash=", newTransactionInternalCountByAddress.TransactionHash,
						" Address=", newTransactionInternalCountByAddress.Address,
						" - Error: ", err.Error())
				} else {
					count = int64(curTransactionInternalCountByAddress.Count)
				}

				// Set count
				err = redis.GetRedisClient().SetCount(countKey, int64(count))
				if err != nil {
					// Redis error
					zap.S().Fatal("Loader=Transaction, Hash=", newTransactionInternalCountByAddress.TransactionHash, " Address=", newTransactionInternalCountByAddress.Address, " - Error: ", err.Error())
				}
			}

			//////////////////////
			// Load to postgres //
			//////////////////////

			// Add transaction to indexed
			newTransactionInternalCountByAddressIndex := &models.TransactionInternalCountByAddressIndex{
				TransactionHash: newTransactionInternalCountByAddress.TransactionHash,
				LogIndex:        newTransactionInternalCountByAddress.LogIndex,
				Address:         newTransactionInternalCountByAddress.Address,
			}
			err = GetTransactionInternalCountByAddressIndexModel().Insert(newTransactionInternalCountByAddressIndex)
			if err != nil {
				// Record already exists, continue
				continue
			}

			// Increment records
			count, err = redis.GetRedisClient().IncCount(countKey)
			if err != nil {
				// Redis error
				zap.S().Fatal("Loader=Transaction, Hash=", newTransactionInternalCountByAddress.TransactionHash, " Address=", newTransactionInternalCountByAddress.Address, " - Error: ", err.Error())
			}
			newTransactionInternalCountByAddress.Count = uint64(count)

			err = GetTransactionInternalCountByAddressModel().UpsertOne(newTransactionInternalCountByAddress)
			zap.S().Debug("Loader=Transaction, Hash=", newTransactionInternalCountByAddress.TransactionHash, " Address=", newTransactionInternalCountByAddress.Address, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Fatal("Loader=Transaction, Hash=", newTransactionInternalCountByAddress.TransactionHash, " Address=", newTransactionInternalCountByAddress.Address, " - Error: ", err.Error())
			}
		}
	}()
}
