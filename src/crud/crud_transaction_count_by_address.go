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

// TransactionCountByAddressModel - type for address table model
type TransactionCountByAddressModel struct {
	db            *gorm.DB
	model         *models.TransactionCountByAddress
	modelORM      *models.TransactionCountByAddressORM
	LoaderChannel chan *models.TransactionCountByAddress
}

var transactionCountByAddressModel *TransactionCountByAddressModel
var transactionCountByAddressModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetTransactionCountByAddressModel() *TransactionCountByAddressModel {
	transactionCountByAddressModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionCountByAddressModel = &TransactionCountByAddressModel{
			db:            dbConn,
			model:         &models.TransactionCountByAddress{},
			LoaderChannel: make(chan *models.TransactionCountByAddress, 1),
		}

		err := transactionCountByAddressModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionCountByAddressModel: Unable migrate postgres table: ", err.Error())
		}

		StartTransactionCountByAddressLoader()
	})

	return transactionCountByAddressModel
}

// Migrate - migrate transactionCountByAddresss table
func (m *TransactionCountByAddressModel) Migrate() error {
	// Only using TransactionCountByAddressRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Select - select from transactionCountByAddresss table
func (m *TransactionCountByAddressModel) SelectOne(address string) (*models.TransactionCountByAddress, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCountByAddress{})

	// Address
	db = db.Where("address = ?", address)

	transactionCountByAddress := &models.TransactionCountByAddress{}
	db = db.First(transactionCountByAddress)

	return transactionCountByAddress, db.Error
}

// SelectMany - select from transactionCountByAddresss table
func (m *TransactionCountByAddressModel) SelectMany(limit int, skip int) (*[]models.TransactionCountByAddress, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.TransactionCountByAddress{})

	// Limit
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	transactionCountByAddresses := &[]models.TransactionCountByAddress{}
	db = db.Find(transactionCountByAddresses)

	return transactionCountByAddresses, db.Error
}

// Select - select from transactionCountByAddresss table
func (m *TransactionCountByAddressModel) SelectCount(address string) (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCountByAddress{})

	// Address
	db = db.Where("address = ?", address)

	transactionCountByAddress := &models.TransactionCountByAddress{}
	db = db.First(transactionCountByAddress)

	count := uint64(0)
	if transactionCountByAddress != nil {
		count = transactionCountByAddress.Count
	}

	return count, db.Error
}

func (m *TransactionCountByAddressModel) UpsertOne(
	transactionCountByAddress *models.TransactionCountByAddress,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*transactionCountByAddress),
		reflect.TypeOf(*transactionCountByAddress),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "address"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(transactionCountByAddress)

	return db.Error
}

// StartTransactionCountByAddressLoader starts loader
func StartTransactionCountByAddressLoader() {
	go func() {
		postgresLoaderChan := GetTransactionCountByAddressModel().LoaderChannel

		for {
			// Read transaction
			newTransactionCountByAddress := <-postgresLoaderChan

			//////////////////////////
			// Get count from redis //
			//////////////////////////
			countKey := "icon_transactions_transaction_count_by_address_" + newTransactionCountByAddress.Address

			count, err := redis.GetRedisClient().GetCount(countKey)
			if err != nil {
				zap.S().Fatal("Loader=Transaction, Hash=", newTransactionCountByAddress.TransactionHash, " Address=", newTransactionCountByAddress.Address, " - Error: ", err.Error())
			}

			// No count set yet
			// Get from database
			if count == -1 {
				curTransactionCountByAddress, err := GetTransactionCountByAddressModel().SelectOne(newTransactionCountByAddress.Address)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					count = 0
				} else if err != nil {
					zap.S().Fatal(
						"Loader=Transaction,",
						"Hash=", newTransactionCountByAddress.TransactionHash,
						" Address=", newTransactionCountByAddress.Address,
						" - Error: ", err.Error())
				} else {
					count = int64(curTransactionCountByAddress.Count)
				}

				// Set count
				err = redis.GetRedisClient().SetCount(countKey, int64(count))
				if err != nil {
					// Redis error
					zap.S().Fatal("Loader=Transaction, Hash=", newTransactionCountByAddress.TransactionHash, " Address=", newTransactionCountByAddress.Address, " - Error: ", err.Error())
				}
			}

			//////////////////////
			// Load to postgres //
			//////////////////////

			// Add transaction to indexed
			newTransactionCountByAddressIndex := &models.TransactionCountByAddressIndex{
				TransactionHash: newTransactionCountByAddress.TransactionHash,
				Address:         newTransactionCountByAddress.Address,
			}
			err = GetTransactionCountByAddressIndexModel().Insert(newTransactionCountByAddressIndex)
			if err != nil {
				// Record already exists, continue
				continue
			}

			// Increment records
			count, err = redis.GetRedisClient().IncCount(countKey)
			if err != nil {
				// Redis error
				zap.S().Fatal("Loader=Transaction, Hash=", newTransactionCountByAddress.TransactionHash, " Address=", newTransactionCountByAddress.Address, " - Error: ", err.Error())
			}
			newTransactionCountByAddress.Count = uint64(count)

			err = GetTransactionCountByAddressModel().UpsertOne(newTransactionCountByAddress)
			zap.S().Debug("Loader=Transaction, Hash=", newTransactionCountByAddress.TransactionHash, " Address=", newTransactionCountByAddress.Address, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Fatal("Loader=Transaction, Hash=", newTransactionCountByAddress.TransactionHash, " Address=", newTransactionCountByAddress.Address, " - Error: ", err.Error())
			}
		}
	}()
}
