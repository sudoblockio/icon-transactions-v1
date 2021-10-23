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

// TransactionCountModel - type for address table model
type TransactionCountModel struct {
	db            *gorm.DB
	model         *models.TransactionCount
	modelORM      *models.TransactionCountORM
	LoaderChannel chan *models.TransactionCount
}

var transactionCountModel *TransactionCountModel
var transactionCountModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetTransactionCountModel() *TransactionCountModel {
	transactionCountModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionCountModel = &TransactionCountModel{
			db:            dbConn,
			model:         &models.TransactionCount{},
			LoaderChannel: make(chan *models.TransactionCount, 1),
		}

		err := transactionCountModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionCountModel: Unable migrate postgres table: ", err.Error())
		}

		StartTransactionCountLoader()
	})

	return transactionCountModel
}

// Migrate - migrate transactionCounts table
func (m *TransactionCountModel) Migrate() error {
	// Only using TransactionCountRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Select - select from transactionCounts table
func (m *TransactionCountModel) SelectOne(_type string) (*models.TransactionCount, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCount{})

	// Address
	db = db.Where("type = ?", _type)

	transactionCount := &models.TransactionCount{}
	db = db.First(transactionCount)

	return transactionCount, db.Error
}

// Select - select from transactionCounts table
func (m *TransactionCountModel) SelectCount(_type string) (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCount{})

	// Address
	db = db.Where("type = ?", _type)

	transactionCount := &models.TransactionCount{}
	db = db.First(transactionCount)

	count := uint64(0)
	if transactionCount != nil {
		count = transactionCount.Count
	}

	return count, db.Error
}

func (m *TransactionCountModel) UpsertOne(
	transactionCount *models.TransactionCount,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*transactionCount),
		reflect.TypeOf(*transactionCount),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "type"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(transactionCount)

	return db.Error
}

// StartTransactionCountLoader starts loader
func StartTransactionCountLoader() {
	go func() {
		postgresLoaderChan := GetTransactionCountModel().LoaderChannel

		for {
			// Read transaction
			newTransactionCount := <-postgresLoaderChan

			//////////////////////////
			// Get count from redis //
			//////////////////////////
			countKey := "transaction_count_" + newTransactionCount.Type

			count, err := redis.GetRedisClient().GetCount(countKey)
			if err != nil {
				zap.S().Fatal(
					"Loader=Transaction,",
					"Hash=", newTransactionCount.TransactionHash,
					" Type=", newTransactionCount.Type,
					" - Error: ", err.Error())
			}

			// No count set yet
			// Get from database
			if count == -1 {
				curTransactionCount, err := GetTransactionCountModel().SelectOne(newTransactionCount.Type)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					count = 0
				} else if err != nil {
					zap.S().Fatal(
						"Loader=Transaction,",
						"Hash=", newTransactionCount.TransactionHash,
						" Type=", newTransactionCount.Type,
						" - Error: ", err.Error())
				} else {
					count = int64(curTransactionCount.Count)
				}

				// Set count
				err = redis.GetRedisClient().SetCount(countKey, int64(count))
				if err != nil {
					// Redis error
					zap.S().Fatal(
						"Loader=Transaction,",
						"Hash=", newTransactionCount.TransactionHash,
						" Type=", newTransactionCount.Type,
						" - Error: ", err.Error())
				}
			}

			//////////////////////
			// Load to postgres //
			//////////////////////

			// Add transaction to indexed
			if newTransactionCount.Type == "regular" {
				newTransactionCountIndex := &models.TransactionCountIndex{
					TransactionHash: newTransactionCount.TransactionHash,
				}
				err = GetTransactionCountIndexModel().Insert(newTransactionCountIndex)
				if err != nil {
					// Record already exists, continue
					continue
				}
			} else if newTransactionCount.Type == "internal" {
				newTransactionInternalCountIndex := &models.TransactionInternalCountIndex{
					TransactionHash: newTransactionCount.TransactionHash,
					LogIndex:        newTransactionCount.LogIndex,
				}
				err = GetTransactionInternalCountIndexModel().Insert(newTransactionInternalCountIndex)
				if err != nil {
					// Record already exists, continue
					continue
				}
			} else if newTransactionCount.Type == "token_transfer" {
				newTransactionTokenTransferCountIndex := &models.TransactionTokenTransferCountIndex{
					TransactionHash: newTransactionCount.TransactionHash,
					LogIndex:        newTransactionCount.LogIndex,
				}
				err = GetTransactionTokenTransferCountIndexModel().Insert(newTransactionTokenTransferCountIndex)
				if err != nil {
					// Record already exists, continue
					continue
				}
			}

			// Increment records
			count, err = redis.GetRedisClient().IncCount(countKey)
			if err != nil {
				// Redis error
				zap.S().Fatal(
					"Loader=Transaction,",
					"Hash=", newTransactionCount.TransactionHash,
					" Type=", newTransactionCount.Type,
					" - Error: ", err.Error())
			}
			newTransactionCount.Count = uint64(count)

			err = GetTransactionCountModel().UpsertOne(newTransactionCount)
			zap.S().Debug(
				"Loader=Transaction,",
				"Hash=", newTransactionCount.TransactionHash,
				" Type=", newTransactionCount.Type,
				" - Upsert")
			if err != nil {
				// Postgres error
				zap.S().Fatal(
					"Loader=Transaction,",
					"Hash=", newTransactionCount.TransactionHash,
					" Type=", newTransactionCount.Type,
					" - Error: ", err.Error())
			}
		}
	}()
}
