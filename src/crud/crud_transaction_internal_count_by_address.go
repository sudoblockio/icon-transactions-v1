package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/models"
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

// Insert - Insert transactionInternalCountByAddress into table
func (m *TransactionInternalCountByAddressModel) Insert(transactionInternalCountByAddress *models.TransactionInternalCountByAddress) error {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionInternalCountByAddress{})

	db = db.Create(transactionInternalCountByAddress)

	return db.Error
}

// Select - select from transactionInternalCountByAddresss table
func (m *TransactionInternalCountByAddressModel) SelectOne(transactionHash string, logIndex uint64, address string) (models.TransactionInternalCountByAddress, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionInternalCountByAddress{})

	// Transaction Hash
	db = db.Where("transaction_hash = ?", transactionHash)

	// Log Index
	db = db.Where("log_index = ?", logIndex)

	// Address
	db = db.Where("address = ?", address)

	transactionInternalCountByAddress := models.TransactionInternalCountByAddress{}
	db = db.First(&transactionInternalCountByAddress)

	return transactionInternalCountByAddress, db.Error
}

func (m *TransactionInternalCountByAddressModel) SelectLargestCountByAddress(address string) (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionInternalCountByAddress{})

	db = db.Where("address = ?", address)

	// Get max id
	count := uint64(0)
	row := db.Select("max(count)").Row()
	row.Scan(&count)

	return count, db.Error
}

// StartTransactionInternalCountByAddressLoader starts loader
func StartTransactionInternalCountByAddressLoader() {
	go func() {

		for {
			// Read transactionInternalCountByAddress
			newTransactionInternalCountByAddress := <-GetTransactionInternalCountByAddressModel().LoaderChannel

			// Insert
			_, err := GetTransactionInternalCountByAddressModel().SelectOne(
				newTransactionInternalCountByAddress.TransactionHash,
				newTransactionInternalCountByAddress.LogIndex,
				newTransactionInternalCountByAddress.Address,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Last count
				lastCount, err := GetTransactionInternalCountByAddressModel().SelectLargestCountByAddress(
					newTransactionInternalCountByAddress.Address,
				)
				if err != nil {
					zap.S().Fatal(err.Error())
				}
				newTransactionInternalCountByAddress.Count = lastCount + 1

				// Insert
				err = GetTransactionInternalCountByAddressModel().Insert(newTransactionInternalCountByAddress)
				if err != nil {
					zap.S().Fatal(err.Error())
				}

				zap.S().Debug("Loader=TransactionInternalCountByAddress, Address=", newTransactionInternalCountByAddress.Address, " - Insert")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
