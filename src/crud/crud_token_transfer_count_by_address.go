package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/models"
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

// Insert - Insert tokenTransferCountByAddress into table
func (m *TokenTransferCountByAddressModel) Insert(tokenTransferCountByAddress *models.TokenTransferCountByAddress) error {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCountByAddress{})

	db = db.Create(tokenTransferCountByAddress)

	return db.Error
}

// Select - select from tokenTransferCountByAddresss table
func (m *TokenTransferCountByAddressModel) SelectOne(transactionHash string, logIndex uint64, address string) (models.TokenTransferCountByAddress, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCountByAddress{})

	// Transaction Hash
	db = db.Where("transaction_hash = ?", transactionHash)

	// Log Index
	db = db.Where("log_index = ?", logIndex)

	// Address
	db = db.Where("address = ?", address)

	tokenTransferCountByAddress := models.TokenTransferCountByAddress{}
	db = db.First(&tokenTransferCountByAddress)

	return tokenTransferCountByAddress, db.Error
}

func (m *TokenTransferCountByAddressModel) SelectLargestCountByAddress(address string) (uint64, error) {
	return 0, nil
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCountByAddress{})

	db = db.Where("address = ?", address)

	// Get max id
	count := uint64(0)
	row := db.Select("max(count)").Row()
	row.Scan(&count)

	return count, db.Error
}

// StartTokenTransferCountByAddressLoader starts loader
func StartTokenTransferCountByAddressLoader() {
	go func() {

		for {
			// Read tokenTransferCountByAddress
			newTokenTransferCountByAddress := <-GetTokenTransferCountByAddressModel().LoaderChannel

			// Insert
			_, err := GetTokenTransferCountByAddressModel().SelectOne(
				newTokenTransferCountByAddress.TransactionHash,
				newTokenTransferCountByAddress.LogIndex,
				newTokenTransferCountByAddress.Address,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Last count
				lastCount, err := GetTokenTransferCountByAddressModel().SelectLargestCountByAddress(
					newTokenTransferCountByAddress.Address,
				)
				if err != nil {
					zap.S().Fatal(err.Error())
				}
				newTokenTransferCountByAddress.Count = lastCount + 1

				// Insert
				err = GetTokenTransferCountByAddressModel().Insert(newTokenTransferCountByAddress)
				if err != nil {
					zap.S().Warn(err.Error())
				}

				zap.S().Debug("Loader=TokenTransferCountByAddress, Address=", newTokenTransferCountByAddress.Address, " - Insert")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
