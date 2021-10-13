package crud

import (
	"errors"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-transactions/models"
)

// TokenTransferCountByTokenContractModel - type for address table model
type TokenTransferCountByTokenContractModel struct {
	db            *gorm.DB
	model         *models.TokenTransferCountByTokenContract
	modelORM      *models.TokenTransferCountByTokenContractORM
	LoaderChannel chan *models.TokenTransferCountByTokenContract
}

var tokenTransferCountByTokenContractModel *TokenTransferCountByTokenContractModel
var tokenTransferCountByTokenContractModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
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

// Insert - Insert tokenTransferCountByTokenContract into table
func (m *TokenTransferCountByTokenContractModel) Insert(tokenTransferCountByTokenContract *models.TokenTransferCountByTokenContract) error {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCountByTokenContract{})

	db = db.Create(tokenTransferCountByTokenContract)

	return db.Error
}

// Select - select from tokenTransferCountByTokenContracts table
func (m *TokenTransferCountByTokenContractModel) SelectOne(transactionHash string, logIndex uint64, tokenContractAddress string) (models.TokenTransferCountByTokenContract, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCountByTokenContract{})

	// Transaction Hash
	db = db.Where("transaction_hash = ?", transactionHash)

	// Log Index
	db = db.Where("log_index = ?", logIndex)

	// Token contract address
	db = db.Where("token_contract_address = ?", tokenContractAddress)

	tokenTransferCountByTokenContract := models.TokenTransferCountByTokenContract{}
	db = db.First(&tokenTransferCountByTokenContract)

	return tokenTransferCountByTokenContract, db.Error
}

func (m *TokenTransferCountByTokenContractModel) SelectLargestCountByTokenContract(tokenContractAddress string) (uint64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransferCountByTokenContract{})

	db = db.Where("token_contract_address = ?", tokenContractAddress)

	// Get max id
	count := uint64(0)
	row := db.Select("max(count)").Row()
	row.Scan(&count)

	return count, db.Error
}

// StartTokenTransferCountByTokenContractLoader starts loader
func StartTokenTransferCountByTokenContractLoader() {
	go func() {

		for {
			// Read tokenTransferCountByTokenContract
			newTokenTransferCountByTokenContract := <-GetTokenTransferCountByTokenContractModel().LoaderChannel

			// Insert
			_, err := GetTokenTransferCountByTokenContractModel().SelectOne(
				newTokenTransferCountByTokenContract.TransactionHash,
				newTokenTransferCountByTokenContract.LogIndex,
				newTokenTransferCountByTokenContract.TokenContractAddress,
			)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Last count
				lastCount, err := GetTokenTransferCountByTokenContractModel().SelectLargestCountByTokenContract(
					newTokenTransferCountByTokenContract.TokenContractAddress,
				)
				if err != nil {
					zap.S().Fatal(err.Error())
				}
				newTokenTransferCountByTokenContract.Count = lastCount + 1

				// Insert
				err = GetTokenTransferCountByTokenContractModel().Insert(newTokenTransferCountByTokenContract)
				if err != nil {
					zap.S().Warn(err.Error())
				}

				zap.S().Debug("Loader=TokenTransferCountByTokenContract, TokenContractAddress=", newTokenTransferCountByTokenContract.TokenContractAddress, " - Insert")
			} else if err != nil {
				// Error
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
