package crud

import (
	"errors"
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-transactions/models"
)

// TokenTransferModel - type for tokenTransfer table model
type TokenTransferModel struct {
	db            *gorm.DB
	model         *models.TokenTransfer
	modelORM      *models.TokenTransferORM
	LoaderChannel chan *models.TokenTransfer
}

var tokenTransferModel *TokenTransferModel
var tokenTransferModelOnce sync.Once

// GetTokenTransferModel - create and/or return the tokenTransfers table model
func GetTokenTransferModel() *TokenTransferModel {
	tokenTransferModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		tokenTransferModel = &TokenTransferModel{
			db:            dbConn,
			model:         &models.TokenTransfer{},
			LoaderChannel: make(chan *models.TokenTransfer, 1),
		}

		err := tokenTransferModel.Migrate()
		if err != nil {
			zap.S().Fatal("TokenTransferModel: Unable migrate postgres table: ", err.Error())
		}

		StartTokenTransferLoader()
	})

	return tokenTransferModel
}

// Migrate - migrate tokenTransfers table
func (m *TokenTransferModel) Migrate() error {
	// Only using TokenTransferRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// SelectOne - select from token_transfers table
// Returns: models, error (if present)
func (m *TokenTransferModel) SelectOne(
	transactionHash string,
	logIndex int32,
) (*models.TokenTransfer, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.TokenTransfer{})

	// Transaction Hash
	db = db.Where("transaction_hash = ?", transactionHash)

	// Log Index
	db = db.Where("log_index = ?", logIndex)

	tokenTransfer := &models.TokenTransfer{}
	db = db.First(tokenTransfer)

	return tokenTransfer, db.Error
}

// SelectMany - select from token_transfers table
// Returns: models, error (if present)
func (m *TokenTransferModel) SelectMany(
	limit int,
	skip int,
	from string,
	to string,
	blockNumber int,
	transactionHash string,
) (*[]models.TokenTransfer, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.TokenTransfer{})

	// Latest transactions first
	db = db.Order("block_number desc")

	// from
	if from != "" {
		db = db.Where("from_address = ?", from)
	}

	// to
	if to != "" {
		db = db.Where("to_address = ?", to)
	}

	// block number
	if blockNumber != 0 {
		db = db.Where("block_number = ?", blockNumber)
	}

	// transaction hash
	if transactionHash != "" {
		db = db.Where("transaction_hash = ?", transactionHash)
	}

	// Limit is required and defaulted to 1
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	tokenTransfers := &[]models.TokenTransfer{}
	db = db.Find(tokenTransfers)

	return tokenTransfers, db.Error
}

// SelectManyByAddress - select from token_transfers table by address
// Returns: models, error (if present)
func (m *TokenTransferModel) SelectManyByAddress(
	limit int,
	skip int,
	address string,
) (*[]models.TokenTransfer, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.TokenTransfer{})

	// Latest transactions first
	db = db.Order("block_number desc")

	// Address
	db = db.Where(`(transaction_hash, log_index)
	IN (
		SELECT
			transaction_hash, log_index
		FROM
			token_transfer_count_by_address_indices
		where
			address = ?
	)`, address)

	// Limit is required and defaulted to 1
	// Note: Count before setting limit
	db = db.Limit(limit)

	// Skip
	// Note: Count before setting skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	tokenTransfers := &[]models.TokenTransfer{}
	db = db.Find(tokenTransfers)

	return tokenTransfers, db.Error
}

// SelectManyByTokenContracAddress - select from token_transfers table by token contract address
// Returns: models, error (if present)
func (m *TokenTransferModel) SelectManyByTokenContractAddress(
	limit int,
	skip int,
	tokenContractAddress string,
) (*[]models.TokenTransfer, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.TokenTransfer{})

	// Latest transactions first
	db = db.Order("block_number desc")

	// address
	db = db.Where("token_contract_address = ?", tokenContractAddress)

	// Limit is required and defaulted to 1
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	tokenTransfers := &[]models.TokenTransfer{}
	db = db.Find(tokenTransfers)

	return tokenTransfers, db.Error
}

// SelectManyDistinctTokenContracts - select from token_transfers
func (m *TokenTransferModel) SelectManyDistinctTokenContracts(
	limit int,
	skip int,
) (*[]models.TokenTransfer, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.TokenTransfer{})

	// Distinct
	db = db.Distinct("token_contract_address")

	// Limit is required and defaulted to 1
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	tokenTransfers := &[]models.TokenTransfer{}
	db = db.Find(tokenTransfers)

	return tokenTransfers, db.Error
}

// SelectManyDistinctFromAddress - select from token_transfers
func (m *TokenTransferModel) SelectManyDistinctFromAddresses(
	limit int,
	skip int,
) (*[]models.TokenTransfer, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.TokenTransfer{})

	// Distinct
	db = db.Distinct("from_address", "token_contract_address")

	// Limit is required and defaulted to 1
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	tokenTransfers := &[]models.TokenTransfer{}
	db = db.Find(tokenTransfers)

	return tokenTransfers, db.Error
}

// SelectManyDistinctToAddress - select from token_transfers
func (m *TokenTransferModel) SelectManyDistinctToAddresses(
	limit int,
	skip int,
) (*[]models.TokenTransfer, error) {
	db := m.db

	// Set table
	db = db.Model(&[]models.TokenTransfer{})

	// Distinct
	db = db.Distinct("to_address", "token_contract_address")

	// Limit is required and defaulted to 1
	db = db.Limit(limit)

	// Skip
	if skip != 0 {
		db = db.Offset(skip)
	}

	tokenTransfers := &[]models.TokenTransfer{}
	db = db.Find(tokenTransfers)

	return tokenTransfers, db.Error
}

// Count - Count all token transfers
// NOTE very slow operation
func (m *TokenTransferModel) Count() (int64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransfer{})

	count := int64(0)
	db = db.Count(&count)

	return count, db.Error
}

// CountByTokenContract - Count by token contract
func (m *TokenTransferModel) CountByTokenContract(tokenContractAddress string) (int64, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TokenTransfer{})

	db = db.Where("token_contract_address = ?", tokenContractAddress)

	count := int64(0)
	db = db.Count(&count)

	return count, db.Error
}

func (m *TokenTransferModel) UpsertOne(
	tokenTransfer *models.TokenTransfer,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*tokenTransfer),
		reflect.TypeOf(*tokenTransfer),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "transaction_hash"}, {Name: "log_index"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(tokenTransfer)

	return db.Error
}

// StartTokenTransferLoader starts loader
func StartTokenTransferLoader() {
	go func() {
		postgresLoaderChan := GetTokenTransferModel().LoaderChannel

		for {
			// Read tokenTransfer
			newTokenTransfer := <-postgresLoaderChan

			/////////////////
			// Enrichments //
			/////////////////
			transactionFee := ""

			// Transaction Fee
			transaction, err := GetTransactionModel().SelectOne(newTokenTransfer.TransactionHash, -1)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// No block_time entry yet
				transactionFee = "0x0"
			} else if err == nil {
				// Success
				transactionFee = transaction.TransactionFee
			} else {
				// Postgres error
				zap.S().Fatal(err.Error())
			}

			newTokenTransfer.TransactionFee = transactionFee

			//////////////////////
			// Load to postgres //
			//////////////////////
			err = GetTokenTransferModel().UpsertOne(newTokenTransfer)
			zap.S().Debug("Loader=TokenTransfer, Hash=", newTokenTransfer.TransactionHash, " LogIndex=", newTokenTransfer.LogIndex, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Info("Loader=TokenTransfer, Hash=", newTokenTransfer.TransactionHash, " LogIndex=", newTokenTransfer.LogIndex, " - FATAL")
				zap.S().Fatal(err.Error())
			}
		}
	}()
}

// reloadTokenTransfer - Send block back to loader for updates
func reloadTokenTransfer(transactionHash string, logIndex int32) error {

	curTokenTransfer, err := GetTokenTransferModel().SelectOne(transactionHash, logIndex)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// Create empty token transfer
		curTokenTransfer = &models.TokenTransfer{}
		curTokenTransfer.TransactionHash = transactionHash
		curTokenTransfer.LogIndex = logIndex
	} else if err != nil {
		// Postgres error
		return err
	}
	GetTokenTransferModel().LoaderChannel <- curTokenTransfer

	return nil
}
