package crud

import (
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

// SelectManyAPI - select from token_transfers table
// Returns: models, total count (if filters), error (if present)
func (m *TokenTransferModel) SelectMany(
	limit int,
	skip int,
	from string,
	to string,
	blockNumber int,
) (*[]models.TokenTransfer, int64, error) {
	db := m.db
	computeCount := false

	// Set table
	db = db.Model(&[]models.TokenTransfer{})

	// Latest transactions first
	db = db.Order("block_number desc")

	// from
	if from != "" {
		computeCount = true
		db = db.Where("from_address = ?", from)
	}

	// to
	if to != "" {
		computeCount = true
		db = db.Where("to_address = ?", to)
	}

	// block number
	if blockNumber != 0 {
		computeCount = true
		db = db.Where("block_number = ?", blockNumber)
	}

	// Count, if needed
	count := int64(-1)
	if computeCount {
		db.Count(&count)
	}

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

	return tokenTransfers, count, db.Error
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

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetTokenTransferModel().UpsertOne(newTokenTransfer)
			zap.S().Debug("Loader=TokenTransfer, Hash=", newTokenTransfer.TransactionHash, " LogIndex=", newTokenTransfer.LogIndex, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Info("Loader=TokenTransfer, Hash=", newTokenTransfer.TransactionHash, " LogIndex=", newTokenTransfer.LogIndex, " - FATAL")
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
