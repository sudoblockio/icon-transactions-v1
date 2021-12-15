package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-transactions/models"
)

// TransactionCreateScoreModel - type for address table model
type TransactionCreateScoreModel struct {
	db            *gorm.DB
	model         *models.TransactionCreateScore
	modelORM      *models.TransactionCreateScoreORM
	LoaderChannel chan *models.TransactionCreateScore
}

var transactionCreateScoreModel *TransactionCreateScoreModel
var transactionCreateScoreModelOnce sync.Once

// GetAddressModel - create and/or return the addresss table model
func GetTransactionCreateScoreModel() *TransactionCreateScoreModel {
	transactionCreateScoreModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionCreateScoreModel = &TransactionCreateScoreModel{
			db:            dbConn,
			model:         &models.TransactionCreateScore{},
			LoaderChannel: make(chan *models.TransactionCreateScore, 1),
		}

		err := transactionCreateScoreModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionCreateScoreModel: Unable migrate postgres table: ", err.Error())
		}

		StartTransactionCreateScoreLoader()
	})

	return transactionCreateScoreModel
}

// Migrate - migrate transactionCreateScores table
func (m *TransactionCreateScoreModel) Migrate() error {
	// Only using TransactionCreateScoreRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

// Select - select from transactionCreateScores table
func (m *TransactionCreateScoreModel) SelectOne(creationTransactionHash string) (*models.TransactionCreateScore, error) {
	db := m.db

	// Set table
	db = db.Model(&models.TransactionCreateScore{})

	// Creation Transaction Hash
	db = db.Where("creation_transaction_hash = ?", creationTransactionHash)

	transactionCreateScore := &models.TransactionCreateScore{}
	db = db.First(transactionCreateScore)

	return transactionCreateScore, db.Error
}

func (m *TransactionCreateScoreModel) UpsertOne(
	transactionCreateScore *models.TransactionCreateScore,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*transactionCreateScore),
		reflect.TypeOf(*transactionCreateScore),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "creation_transaction_hash"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(transactionCreateScore)

	return db.Error
}

// StartTransactionCreateScoreLoader starts loader
func StartTransactionCreateScoreLoader() {
	go func() {
		postgresLoaderChan := GetTransactionCreateScoreModel().LoaderChannel

		for {
			// Read transaction
			newTransactionCreateScore := <-postgresLoaderChan

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetTransactionCreateScoreModel().UpsertOne(newTransactionCreateScore)
			zap.S().Debug("Loader=TransactionCreateScore, CreationTransactionHash=", newTransactionCreateScore.CreationTransactionHash, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Info("Loader=TransactionCreateScore, CreationTransactionHash=", newTransactionCreateScore.CreationTransactionHash, " - FATAL")
				zap.S().Fatal(err.Error())
			}

			// Reload transaction
			reloadTransaction(newTransactionCreateScore.CreationTransactionHash)
		}
	}()
}
