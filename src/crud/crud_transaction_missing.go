package crud

import (
	"reflect"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/geometry-labs/icon-transactions/models"
)

// TransactionMissingModel - type for transactionMissing table model
type TransactionMissingModel struct {
	db            *gorm.DB
	model         *models.TransactionMissing
	modelORM      *models.TransactionMissingORM
	LoaderChannel chan *models.TransactionMissing
}

var transactionMissingModel *TransactionMissingModel
var transactionMissingModelOnce sync.Once

// GetTransactionMissingModel - create and/or return the transactionMissings table model
func GetTransactionMissingModel() *TransactionMissingModel {
	transactionMissingModelOnce.Do(func() {
		dbConn := getPostgresConn()
		if dbConn == nil {
			zap.S().Fatal("Cannot connect to postgres database")
		}

		transactionMissingModel = &TransactionMissingModel{
			db:            dbConn,
			model:         &models.TransactionMissing{},
			modelORM:      &models.TransactionMissingORM{},
			LoaderChannel: make(chan *models.TransactionMissing, 1),
		}

		err := transactionMissingModel.Migrate()
		if err != nil {
			zap.S().Fatal("TransactionMissingModel: Unable migrate postgres table: ", err.Error())
		}

		StartTransactionMissingLoader()
	})

	return transactionMissingModel
}

// Migrate - migrate transactionMissings table
func (m *TransactionMissingModel) Migrate() error {
	// Only using TransactionMissingRawORM (ORM version of the proto generated struct) to create the TABLE
	err := m.db.AutoMigrate(m.modelORM) // Migration and Index creation
	return err
}

func (m *TransactionMissingModel) UpsertOne(
	transactionMissing *models.TransactionMissing,
) error {
	db := m.db

	// map[string]interface{}
	updateOnConflictValues := extractFilledFieldsFromModel(
		reflect.ValueOf(*transactionMissing),
		reflect.TypeOf(*transactionMissing),
	)

	// Upsert
	db = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "hash"}}, // NOTE set to primary keys for table
		DoUpdates: clause.Assignments(updateOnConflictValues),
	}).Create(transactionMissing)

	return db.Error
}

// StartTransactionMissingLoader starts loader
func StartTransactionMissingLoader() {
	go func() {
		postgresLoaderChan := GetTransactionMissingModel().LoaderChannel

		for {
			// Read transactionMissing
			newTransactionMissing := <-postgresLoaderChan

			//////////////////////
			// Load to postgres //
			//////////////////////
			err := GetTransactionMissingModel().UpsertOne(newTransactionMissing)
			zap.S().Debug("Loader=TransactionMissing, Hash=", newTransactionMissing.Hash, " - Upserted")
			if err != nil {
				// Postgres error
				zap.S().Info("Loader=TransactionMissing, Hash=", newTransactionMissing.Hash, " - FATAL")
				zap.S().Fatal(err.Error())
			}
		}
	}()
}
