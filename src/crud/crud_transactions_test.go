package crud_test

import (
	"context"
	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/fixtures"
	"github.com/geometry-labs/icon-transactions/logging"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"log"
	"time"
)

var _ = Describe("CrudTransactions", func() {
	config.ReadEnvironment()
	logging.StartLoggingInit()

	testFixtures, _ := fixtures.LoadTestFixtures("transaction_raws.json")
	transactionModelMongo := crud.GetTransactionModelMongo()

	Describe("TransactionModel with mongodb", func() {

		Context("Create & Select in Transactions collection", func() {
			for _, fixture := range testFixtures {
				transaction := fixture.GetTransaction(fixture.Input)

				It("insert in mongodb", func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					_, err := transactionModelMongo.RetryCreate(ctx, transaction)
					if err != nil {
						log.Println(err.Error())
					}
					Expect(err).To(BeNil())

					result := transactionModelMongo.Select(ctx, 1, 0, transaction.FromAddress, "", "")
					log.Println(result)
				}) // It
			} // For each fixture
		}) // Context "Create and Select in transaction collection"

	}) // Describe "TransactionModel with mongodb"
})
