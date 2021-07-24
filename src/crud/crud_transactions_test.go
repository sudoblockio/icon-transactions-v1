package crud_test

import (
	"github.com/geometry-labs/icon-transactions/config"
	"github.com/geometry-labs/icon-transactions/crud"
	"github.com/geometry-labs/icon-transactions/fixtures"
	"github.com/geometry-labs/icon-transactions/logging"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"log"
)

var _ = Describe("CrudTransactions", func() {
	config.ReadEnvironment()
	logging.StartLoggingInit()

	testFixtures, _ := fixtures.LoadTestFixtures("transaction_raws.json")
	transactionModelMongo := crud.GetTransactionModelMongo()

	Describe("TransactionModel with mongodb", func() {

		Context("Insert in Transactions collection", func() {
			for _, fixture := range testFixtures {
				transaction := fixture.GetTransaction(fixture.Input)

				It("insert in mongodb", func() {
					_, err := transactionModelMongo.RetryCreate(transaction)
					if err != nil {
						log.Println(err.Error())
					}
					Expect(err).To(BeNil())

					//result := transactionModelMongo.Select(1,0, transaction.FromAddress, "", "")
					//kv := &crud.KeyValue{
					//	Key:   "signature",
					//	Value: transaction.Signature,
					//}
				}) // It
			} // For each fixture
		}) // Context "Insert in block collection"

	}) // Describe "BlockModel with mongodb"
})
