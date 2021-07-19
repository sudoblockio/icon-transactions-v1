package crud

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/geometry-labs/icon-transactions/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
	"sync"
	"time"
)

type MongoConn struct {
	client *mongo.Client
	ctx    context.Context
}

var mongoInstance *MongoConn
var mongoConnOnce sync.Once

func GetMongoConn() *MongoConn {
	mongoConnOnce.Do(func() {
		uri := fmt.Sprintf("%s://%s:%s", config.Config.DbDriver, config.Config.DbHost, config.Config.DbPort) //"mongodb://localhost:27017"
		// Getting client (or session)
		client, err := retryMongoConn(uri)
		if err != nil {
			zap.S().Fatal("MONGO: Finally Connection cannot be established")
		} else {
			zap.S().Info("MONGO: Finally Connection established")
		}

		//ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		//defer cancel()
		ctx, _ := context.WithCancel(context.Background())
		//go func() {
		//	<-global.ShutdownChan
		//	zap.S().Info("Closing Mongodb client context")
		//	defer cancel()
		//}()

		err = client.Connect(ctx)
		if err != nil {
			zap.S().Fatal("Cannot connect to context for mongodb", err)
		}
		mongoInstance = &MongoConn{
			client: client,
			ctx:    ctx,
		}

		err = mongoInstance.retryPing()
		if err != nil {
			zap.S().Fatal("MONGO: Finally cannot ping mongodb")
		} else {
			zap.S().Info("MONGO: Finally pinged mongodb")
		}
	})

	return mongoInstance
}

func (m *MongoConn) GetClient() *mongo.Client {
	return m.client
}

func (m *MongoConn) GetCtx() context.Context {
	return m.ctx
}

func (m *MongoConn) Close() error {
	err := m.client.Disconnect(m.ctx)
	if err != nil {
		zap.S().Fatal("Cannot disconnect from mongodb", err)
	}
	return err
}

func (m *MongoConn) Ping() error {
	ctx, _ := context.WithTimeout(context.TODO(), 5*time.Second)
	err := m.client.Ping(ctx, readpref.Primary())
	if err != nil {
		zap.S().Info("Cannot ping mongodb", err)
	}
	return err
}

func (m *MongoConn) retryPing() error {
	operation := func() error {
		return m.Ping()
	}
	neb := backoff.NewExponentialBackOff()
	err := backoff.Retry(operation, neb)

	return err
}

func (m *MongoConn) ListAllDatabases() []string {
	databases, err := m.client.ListDatabaseNames(m.ctx, bson.M{})
	if err != nil {
		zap.S().Fatal("Cannot List databases", err)
	}
	return databases
}

func (m *MongoConn) DatabaseHandle(database string) *mongo.Database {
	return m.client.Database(database)
}

func retryMongoConn(uri string) (*mongo.Client, error) {
	var client *mongo.Client
	operation := func() error {
		cli, err := mongo.NewClient(options.Client().ApplyURI(uri).SetAuth(options.Credential{
			AuthMechanism:           "",
			AuthMechanismProperties: nil,
			AuthSource:              "",
			Username:                config.Config.DbUser,     //"mongo",
			Password:                config.Config.DbPassword, //"changethis",
			PasswordSet:             true,
		}))
		if err != nil {
			zap.S().Fatal("Cannot create a connection to mongodb", err)
		} else {
			client = cli
		}
		return err
	}
	neb := backoff.NewExponentialBackOff()
	err := backoff.Retry(operation, neb)
	return client, err
}
