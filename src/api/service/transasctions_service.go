package service

import (
	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/global"
	"github.com/geometry-labs/icon-blocks/models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"reflect"
	"strconv"
)

type TransactionsQuery struct {
	Page     int `query:"page"`
	PageSize int `query:"page_size"`

	Hash      string `query:"hash"`
	Height    uint32 `query:"height"`
	CreatedBy string `query:"created_by"`
	Start     uint32 `query:"start"`
}

func (service *TransactionsQuery) RunQuery() *[]models.Transaction {
	transactionsModel := global.GetGlobal().Transactions
	db := transactionsModel.GetDB()

	whereClauseStrings := service.buildWhereClauseStrings()
	orderClauseStrings := service.buildOrderClauseStrings()
	transactions := &[]models.Transaction{}
	_ = db.Scopes(Paginate(service)).
		Order(orderClauseStrings).
		Find(transactions, whereClauseStrings...)

	zap.S().Debug("Transactions: ", transactions)
	return transactions
}

func (service *TransactionsQuery) buildWhereClauseStrings() []interface{} {
	var strArr []interface{}
	if service.Height > 0 || service.Start > 0 {
		if service.Start > 0 {
			strArr = append(strArr, "number > ?", strconv.Itoa(int(service.Start)))
		} else if service.Height > 0 {
			strArr = append(strArr, "number = ?", strconv.Itoa(int(service.Height)))
		}
	}
	if service.Hash != "" {
		strArr = append(strArr, "hash = ?", service.Hash)
	}
	if service.CreatedBy != "" {
		strArr = append(strArr, "peer_id = ?", service.CreatedBy)
	}
	return strArr
}

func (service *TransactionsQuery) buildOrderClauseStrings() interface{} {
	var strArr string
	strArr = "number desc" //number desc, item_timestamp"
	return strArr
}

func (service *TransactionsQuery) buildLimitClause() int {
	empty := TransactionsQuery{}

	pageSize := 1
	if isEmpty := reflect.DeepEqual(empty, *service); isEmpty {
		return pageSize
	}

	pageSize = service.PageSize
	switch {
	case pageSize > 100:
		pageSize = config.Config.MaxPageSize
	case pageSize <= 0:
		pageSize = config.Config.MaxPageSize
	}
	return pageSize
}

func Paginate(service *TransactionsQuery) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		page := service.Page
		if page == 0 {
			page = 1
		}

		pageSize := service.buildLimitClause()

		offset := (page - 1) * pageSize
		return db.Offset(offset).Limit(pageSize)
	}
}
