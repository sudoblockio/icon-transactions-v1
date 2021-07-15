package models

import (
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"math"
	"strconv"
)

func ConvertToTransactionRaw(value []byte) (*TransactionRaw, error) {
	tx := TransactionRaw{}
	err := protojson.Unmarshal(value, &tx)
	if err != nil {
		zap.S().Error("Transaction_raw_helper: Error in ConvertToTransactionRaw: %v", err)
	}
	return &tx, err
}

func ValidateHeight(heightString string) bool {
	height, err := strconv.Atoi(heightString)
	if err != nil {
		return false
	}
	if height < 0 || height > math.MaxUint32 {
		return false
	}
	return true
}
