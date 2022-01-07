package utils

import (
	"math/big"

	"go.uber.org/zap"
)

func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func StringHexToFloat64(hex string, base int) float64 {
	valueDecimal := float64(0)

	valueBigInt, err := new(big.Int).SetString(hex[2:], 16)
	if err != nil {
		zap.S().Warn("StringHexToFloat64 - ERROR: ", err.Error())
		return 0
	}

	baseBigFloatString := "1"
	for i := 0; i < base; i++ {
		baseBigFloatString += "0"
	}
	baseBigFloat, err := new(big.Float).SetString(baseBigFloatString) // 10^(base)
	if err != nil {
		zap.S().Warn("StringHexToFloat64 - ERROR: ", err.Error())
		return 0
	}

	valueBigFloat := new(big.Float).SetInt(valueBigInt)
	valueBigFloat = valueBigFloat.Quo(valueBigFloat, baseBigFloat)

	valueDecimal, err = valueBigFloat.Float64()
	if err != nil {
		zap.S().Warn("StringHexToFloat64 - ERROR: ", err.Error())
		return 0
	}

	return valueDecimal
}
