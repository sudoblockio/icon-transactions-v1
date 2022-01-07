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

	valueBigInt, success := new(big.Int).SetString(hex[2:], 16)
	if success == false {
		zap.S().Warn("Set String Error: hex=", hex)
		return 0
	}

	baseBigFloatString := "1"
	for i := 0; i < base; i++ {
		baseBigFloatString += "0"
	}
	baseBigFloat, success := new(big.Float).SetString(baseBigFloatString) // 10^(base)
	if success == false {
		zap.S().Warn("Set String Error: base=", base)
		return 0
	}

	valueBigFloat := new(big.Float).SetInt(valueBigInt)
	valueBigFloat = valueBigFloat.Quo(valueBigFloat, baseBigFloat)

	valueDecimal, success = valueBigFloat.Float64()
	if success == false {
		zap.S().Warn("Set String Error: hex=", hex)
		return 0
	}

	return valueDecimal
}
