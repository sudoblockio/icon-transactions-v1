package utils

import "math/big"

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

	valueBigInt, _ := new(big.Int).SetString(hex[2:], 16)

	baseBigFloatString := "1"
	for i := 0; i < base; i++ {
		baseBigFloatString += "0"
	}
	baseBigFloat, _ := new(big.Float).SetString(baseBigFloatString) // 10^(base)

	valueBigFloat := new(big.Float).SetInt(valueBigInt)
	valueBigFloat = valueBigFloat.Quo(valueBigFloat, baseBigFloat)

	valueDecimal, _ = valueBigFloat.Float64()

	return valueDecimal
}
