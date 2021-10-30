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

func StringHexBase18ToFloat64(hex string) float64 {
	valueDecimal := float64(0)

	valueBigInt, _ := new(big.Int).SetString(hex[2:], 16)

	baseBigFloat, _ := new(big.Float).SetString("1000000000000000000") // 10^18
	valueBigFloat := new(big.Float).SetInt(valueBigInt)
	valueBigFloat = valueBigFloat.Quo(valueBigFloat, baseBigFloat)

	valueDecimal, _ = valueBigFloat.Float64()

	return valueDecimal
}
