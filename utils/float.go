package utils

import "strconv"

func FloatToBytes(val float64) []byte {
	// prec 精度，-1意味着不特别指定小数点后的位数，f 定点格式，不使用科学计数法。
	return []byte(strconv.FormatFloat(val, 'f', -1, 64))
}

func FloatFromBytes(val []byte) float64 {
	f, _ := strconv.ParseFloat(string(val), 64)
	return f
}
