package internal

import (
	"fmt"
	"os"
)

var _sizeUnits = [...]string{
	"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB",
}

func FormatSize(s float64) string {
	for _, u := range _sizeUnits {
		if s < 1024.0 {
			return fmt.Sprintf("%3.1f %s", s, u)
		}
		s /= 1024.0
	}
	return fmt.Sprintf("%.1f YiB", s)
}

func GetEnv(k, def string) string {
	if v, e := os.LookupEnv(k); e {
		return v
	}
	return def
}

func LimitReached(cur, limit int) bool {
	if limit < 0 {
		return true
	}
	return cur <= limit
}
