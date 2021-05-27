package utils

import (
	"math/rand"
	"time"
)

// generates a random n digit byte array
func RandString(n int) string {
	return string(RandBytes(n))
}

func RandBytes(length int) []byte {
	all := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	digits := "0123456789"
	rand.Seed(time.Now().UnixNano())

	buf := make([]byte, length)
	buf[0] = digits[rand.Intn(len(digits))]
	for i := 1; i < length; i++ {
		buf[i] = all[rand.Intn(len(all))]
	}
	rand.Shuffle(len(buf), func(i, j int) {
		buf[i], buf[j] = buf[j], buf[i]
	})
	return buf
}

