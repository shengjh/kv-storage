package codec

import (
	"fmt"
)

func MvccEncode(key []byte, ts uint64) string {
	//TODO: should we encode key to memory comparable
	formatTS := fmt.Sprintf("%0#16x", ^ts)
	return string(key) + "_" + formatTS
}

