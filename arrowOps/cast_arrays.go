package arrowops

import (
	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
)

func CastArraysToBaseDataType[T arrow.Array](arrays ...arrow.Array) ([]T, error) {
	boolArrays := make([]T, len(arrays))
	for i, arr := range arrays {
		boolArr, ok := arr.(T)
		if !ok {
			return nil, errs.NewStackError(ErrUnsupportedDataType)
		}
		boolArrays[i] = boolArr
	}
	return boolArrays, nil
}
