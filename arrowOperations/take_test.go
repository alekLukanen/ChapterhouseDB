package arrowops

import (
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

func BenchmarkTakeRecord(b *testing.B) {
	mem := memory.NewGoAllocator()

	size := 1_000_000

	// create large records to compare
	r1 := mockData(mem, 1_000_000)
	defer r1.Release()

	r2 := mockData(mem, 1_000_000)
	defer r2.Release()

	// create indices every 10th row
	indicesBuilder := array.NewUint32Builder(mem)
	defer indicesBuilder.Release()
	for i := uint32(0); i < uint32(r1.NumRows()); i += 10 {
		indicesBuilder.Append(i)
	}

	b.ResetTimer()

	if val, ifErr := TakeRecord(mem, r1, indicesBuilder.NewUint32Array()); ifErr != nil {
		b.Fatalf("received error while taking rows '%s'", ifErr)
	} else if val == nil || val.NumRows() != int64(size/10) {
		b.Fatalf("expected taken record to have %d rows", size/10)
	}

}

func TestTakeRecord(t *testing.T) {
	mem := memory.NewGoAllocator()

	// recrod to test
	rb1 := array.NewRecordBuilder(mem, arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "b", Type: arrow.PrimitiveTypes.Float32},
			{Name: "c", Type: arrow.BinaryTypes.String},
		},
		nil,
	))
	defer rb1.Release()
	rb1.Field(0).(*array.Uint32Builder).AppendValues([]uint32{1, 2, 3}, nil)
	rb1.Field(1).(*array.Float32Builder).AppendValues([]float32{1.0, 2.0, 3.0}, nil)
	rb1.Field(2).(*array.StringBuilder).AppendValues([]string{"s1", "s2", "s3"}, nil)

	record := rb1.NewRecord()
	defer record.Release()

	// expected record
	rb2 := array.NewRecordBuilder(mem, arrow.NewSchema(
		[]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "b", Type: arrow.PrimitiveTypes.Float32},
			{Name: "c", Type: arrow.BinaryTypes.String},
		},
		nil,
	))
	defer rb2.Release()
	rb2.Field(0).(*array.Uint32Builder).AppendValues([]uint32{3, 1}, []bool{true, true})
	rb2.Field(1).(*array.Float32Builder).AppendValues([]float32{3.0, 1.0}, nil)
	rb2.Field(2).(*array.StringBuilder).AppendValues([]string{"s3", "s1"}, nil)

	expectedRecord := rb2.NewRecord()
	defer expectedRecord.Release()

	// take rows from the record
	indicesBuilder := array.NewUint32Builder(mem)
	defer indicesBuilder.Release()
	indicesBuilder.AppendValues([]uint32{2, 0}, nil)

	indices := indicesBuilder.NewUint32Array()
	defer indices.Release()

	takenRecord, err := TakeRecord(mem, record, indices)
	if err != nil {
		t.Errorf("TakeRecord() error = %v, wantErr %v", err, nil)
		return
	}
	defer takenRecord.Release()

	if eq, ifErr := RecordsEqual(expectedRecord, takenRecord); ifErr != nil {
		t.Errorf("received error while comparing records: %s", ifErr)
	} else if !eq {
		t.Errorf("TakeRecord() = %v, want %v", takenRecord, expectedRecord)
	}

}
