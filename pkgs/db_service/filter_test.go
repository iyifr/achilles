package dbservice

import (
	"testing"
)

// Test data
var testDoc = map[string]interface{}{
	"name":     "John Doe",
	"age":      30,
	"salary":   75000.50,
	"active":   true,
	"city":     "New York",
	"tags":     []interface{}{"developer", "golang", "backend"},
	"score":    95,
	"level":    5,
	"category": "engineering",
}

var largeInArray = make([]interface{}, 100)

func init() {
	for i := 0; i < 100; i++ {
		largeInArray[i] = i
	}
}

// Unit tests for matchesFilter
func TestMatchesFilter_EmptyFilter(t *testing.T) {
	result, err := matchesFilter(testDoc, map[string]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected true for empty filter")
	}
}

func TestMatchesFilter_SimpleEquality(t *testing.T) {
	filter := map[string]interface{}{"name": "John Doe"}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected match for simple equality")
	}
}

func TestMatchesFilter_SimpleEqualityNoMatch(t *testing.T) {
	filter := map[string]interface{}{"name": "Jane Doe"}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result {
		t.Fatal("expected no match")
	}
}

func TestMatchesFilter_NumericEquality(t *testing.T) {
	filter := map[string]interface{}{"age": 30}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected match for numeric equality")
	}
}

func TestMatchesFilter_OperatorGt(t *testing.T) {
	filter := map[string]interface{}{"age": map[string]interface{}{"$gt": 25}}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected match for $gt")
	}
}

func TestMatchesFilter_OperatorLt(t *testing.T) {
	filter := map[string]interface{}{"age": map[string]interface{}{"$lt": 35}}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected match for $lt")
	}
}

func TestMatchesFilter_OperatorGte(t *testing.T) {
	filter := map[string]interface{}{"age": map[string]interface{}{"$gte": 30}}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected match for $gte")
	}
}

func TestMatchesFilter_OperatorLte(t *testing.T) {
	filter := map[string]interface{}{"age": map[string]interface{}{"$lte": 30}}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected match for $lte")
	}
}

func TestMatchesFilter_OperatorIn(t *testing.T) {
	filter := map[string]interface{}{"city": map[string]interface{}{"$in": []interface{}{"New York", "Boston", "Chicago"}}}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected match for $in")
	}
}

func TestMatchesFilter_OperatorInNoMatch(t *testing.T) {
	filter := map[string]interface{}{"city": map[string]interface{}{"$in": []interface{}{"Boston", "Chicago"}}}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result {
		t.Fatal("expected no match for $in")
	}
}

func TestMatchesFilter_OperatorNin(t *testing.T) {
	filter := map[string]interface{}{"city": map[string]interface{}{"$nin": []interface{}{"Boston", "Chicago"}}}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected match for $nin")
	}
}

func TestMatchesFilter_OperatorNe(t *testing.T) {
	filter := map[string]interface{}{"name": map[string]interface{}{"$ne": "Jane Doe"}}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected match for $ne")
	}
}

func TestMatchesFilter_And(t *testing.T) {
	filter := map[string]interface{}{
		"$and": []interface{}{
			map[string]interface{}{"age": map[string]interface{}{"$gt": 25}},
			map[string]interface{}{"active": true},
		},
	}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected match for $and")
	}
}

func TestMatchesFilter_AndNoMatch(t *testing.T) {
	filter := map[string]interface{}{
		"$and": []interface{}{
			map[string]interface{}{"age": map[string]interface{}{"$gt": 35}},
			map[string]interface{}{"active": true},
		},
	}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result {
		t.Fatal("expected no match for $and")
	}
}

func TestMatchesFilter_Or(t *testing.T) {
	filter := map[string]interface{}{
		"$or": []interface{}{
			map[string]interface{}{"age": map[string]interface{}{"$gt": 35}},
			map[string]interface{}{"active": true},
		},
	}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected match for $or")
	}
}

func TestMatchesFilter_OrNoMatch(t *testing.T) {
	filter := map[string]interface{}{
		"$or": []interface{}{
			map[string]interface{}{"age": map[string]interface{}{"$gt": 35}},
			map[string]interface{}{"active": false},
		},
	}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result {
		t.Fatal("expected no match for $or")
	}
}

func TestMatchesFilter_ArrContains(t *testing.T) {
	filter := map[string]interface{}{"tags": map[string]interface{}{"$arrContains": []interface{}{"golang"}}}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected match for $arrContains")
	}
}

func TestMatchesFilter_MultipleConditions(t *testing.T) {
	filter := map[string]interface{}{
		"age":    map[string]interface{}{"$gte": 25, "$lte": 35},
		"active": true,
	}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Fatal("expected match for multiple conditions")
	}
}

func TestMatchesFilter_FieldNotExists(t *testing.T) {
	filter := map[string]interface{}{"nonexistent": "value"}
	result, err := matchesFilter(testDoc, filter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result {
		t.Fatal("expected no match for non-existent field")
	}
}

// Benchmarks
func BenchmarkMatchesFilter_EmptyFilter(b *testing.B) {
	filter := map[string]interface{}{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchesFilter(testDoc, filter)
	}
}

func BenchmarkMatchesFilter_SimpleEquality(b *testing.B) {
	filter := map[string]interface{}{"name": "John Doe"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchesFilter(testDoc, filter)
	}
}

func BenchmarkMatchesFilter_NumericEquality(b *testing.B) {
	filter := map[string]interface{}{"age": 30}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchesFilter(testDoc, filter)
	}
}

func BenchmarkMatchesFilter_OperatorGt(b *testing.B) {
	filter := map[string]interface{}{"age": map[string]interface{}{"$gt": 25}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchesFilter(testDoc, filter)
	}
}

func BenchmarkMatchesFilter_OperatorLt(b *testing.B) {
	filter := map[string]interface{}{"age": map[string]interface{}{"$lt": 35}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchesFilter(testDoc, filter)
	}
}

func BenchmarkMatchesFilter_OperatorIn_Small(b *testing.B) {
	filter := map[string]interface{}{"city": map[string]interface{}{"$in": []interface{}{"New York", "Boston", "Chicago"}}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchesFilter(testDoc, filter)
	}
}

func BenchmarkMatchesFilter_OperatorIn_Large(b *testing.B) {
	filter := map[string]interface{}{"score": map[string]interface{}{"$in": largeInArray}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchesFilter(testDoc, filter)
	}
}

func BenchmarkMatchesFilter_OperatorNin_Small(b *testing.B) {
	filter := map[string]interface{}{"city": map[string]interface{}{"$nin": []interface{}{"Boston", "Chicago", "LA"}}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchesFilter(testDoc, filter)
	}
}

func BenchmarkMatchesFilter_OperatorNin_Large(b *testing.B) {
	filter := map[string]interface{}{"score": map[string]interface{}{"$nin": largeInArray}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchesFilter(testDoc, filter)
	}
}

func BenchmarkMatchesFilter_And(b *testing.B) {
	filter := map[string]interface{}{
		"$and": []interface{}{
			map[string]interface{}{"age": map[string]interface{}{"$gt": 25}},
			map[string]interface{}{"active": true},
			map[string]interface{}{"city": "New York"},
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchesFilter(testDoc, filter)
	}
}

func BenchmarkMatchesFilter_Or(b *testing.B) {
	filter := map[string]interface{}{
		"$or": []interface{}{
			map[string]interface{}{"age": map[string]interface{}{"$gt": 35}},
			map[string]interface{}{"active": true},
			map[string]interface{}{"city": "Boston"},
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchesFilter(testDoc, filter)
	}
}

func BenchmarkMatchesFilter_ArrContains(b *testing.B) {
	filter := map[string]interface{}{"tags": map[string]interface{}{"$arrContains": []interface{}{"golang", "python", "rust"}}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchesFilter(testDoc, filter)
	}
}

func BenchmarkMatchesFilter_ComplexFilter(b *testing.B) {
	filter := map[string]interface{}{
		"$and": []interface{}{
			map[string]interface{}{
				"$or": []interface{}{
					map[string]interface{}{"age": map[string]interface{}{"$gt": 25}},
					map[string]interface{}{"salary": map[string]interface{}{"$gte": 50000}},
				},
			},
			map[string]interface{}{"active": true},
			map[string]interface{}{"city": map[string]interface{}{"$in": []interface{}{"New York", "Boston", "Chicago"}}},
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchesFilter(testDoc, filter)
	}
}

func BenchmarkMatchesFilter_MultipleConditions(b *testing.B) {
	filter := map[string]interface{}{
		"age":    map[string]interface{}{"$gte": 25, "$lte": 35},
		"active": true,
		"city":   "New York",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matchesFilter(testDoc, filter)
	}
}

func BenchmarkObjectsEqual_SameString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		objectsEqual("test", "test")
	}
}

func BenchmarkObjectsEqual_SameInt(b *testing.B) {
	for i := 0; i < b.N; i++ {
		objectsEqual(42, 42)
	}
}

func BenchmarkObjectsEqual_IntFloat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		objectsEqual(42, 42.0)
	}
}

func BenchmarkCompareNumbers_Ints(b *testing.B) {
	for i := 0; i < b.N; i++ {
		compareNumbers(30, 25)
	}
}

func BenchmarkCompareNumbers_Floats(b *testing.B) {
	for i := 0; i < b.N; i++ {
		compareNumbers(30.5, 25.5)
	}
}

func BenchmarkToFloat64_Int(b *testing.B) {
	for i := 0; i < b.N; i++ {
		toFloat64(42)
	}
}

func BenchmarkToFloat64_Float64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		toFloat64(42.0)
	}
}
