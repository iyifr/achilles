package dbservice

import (
	"fmt"
	"reflect"
	"strconv"
)

// matchesFilter checks if a document's metadata matches the given filter conditions.
func matchesFilter(docMetadata map[string]any, filter map[string]any) (bool, error) {
	if len(filter) == 0 {
		return true, nil
	}

	for key, condition := range filter {
		if len(key) > 0 && key[0] == '$' {
			switch key {
			case "$and":
				match, err := evalAnd(docMetadata, condition)
				if err != nil || !match {
					return false, err
				}
			case "$or":
				match, err := evalOr(docMetadata, condition)
				if err != nil || !match {
					return false, err
				}
			default:
				return false, fmt.Errorf("unknown top-level operator %s", key)
			}
		} else {
			// Direct field filter
			val, exists := docMetadata[key]
			if !exists {
				return false, nil
			}

			match, err := checkCondition(val, condition)
			if err != nil || !match {
				return false, err
			}
		}
	}

	return true, nil
}

func evalAnd(docMetadata map[string]any, condition any) (bool, error) {
	// Try []interface{} first
	if filters, ok := condition.([]any); ok {
		for _, item := range filters {
			subFilter, ok := item.(map[string]any)
			if !ok {
				return false, fmt.Errorf("item in $and array must be an object")
			}
			match, err := matchesFilter(docMetadata, subFilter)
			if err != nil || !match {
				return false, err
			}
		}
		return true, nil
	}

	// Fallback to []map[string]interface{}
	if filters, ok := condition.([]map[string]any); ok {
		for _, subFilter := range filters {
			match, err := matchesFilter(docMetadata, subFilter)
			if err != nil || !match {
				return false, err
			}
		}
		return true, nil
	}

	return false, fmt.Errorf("$and must be an array of objects")
}

// evalOr evaluates $or operator with short-circuit on first true
func evalOr(docMetadata map[string]any, condition any) (bool, error) {
	// Try []interface{} first
	if filters, ok := condition.([]any); ok {
		for _, item := range filters {
			subFilter, ok := item.(map[string]any)
			if !ok {
				return false, fmt.Errorf("item in $or array must be an object")
			}
			match, err := matchesFilter(docMetadata, subFilter)
			if err == nil && match {
				return true, nil
			}
		}
		return false, nil
	}

	// Fallback to []map[string]interface{}
	if filters, ok := condition.([]map[string]any); ok {
		for _, subFilter := range filters {
			match, err := matchesFilter(docMetadata, subFilter)
			if err == nil && match {
				return true, nil
			}
		}
		return false, nil
	}

	return false, fmt.Errorf("$or must be an array of objects")
}

func checkCondition(docVal any, condition any) (bool, error) {
	// If condition is a map, it might contain operators like $eq, $gt, etc.
	condMap, isMap := condition.(map[string]any)
	if !isMap {
		// Direct equality check (implicit $eq)
		return objectsEqual(docVal, condition), nil
	}

	// is this an operator map?
	// Check if any key starts with '$'
	hasOperator := false
	for k := range condMap {
		if len(k) > 0 && k[0] == '$' {
			hasOperator = true
			break
		}
	}

	if !hasOperator {
		return objectsEqual(docVal, condition), nil
	}

	// Process operators
	for op, opVal := range condMap {
		if len(op) == 0 || op[0] != '$' {
			continue // skip non-operator keys
		}

		switch op {
		case "$eq":
			if !objectsEqual(docVal, opVal) {
				return false, nil
			}
		case "$ne":
			if objectsEqual(docVal, opVal) {
				return false, nil
			}
		case "$gt":
			res, err := compareNumbers(docVal, opVal)
			if err != nil || res <= 0 {
				return false, err
			}
		case "$gte":
			res, err := compareNumbers(docVal, opVal)
			if err != nil || res < 0 {
				return false, err
			}
		case "$lt":
			res, err := compareNumbers(docVal, opVal)
			if err != nil || res >= 0 {
				return false, err
			}
		case "$lte":
			res, err := compareNumbers(docVal, opVal)
			if err != nil || res > 0 {
				return false, err
			}
		case "$in":
			match, err := evalIn(docVal, opVal)
			if err != nil || !match {
				return false, err
			}
		case "$nin":
			match, err := evalNin(docVal, opVal)
			if err != nil || !match {
				return false, err
			}
		case "$arrContains":
			match, err := evalArrContains(docVal, opVal)
			if err != nil || !match {
				return false, err
			}
		default:
			return false, fmt.Errorf("unknown operator %s", op)
		}
	}
	return true, nil
}

// evalIn checks if docVal is in the opVal slice
// Avoids reflection for common slice types
func evalIn(docVal any, opVal any) (bool, error) {
	// Try direct type assertion first (avoid reflection)
	switch arr := opVal.(type) {
	case []any:
		// Linear scan - faster than hash set for single lookups (no allocations)
		for _, v := range arr {
			if objectsEqual(docVal, v) {
				return true, nil
			}
		}
		return false, nil
	case []string:
		docStr, ok := docVal.(string)
		if !ok {
			return false, nil
		}
		for _, v := range arr {
			if v == docStr {
				return true, nil
			}
		}
		return false, nil
	case []int:
		docNum, ok := toFloat64(docVal)
		if !ok {
			return false, nil
		}
		for _, v := range arr {
			if float64(v) == docNum {
				return true, nil
			}
		}
		return false, nil
	case []float64:
		docNum, ok := toFloat64(docVal)
		if !ok {
			return false, nil
		}
		for _, v := range arr {
			if v == docNum {
				return true, nil
			}
		}
		return false, nil
	default:
		// Fallback to reflection only if necessary
		val := reflect.ValueOf(opVal)
		if val.Kind() != reflect.Slice {
			return false, fmt.Errorf("$in expects an array")
		}
		for i := 0; i < val.Len(); i++ {
			if objectsEqual(docVal, val.Index(i).Interface()) {
				return true, nil
			}
		}
		return false, nil
	}
}

// evalNin checks if docVal is NOT in the opVal slice
func evalNin(docVal any, opVal any) (bool, error) {
	// Try direct type assertion first
	switch arr := opVal.(type) {
	case []any:
		for _, v := range arr {
			if objectsEqual(docVal, v) {
				return false, nil
			}
		}
		return true, nil
	case []string:
		docStr, ok := docVal.(string)
		if !ok {
			return true, nil // type mismatch means not in array
		}
		for _, v := range arr {
			if v == docStr {
				return false, nil
			}
		}
		return true, nil
	case []int:
		docNum, ok := toFloat64(docVal)
		if !ok {
			return true, nil
		}
		for _, v := range arr {
			if float64(v) == docNum {
				return false, nil
			}
		}
		return true, nil
	case []float64:
		docNum, ok := toFloat64(docVal)
		if !ok {
			return true, nil
		}
		for _, v := range arr {
			if v == docNum {
				return false, nil
			}
		}
		return true, nil
	default:
		// Fallback to reflection
		val := reflect.ValueOf(opVal)
		if val.Kind() != reflect.Slice {
			return false, fmt.Errorf("$nin expects an array")
		}
		for i := 0; i < val.Len(); i++ {
			if objectsEqual(docVal, val.Index(i).Interface()) {
				return false, nil
			}
		}
		return true, nil
	}
}

// evalArrContains checks if docVal array contains any item from opVal array
func evalArrContains(docVal any, opVal any) (bool, error) {
	// Try direct type assertions first
	docArr, ok := docVal.([]any)
	if !ok {
		// Fallback to reflection for non-standard slice types
		docReflect := reflect.ValueOf(docVal)
		if docReflect.Kind() != reflect.Slice {
			return false, fmt.Errorf("$arrContains requires the field to be an array")
		}
		docArr = make([]any, docReflect.Len())
		for i := 0; i < docReflect.Len(); i++ {
			docArr[i] = docReflect.Index(i).Interface()
		}
	}

	checkArr, ok := opVal.([]any)
	if !ok {
		// Fallback to reflection
		checkReflect := reflect.ValueOf(opVal)
		if checkReflect.Kind() != reflect.Slice {
			return false, fmt.Errorf("$arrContains expects an array of values to check")
		}
		checkArr = make([]any, checkReflect.Len())
		for i := 0; i < checkReflect.Len(); i++ {
			checkArr[i] = checkReflect.Index(i).Interface()
		}
	}

	// Build hash set from document array for O(1) lookups
	docSet := make(map[any]struct{}, len(docArr))
	for _, v := range docArr {
		if isHashable(v) {
			docSet[normalizeForHash(v)] = struct{}{}
		}
	}

	// Check if any item from checkArr exists in the set
	for _, v := range checkArr {
		if isHashable(v) {
			if _, found := docSet[normalizeForHash(v)]; found {
				return true, nil
			}
		} else {
			// Fallback to linear scan for non-hashable
			for _, dv := range docArr {
				if objectsEqual(dv, v) {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

// isHashable returns true if the value can be used as a map key
func isHashable(v any) bool {
	switch v.(type) {
	case string, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool:
		return true
	default:
		return false
	}
}

// normalizeForHash converts numeric types to float64 for consistent hashing
func normalizeForHash(v any) any {
	if f, ok := toFloat64(v); ok {
		return f
	}
	return v
}

func objectsEqual(a, b any) bool {
	// Fast path: direct equality
	if a == b {
		return true
	}

	// Number normalization - handle common numeric types
	f1, ok1 := toFloat64(a)
	f2, ok2 := toFloat64(b)
	if ok1 && ok2 {
		return f1 == f2
	}

	// For non-numeric types, check type-specific equality
	switch av := a.(type) {
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
		return false
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
		return false
	case nil:
		return b == nil
	}

	// Deep equality for slices/maps - only as last resort
	return reflect.DeepEqual(a, b)
}

func compareNumbers(val, target any) (int, error) {
	v1, ok1 := toFloat64(val)
	v2, ok2 := toFloat64(target)

	// Try to convert string values to numbers
	if !ok1 {
		if str, isString := val.(string); isString {
			if f, err := strconv.ParseFloat(str, 64); err == nil {
				v1 = f
				ok1 = true
			}
		}
	}

	if !ok2 {
		if str, isString := target.(string); isString {
			if f, err := strconv.ParseFloat(str, 64); err == nil {
				v2 = f
				ok2 = true
			}
		}
	}

	if !ok1 || !ok2 {
		return 0, fmt.Errorf("comparison requires numbers")
	}

	if v1 < v2 {
		return -1, nil
	} else if v1 > v2 {
		return 1, nil
	}
	return 0, nil
}

// toFloat64 converts numeric types to float64
// Ordered by most common types first for performance
func toFloat64(v any) (float64, bool) {
	switch i := v.(type) {
	case float64:
		return i, true
	case int:
		return float64(i), true
	case int64:
		return float64(i), true
	case float32:
		return float64(i), true
	case int32:
		return float64(i), true
	case int8:
		return float64(i), true
	case int16:
		return float64(i), true
	case uint:
		return float64(i), true
	case uint64:
		return float64(i), true
	case uint32:
		return float64(i), true
	case uint16:
		return float64(i), true
	case uint8:
		return float64(i), true
	default:
		return 0, false
	}
}
