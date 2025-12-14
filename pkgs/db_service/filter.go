package dbservice

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// matchesFilter checks if a document's metadata matches the given filter conditions.
func matchesFilter(docMetadata map[string]interface{}, filter map[string]interface{}) (bool, error) {
	if len(filter) == 0 {
		return true, nil
	}

	for key, condition := range filter {
		switch key {
		case "$and":
			// condition must be a slice of filters
			filters, ok := condition.([]interface{})
			if !ok {
				// Try map slice if interface slice fails
				if f, ok := condition.([]map[string]interface{}); ok {
					for _, subFilter := range f {
						match, err := matchesFilter(docMetadata, subFilter)
						if err != nil || !match {
							return false, err
						}
					}
					return true, nil
				}
				return false, fmt.Errorf("$and must be an array of objects")
			}
			for _, item := range filters {
				subFilter, ok := item.(map[string]interface{})
				if !ok {
					// Fallback for when JSON unmarshals into map[string]interface{} but inside []interface{}
					return false, fmt.Errorf("item in $and array must be an object")
				}
				match, err := matchesFilter(docMetadata, subFilter)
				if err != nil || !match {
					return false, err
				}
			}
		case "$or":
			// condition must be a slice of filters
			filters, ok := condition.([]interface{})
			if !ok {
				if f, ok := condition.([]map[string]interface{}); ok {
					for _, subFilter := range f {
						match, err := matchesFilter(docMetadata, subFilter)
						if err == nil && match {
							return true, nil
						}
					}
					return false, nil
				}
				return false, fmt.Errorf("$or must be an array of objects")
			}
			matchedAny := false
			for _, item := range filters {
				subFilter, ok := item.(map[string]interface{})
				if !ok {
					return false, fmt.Errorf("item in $or array must be an object")
				}
				match, err := matchesFilter(docMetadata, subFilter)
				if err == nil && match {
					matchedAny = true
					break
				}
			}
			if !matchedAny {
				return false, nil
			}
		default:
			// Direct field filter
			// Key is the field name
			// condition is the value or operator map
			// e.g {where: {"age": 12}} or { where: {"age": {"$gt": "15"} }
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

func checkCondition(docVal interface{}, condition interface{}) (bool, error) {
	// If condition is a map, it might contain operators like $eq, $gt, etc.
	condMap, isMap := condition.(map[string]any)
	if isMap {
		// Check for operators
		isOperatorMap := false
		for k := range condMap {
			if strings.HasPrefix(k, "$") {
				isOperatorMap = true
				break
			}
		}

		if isOperatorMap {
			for op, opVal := range condMap {
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
					// opVal should be a slice - use reflection to iterate directly
					val := reflect.ValueOf(opVal)
					if val.Kind() != reflect.Slice {
						return false, fmt.Errorf("$in expects an array")
					}

					match := false
					for i := 0; i < val.Len(); i++ {
						if objectsEqual(docVal, val.Index(i).Interface()) {
							match = true
							break
						}
					}
					if !match {
						return false, nil
					}
				case "$arrContains":
					// docVal should be an array, opVal should be a slice of items to check
					docVal := reflect.ValueOf(docVal)
					if docVal.Kind() != reflect.Slice {
						return false, fmt.Errorf("$arrContains requires the field to be an array")
					}

					checkVal := reflect.ValueOf(opVal)
					if checkVal.Kind() != reflect.Slice {
						return false, fmt.Errorf("$arrContains expects an array of values to check")
					}

					// Create a hash set from docVal for O(1) lookups
					docSet := make(map[interface{}]bool, docVal.Len())
					for i := 0; i < docVal.Len(); i++ {
						docSet[docVal.Index(i).Interface()] = true
					}

					// Check if any item from checkVal exists in the set
					match := false
					for i := 0; i < checkVal.Len(); i++ {
						if docSet[checkVal.Index(i).Interface()] {
							match = true
							break
						}
					}

					if !match {
						return false, nil
					}
				case "$nin":
					// opVal should be a slice - use reflection to iterate directly
					val := reflect.ValueOf(opVal)
					if val.Kind() != reflect.Slice {
						return false, fmt.Errorf("$nin expects an array")
					}

					for i := 0; i < val.Len(); i++ {
						if objectsEqual(docVal, val.Index(i).Interface()) {
							return false, nil
						}
					}
				default:
					return false, fmt.Errorf("unknown operator %s", op)
				}
			}
			return true, nil
		}
	}

	// Direct equality check (implicit $eq)
	if !objectsEqual(docVal, condition) {
		return false, nil
	}
	return true, nil
}

func objectsEqual(a, b interface{}) bool {
	// Simple equality first
	if a == b {
		return true
	}

	// Number normalization
	f1, ok1 := toFloat64(a)
	f2, ok2 := toFloat64(b)
	if ok1 && ok2 {
		return f1 == f2
	}

	// Deep equality for slices/maps if needed, but for this schema it's mostly primitives
	return reflect.DeepEqual(a, b)
}

func compareNumbers(val, target interface{}) (int, error) {
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

func toFloat64(v interface{}) (float64, bool) {
	switch i := v.(type) {
	case float64:
		return i, true
	case float32:
		return float64(i), true
	case int:
		return float64(i), true
	case int64:
		return float64(i), true
	case int32:
		return float64(i), true
	default:
		return 0, false
	}
}
