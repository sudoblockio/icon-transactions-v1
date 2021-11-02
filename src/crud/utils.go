package crud

import (
	"reflect"
	"strconv"
)

func extractFilledFieldsFromModel(modelValueOf reflect.Value, modelTypeOf reflect.Type) map[string]interface{} {

	fields := map[string]interface{}{}

	for i := 0; i < modelValueOf.NumField(); i++ {
		modelField := modelValueOf.Field(i)
		modelType := modelTypeOf.Field(i)

		modelTypeJSONTag := modelType.Tag.Get("json")
		if modelTypeJSONTag != "" {
			// exported field

			// Check if field if filled
			modelFieldKind := modelField.Kind()
			isFieldFilled := true
			switch modelFieldKind {
			case reflect.String:
				v := modelField.Interface().(string)
				if v == "" {
					isFieldFilled = false
				}
			case reflect.Int:
				v := modelField.Interface().(int)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Int8:
				v := modelField.Interface().(int8)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Int16:
				v := modelField.Interface().(int16)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Int32:
				v := modelField.Interface().(int32)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Int64:
				v := modelField.Interface().(int64)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Uint:
				v := modelField.Interface().(uint)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Uint8:
				v := modelField.Interface().(uint8)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Uint16:
				v := modelField.Interface().(uint16)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Uint32:
				v := modelField.Interface().(uint32)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Uint64:
				v := modelField.Interface().(uint64)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Float32:
				v := modelField.Interface().(float32)
				if v == 0 {
					isFieldFilled = false
				}
			case reflect.Float64:
				v := modelField.Interface().(float64)
				if v == 0 {
					isFieldFilled = false
				}
			}

			if isFieldFilled == true {
				fields[modelTypeJSONTag] = modelField.Interface()
			}
		}
	}

	return fields
}

func extractMatrializedViewWithWhereClauseRaw(
	tableName string,
	modelValueOf reflect.Value,
	modelTypeOf reflect.Type,
	whereClause string,
	orderByClause string,
	limit int,
	skip int,
) string {
	rawSQL := ""

	// Example output:
	/*
			WITH a (from_address, to_address, value, block_timestamp, hash, block_number, transaction_fee, receipt_status, type, method, value_decimal) // With Clause
			AS MATERIALIZED (																																																														// Materialized Clause
		    SELECT
		        "transactions"."from_address",
		        "transactions"."to_address",
		        "transactions"."value",
		        "transactions"."block_timestamp",
		        "transactions"."hash",
		        "transactions"."block_number",
		        "transactions"."transaction_fee",
		        "transactions"."receipt_status",
		        "transactions"."type",
		        "transactions"."method",
		        "transactions"."value_decimal"
		    FROM "transactions"
		    WHERE type = 'transaction'
		    AND (from_address = ? OR to_address = ?)
			) SELECT * FROM a ORDER BY a.block_number desc LIMIT ? OFFSET ?;																																						// Select Clause
	*/

	/////////////////
	// With Clause //
	/////////////////
	rawSQL += "WITH a ("

	for i := 0; i < modelValueOf.NumField(); i++ {
		modelType := modelTypeOf.Field(i)

		modelTypeJSONTag := modelType.Tag.Get("json")
		if modelTypeJSONTag == "" {
			continue
		}

		rawSQL += modelTypeJSONTag
		if i != (modelValueOf.NumField() - 1) {
			rawSQL += ", "
		} else {
			rawSQL += ") "
		}
	}

	/////////////////////////
	// Materialized Clause //
	/////////////////////////
	rawSQL += "AS MATERIALIZED (SELECT "

	for i := 0; i < modelValueOf.NumField(); i++ {
		modelType := modelTypeOf.Field(i)

		modelTypeJSONTag := modelType.Tag.Get("json")
		if modelTypeJSONTag == "" {
			continue
		}

		rawSQL += "\"" + tableName + "\"." + "\"" + modelTypeJSONTag + "\""
		if i != (modelValueOf.NumField() - 1) {
			rawSQL += ", "
		} else {
			rawSQL += "FROM \"" + tableName + "\" " + whereClause + ") "
		}
	}

	///////////////////
	// Select Clause //
	///////////////////
	rawSQL += "SELECT * from a " + orderByClause + " LIMIT " + strconv.Itoa(limit) + " OFFSET " + strconv.Itoa(skip) + ";"

	return rawSQL
}
