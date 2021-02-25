package db

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/caiguanhao/furk/logger"
)

type (
	Model struct {
		connection         DB
		logger             logger.Logger
		tableName          string
		modelFields        []field
		permittedFieldsIdx []int
		jsonbColumns       []string
	}

	ModelWithTableName interface {
		TableName() string
	}

	field struct {
		Name       string // struct field name
		ColumnName string // column name (or jsonb key name) in database
		JsonName   string // key name in json input and output
		Jsonb      string // jsonb column name in database
		DataType   string // data type in database
		Exported   bool
	}

	RawChanges map[string]interface{}
	Changes    map[field]interface{}
)

// initialize a model from a struct
func NewModel(object interface{}) (m *Model) {
	m = &Model{
		tableName: ToTableName(object),
	}
	m.parseStruct(object)
	return
}

// initialize a model from a struct without parsing
func NewModelSlim(object interface{}) (m *Model) {
	m = &Model{
		tableName: ToTableName(object),
	}
	return
}

// get table name of a model (see ToTableName())
func (m Model) String() string {
	f := m.PermittedFields()
	return `model (table: "` + m.tableName + `") has ` +
		strconv.Itoa(len(m.modelFields)) + " modelFields, " +
		strconv.Itoa(len(f)) + " permittedFields"
}

// get table name of a model (see ToTableName())
func (m Model) TableName() string {
	return m.tableName
}

// get permitted struct field names
func (m Model) PermittedFields() (out []string) {
	for _, i := range m.permittedFieldsIdx {
		field := m.modelFields[i]
		out = append(out, field.Name)
	}
	return
}

// generate CREATE TABLE statement
func (m Model) Schema() string {
	sql := []string{}
	jsonbDataType := map[string]string{}
	for _, f := range m.modelFields {
		if f.Jsonb != "" {
			if _, ok := jsonbDataType[f.Jsonb]; !ok && f.DataType != "" {
				jsonbDataType[f.Jsonb] = f.DataType
			}
			continue
		}
		sql = append(sql, "\t"+f.ColumnName+" "+f.DataType)
	}
	for _, jsonbField := range m.jsonbColumns {
		dataType := jsonbDataType[jsonbField]
		if dataType == "" {
			dataType = "jsonb"
		}
		sql = append(sql, "\t"+jsonbField+" "+dataType)
	}
	return "CREATE TABLE " + m.tableName + " (\n" + strings.Join(sql, ",\n") + "\n);\n"
}

// generate DROP TABLE statement
func (m Model) DropSchema() string {
	return "DROP TABLE IF EXISTS " + m.tableName + ";\n"
}

// set a database connection
func (m *Model) SetConnection(db DB) *Model {
	m.connection = db
	return m
}

// set the logger
func (m *Model) SetLogger(logger logger.Logger) *Model {
	m.logger = logger
	return m
}

// permits field names of a struct for Filter()
func (m *Model) Permit(fieldNames ...string) *Model {
	m.permittedFieldsIdx = []int{}
	for i, field := range m.modelFields {
		for _, fieldName := range fieldNames {
			if fieldName != field.Name {
				continue
			}
			m.permittedFieldsIdx = append(m.permittedFieldsIdx, i)
			break
		}
	}
	return m
}

// field name exceptions of a struct for Filter()
func (m *Model) PermitAllExcept(fieldNames ...string) *Model {
	m.permittedFieldsIdx = []int{}
	for i, field := range m.modelFields {
		found := false
		for _, fieldName := range fieldNames {
			if fieldName == field.Name {
				found = true
				break
			}
		}
		if !found {
			m.permittedFieldsIdx = append(m.permittedFieldsIdx, i)
		}
	}
	return m
}

// convert RawChanges to Changes, only field names set by last Permit() are permitted
func (m Model) Filter(in RawChanges) (out Changes) {
	out = Changes{}
	for _, i := range m.permittedFieldsIdx {
		field := m.modelFields[i]
		if _, ok := in[field.JsonName]; !ok {
			continue
		}
		out[field] = in[field.JsonName]
	}
	return
}

// convert RawChanges to Changes
func (m Model) Changes(in RawChanges) (out Changes) {
	out = Changes{}
	for _, field := range m.modelFields {
		if _, ok := in[field.JsonName]; !ok {
			continue
		}
		out[field] = in[field.JsonName]
	}
	return
}

// create a SELECT statement
func (m Model) Find(values ...interface{}) sqlWithValues {
	fields := []string{}
	for _, field := range m.modelFields {
		if field.Jsonb != "" {
			continue
		}
		fields = append(fields, field.ColumnName)
	}
	for _, jsonbField := range m.jsonbColumns {
		fields = append(fields, jsonbField)
	}
	return m.Select(strings.Join(fields, ", "), values...)
}

// create a SELECT statement with custom fields
func (m Model) Select(fields string, values ...interface{}) sqlWithValues {
	var where string
	if len(values) > 0 {
		if w, ok := values[0].(string); ok {
			where = w
			values = values[1:]
		}
	}
	sql := "SELECT " + fields + " FROM " + m.tableName + " " + where
	return m.NewSQLWithValues(sql, values...)
}

// a helper to create and execute SELECT COUNT(*) statement
func (m Model) Count(values ...interface{}) (count int, err error) {
	err = m.Select("COUNT(*)", values...).QueryRow(&count)
	return
}

// convert Changes to an INSERT INTO statement
func (m Model) Insert(lotsOfChanges ...Changes) func(...string) sqlWithValues {
	return func(args ...string) sqlWithValues {
		var suffix string
		if len(args) > 0 {
			suffix = args[0]
		}
		fields := []string{}
		fieldsIndex := map[string]int{}
		numbers := []string{}
		values := []interface{}{}
		jsonbFields := map[string]Changes{}
		i := 1
		for _, changes := range lotsOfChanges {
			for field, value := range changes {
				if field.Jsonb != "" {
					if _, ok := jsonbFields[field.Jsonb]; !ok {
						jsonbFields[field.Jsonb] = Changes{}
					}
					jsonbFields[field.Jsonb][field] = value
					continue
				}
				if idx, ok := fieldsIndex[field.Name]; ok { // prevent duplication
					values[idx] = value
					continue
				}
				fields = append(fields, field.ColumnName)
				fieldsIndex[field.Name] = i - 1
				numbers = append(numbers, fmt.Sprintf("$%d", i))
				values = append(values, value)
				i += 1
			}
		}
		for jsonbField, changes := range jsonbFields {
			fields = append(fields, jsonbField)
			numbers = append(numbers, fmt.Sprintf("$%d", i))
			out := map[string]interface{}{}
			for field, value := range changes {
				out[field.ColumnName] = value
			}
			j, _ := json.Marshal(out)
			values = append(values, string(j))
			i += 1
		}
		sql := "INSERT INTO " + m.tableName + " (" + strings.Join(fields, ", ") + ") VALUES (" + strings.Join(numbers, ", ") + ") " + suffix
		return m.NewSQLWithValues(sql, values...)
	}
}

// convert Changes to an UPDATE statement
func (m Model) Update(lotsOfChanges ...Changes) func(...interface{}) sqlWithValues {
	return func(args ...interface{}) sqlWithValues {
		var where string
		if len(args) > 0 {
			if w, ok := args[0].(string); ok {
				where = w
				args = args[1:]
			}
		}
		fields := []string{}
		fieldsIndex := map[string]int{}
		values := []interface{}{}
		values = append(values, args...)
		jsonbFields := map[string]Changes{}
		i := len(args) + 1
		for _, changes := range lotsOfChanges {
			for field, value := range changes {
				if field.Jsonb != "" {
					if _, ok := jsonbFields[field.Jsonb]; !ok {
						jsonbFields[field.Jsonb] = Changes{}
					}
					jsonbFields[field.Jsonb][field] = value
					continue
				}
				if idx, ok := fieldsIndex[field.Name]; ok { // prevent duplication
					values[idx] = value
					continue
				}
				fields = append(fields, fmt.Sprintf("%s = $%d", field.ColumnName, i))
				fieldsIndex[field.Name] = i - 1
				values = append(values, value)
				i += 1
			}
		}
		for jsonbField, changes := range jsonbFields {
			var field = fmt.Sprintf("COALESCE(%s, '{}'::jsonb)", jsonbField)
			for f, value := range changes {
				field = fmt.Sprintf("jsonb_set(%s, '{%s}', $%d)", field, f.ColumnName, i)
				j, _ := json.Marshal(value)
				values = append(values, string(j))
				i += 1
			}
			fields = append(fields, jsonbField+" = "+field)
		}
		sql := "UPDATE " + m.tableName + " SET " + strings.Join(fields, ", ") + " " + where
		return m.NewSQLWithValues(sql, values...)
	}
}

// create a DELETE FROM statement
func (m Model) Delete(values ...interface{}) sqlWithValues {
	var where string
	if len(values) > 0 {
		if w, ok := values[0].(string); ok {
			where = w
			values = values[1:]
		}
	}
	sql := "DELETE FROM " + m.tableName + " " + where
	return m.NewSQLWithValues(sql, values...)
}

// a helper to add CreatedAt changes
func (m Model) CreatedAt() Changes {
	return m.Changes(RawChanges{
		"CreatedAt": time.Now().UTC(),
	})
}

// a helper to add UpdatedAt changes
func (m Model) UpdatedAt() Changes {
	return m.Changes(RawChanges{
		"UpdatedAt": time.Now().UTC(),
	})
}

// parseStruct collects column names, json names and jsonb names
func (m *Model) parseStruct(obj interface{}) {
	var rt reflect.Type
	if o, ok := obj.(reflect.Type); ok {
		rt = o
	} else {
		rt = reflect.TypeOf(obj)
	}
	fields := []field{}
	jsonbColumns := []string{}
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Anonymous {
			continue
		}

		columnName := f.Tag.Get("column")
		if columnName == "-" {
			continue
		}
		if idx := strings.Index(columnName, ","); idx != -1 {
			columnName = columnName[:idx]
		}
		if columnName == "" {
			if f.PkgPath != "" {
				continue // ignore unexported field if no column specified
			}
			columnName = ToColumnName(f.Name)
		}

		jsonName := f.Tag.Get("json")
		if jsonName == "-" {
			jsonName = ""
		} else {
			if idx := strings.Index(jsonName, ","); idx != -1 {
				jsonName = jsonName[:idx]
			}
			if jsonName == "" {
				jsonName = f.Name
			}
		}

		jsonb := f.Tag.Get("jsonb")
		if idx := strings.Index(jsonb, ","); idx != -1 {
			jsonb = jsonb[:idx]
		}
		jsonb = ToColumnName(jsonb)
		if jsonb != "" {
			exists := false
			for _, column := range jsonbColumns {
				if column == jsonb {
					exists = true
					break
				}
			}
			if !exists {
				jsonbColumns = append(jsonbColumns, jsonb)
			}
		}

		dataType := f.Tag.Get("dataType")
		if dataType == "" {
			tp := f.Type.String()
			var null bool
			if strings.HasPrefix(tp, "*") {
				tp = strings.TrimPrefix(tp, "*")
				null = true
			}
			if columnName == "id" && strings.Contains(tp, "int") {
				dataType = "SERIAL PRIMARY KEY"
			} else if jsonb == "" {
				switch tp {
				case "int8", "int16", "int32", "uint8", "uint16", "uint32":
					dataType = "integer DEFAULT 0"
				case "int64", "uint64", "int", "uint":
					dataType = "bigint DEFAULT 0"
				case "time.Time":
					dataType = "timestamptz DEFAULT NOW()"
				case "float32", "float64":
					dataType = "numeric(10,2) DEFAULT 0.0"
				case "decimal.Decimal":
					dataType = "numeric(10, 2) DEFAULT 0.0"
				case "bool":
					dataType = "boolean DEFAULT false"
				default:
					dataType = "text DEFAULT ''::text"
				}
				if !null {
					dataType += " NOT NULL"
				}
			}
		}

		fields = append(fields, field{
			Name:       f.Name,
			Exported:   f.PkgPath == "",
			ColumnName: columnName,
			JsonName:   jsonName,
			Jsonb:      jsonb,
			DataType:   dataType,
		})
	}
	m.modelFields = fields
	m.jsonbColumns = jsonbColumns
}
