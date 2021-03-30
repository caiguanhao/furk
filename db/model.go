package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/caiguanhao/furk/logger"
)

type (
	Model struct {
		connection   DB
		logger       logger.Logger
		structType   reflect.Type
		tableName    string
		modelFields  []Field
		jsonbColumns []string
	}

	ModelWithPermittedFields struct {
		*Model
		permittedFieldsIdx []int
	}

	ModelWithTableName interface {
		TableName() string
	}

	Field struct {
		Name       string // struct field name
		ColumnName string // column name (or jsonb key name) in database
		JsonName   string // key name in json input and output
		Jsonb      string // jsonb column name in database
		DataType   string // data type in database
		Exported   bool
	}

	RawChanges map[string]interface{}
	Changes    map[Field]interface{}
)

var (
	ErrMustBePointer = errors.New("must be pointer")
)

// initialize a model from a struct
func NewModel(object interface{}) (m *Model) {
	m = NewModelSlim(object)
	m.modelFields, m.jsonbColumns = m.parseStruct(object)
	return
}

// initialize a model from a struct without parsing
func NewModelSlim(object interface{}) (m *Model) {
	m = &Model{
		tableName:  ToTableName(object),
		structType: reflect.TypeOf(object),
	}
	return
}

// get table name of a model (see ToTableName())
func (m Model) String() string {
	return `model (table: "` + m.tableName + `") has ` +
		strconv.Itoa(len(m.modelFields)) + " modelFields"
}

// get table name of a model (see ToTableName())
func (m Model) TableName() string {
	return m.tableName
}

// get field by struct name, nil will be returned if no such field
func (m Model) FieldByName(name string) *Field {
	for _, f := range m.modelFields {
		if f.Name == name {
			return &f
		}
	}
	return nil
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
			dataType = "jsonb DEFAULT '{}'::jsonb NOT NULL"
		}
		sql = append(sql, "\t"+jsonbField+" "+dataType)
	}
	out := "CREATE TABLE " + m.tableName + " (\n" + strings.Join(sql, ",\n") + "\n);\n"
	n := reflect.New(m.structType).Interface()
	if a, ok := n.(interface{ BeforeCreateSchema() string }); ok {
		out = a.BeforeCreateSchema() + "\n\n" + out
	}
	if a, ok := n.(interface{ AfterCreateSchema() string }); ok {
		out += "\n" + a.AfterCreateSchema() + "\n"
	}
	return out
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
func (m Model) Permit(fieldNames ...string) *ModelWithPermittedFields {
	idx := []int{}
	for i, field := range m.modelFields {
		for _, fieldName := range fieldNames {
			if fieldName != field.Name {
				continue
			}
			idx = append(idx, i)
			break
		}
	}
	return &ModelWithPermittedFields{&m, idx}
}

// field name exceptions of a struct for Filter()
func (m Model) PermitAllExcept(fieldNames ...string) *ModelWithPermittedFields {
	idx := []int{}
	for i, field := range m.modelFields {
		found := false
		for _, fieldName := range fieldNames {
			if fieldName == field.Name {
				found = true
				break
			}
		}
		if !found {
			idx = append(idx, i)
		}
	}
	return &ModelWithPermittedFields{&m, idx}
}

// get permitted struct field names
func (m ModelWithPermittedFields) PermittedFields() (out []string) {
	for _, i := range m.permittedFieldsIdx {
		field := m.modelFields[i]
		out = append(out, field.Name)
	}
	return
}

func (m ModelWithPermittedFields) Bind(ctx interface{ Bind(interface{}) error }, i interface{}) (Changes, error) {
	rt := reflect.TypeOf(i)
	if rt.Kind() != reflect.Ptr {
		return nil, ErrMustBePointer
	}
	rv := reflect.ValueOf(i).Elem()
	nv := reflect.New(rt.Elem())
	if err := ctx.Bind(nv.Interface()); err != nil {
		return nil, err
	}
	nv = nv.Elem()
	out := Changes{}
	for _, i := range m.permittedFieldsIdx {
		field := m.modelFields[i]
		v := nv.FieldByName(field.Name)
		rv.FieldByName(field.Name).Set(v)
		out[field] = v.Interface()
	}
	return out, nil
}

// convert RawChanges to Changes, only field names set by last Permit() are permitted
func (m ModelWithPermittedFields) Filter(inputs ...interface{}) (out Changes) {
	out = Changes{}
	for _, input := range inputs {
		switch in := input.(type) {
		case RawChanges:
			m.filterPermits(in, &out)
		case map[string]interface{}:
			m.filterPermits(in, &out)
		case string:
			var c RawChanges
			if json.Unmarshal([]byte(in), &c) == nil {
				m.filterPermits(c, &out)
			}
		case []byte:
			var c RawChanges
			if json.Unmarshal(in, &c) == nil {
				m.filterPermits(c, &out)
			}
		case io.Reader:
			var c RawChanges
			if json.NewDecoder(in).Decode(&c) == nil {
				m.filterPermits(c, &out)
			}
		default:
			rt := reflect.TypeOf(in)
			if rt.Kind() == reflect.Struct {
				rv := reflect.ValueOf(in)
				fields := map[string]Field{}
				for _, i := range m.permittedFieldsIdx {
					field := m.modelFields[i]
					fields[field.Name] = field
				}
				for i := 0; i < rt.NumField(); i++ {
					if field, ok := fields[rt.Field(i).Name]; ok {
						out[field] = rv.Field(i).Interface()
					}
				}
			}

		}
	}
	return
}

func (m ModelWithPermittedFields) filterPermits(in RawChanges, out *Changes) {
	for _, i := range m.permittedFieldsIdx {
		field := m.modelFields[i]
		if _, ok := in[field.JsonName]; !ok {
			continue
		}
		f, ok := m.structType.FieldByName(field.Name)
		if !ok {
			continue
		}
		v, err := json.Marshal(in[field.JsonName])
		if err != nil {
			continue
		}
		x := reflect.New(f.Type)
		if err := json.Unmarshal(v, x.Interface()); err != nil {
			continue
		}
		(*out)[field] = x.Elem().Interface()
	}
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
func (m Model) Find(values ...interface{}) SQLWithValues {
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
func (m Model) Select(fields string, values ...interface{}) SQLWithValues {
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

func (m Model) MustCount(values ...interface{}) int {
	count, err := m.Count(values...)
	if err != nil {
		panic(err)
	}
	return count
}

// a helper to create and execute SELECT COUNT(*) statement
func (m Model) Count(values ...interface{}) (count int, err error) {
	err = m.Select("COUNT(*)", values...).QueryRow(&count)
	return
}

// Just like Exists(), panics if connection error; returns true if record exists, false if not
func (m Model) MustExists(values ...interface{}) bool {
	exists, err := m.Exists(values...)
	if err != nil {
		panic(err)
	}
	return exists
}

// Helper function to create and execute SELECT 1 AS one statement
func (m Model) Exists(values ...interface{}) (exists bool, err error) {
	var ret int
	err = m.Select("1 AS one", values...).QueryRow(&ret)
	if err == m.connection.ErrNoRows() {
		err = nil
		return
	}
	exists = ret == 1
	return
}

func (m Model) MustAssign(i interface{}, lotsOfChanges ...Changes) []Changes {
	out, err := m.Assign(i, lotsOfChanges...)
	if err != nil {
		panic(err)
	}
	return out
}

func (m Model) Assign(i interface{}, lotsOfChanges ...Changes) (out []Changes, err error) {
	rt := reflect.TypeOf(i)
	if rt.Kind() != reflect.Ptr {
		err = ErrMustBePointer
		return
	}
	rv := reflect.ValueOf(i).Elem()
	for _, changes := range lotsOfChanges {
		for field, value := range changes {
			f := rv.FieldByName(field.Name)
			var pointer interface{}
			if field.Exported {
				pointer = f.Addr().Interface()
			} else {
				pointer = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Interface()
			}
			b, _ := json.Marshal(value)
			json.Unmarshal(b, pointer)
		}
	}
	out = lotsOfChanges
	return
}

// convert Changes to an INSERT INTO statement
func (m Model) Insert(lotsOfChanges ...Changes) func(...string) SQLWithValues {
	return func(args ...string) SQLWithValues {
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
func (m Model) Update(lotsOfChanges ...Changes) func(...interface{}) SQLWithValues {
	return func(args ...interface{}) SQLWithValues {
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
func (m Model) Delete(values ...interface{}) SQLWithValues {
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
func (m *Model) parseStruct(obj interface{}) (fields []Field, jsonbColumns []string) {
	var rt reflect.Type
	if o, ok := obj.(reflect.Type); ok {
		rt = o
	} else {
		rt = reflect.TypeOf(obj)
	}
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.Anonymous {
			f, j := m.parseStruct(f.Type)
			fields = append(fields, f...)
			jsonbColumns = append(jsonbColumns, j...)
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

		fields = append(fields, Field{
			Name:       f.Name,
			Exported:   f.PkgPath == "",
			ColumnName: columnName,
			JsonName:   jsonName,
			Jsonb:      jsonb,
			DataType:   dataType,
		})
	}
	return
}
