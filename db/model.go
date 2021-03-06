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
	// Model is a database table and it is created from struct. Table name
	// is inferred from the name of thea struct, the tag of __TABLE_NAME__
	// field or its TableName() receiver. Column names are inferred from
	// struct field names or theirs "column" tags. Both table names and
	// field names are in snake_case by default.
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
		Exported   bool   // false if field name is lower case (unexported)
	}

	RawChanges map[string]interface{}
	Changes    map[Field]interface{}
)

var (
	ErrMustBePointer = errors.New("must be pointer")
)

// Initialize a Model from a struct. For available options, see SetOptions().
func NewModel(object interface{}, options ...interface{}) (m *Model) {
	m = NewModelSlim(object, options...)
	m.modelFields, m.jsonbColumns = m.parseStruct(object)
	return
}

// Initialize a Model from a struct without parsing fields of the struct.
// Useful if you are calling functions that don't need fields, for example:
//  db.NewModelSlim(models.User{}, conn).MustCount()
// For available options, see SetOptions().
func NewModelSlim(object interface{}, options ...interface{}) (m *Model) {
	m = &Model{
		tableName:  ToTableName(object),
		structType: reflect.TypeOf(object),
	}
	m.SetOptions(options...)
	return
}

// Initialize a Model by defining table name only. Useful if you are calling
// functions that don't need fields, for example:
//  db.NewModelTable("users", conn).MustCount()
// For available options, see SetOptions().
func NewModelTable(tableName string, options ...interface{}) (m *Model) {
	m = &Model{
		tableName:  tableName,
		structType: nil,
	}
	m.SetOptions(options...)
	return
}

func (m Model) String() string {
	return `model (table: "` + m.tableName + `") has ` +
		strconv.Itoa(len(m.modelFields)) + " modelFields"
}

// Table name of the Model (see ToTableName()).
func (m Model) TableName() string {
	return m.tableName
}

// Get field by struct field name, nil will be returned if no such field.
func (m Model) FieldByName(name string) *Field {
	for _, f := range m.modelFields {
		if f.Name == name {
			return &f
		}
	}
	return nil
}

// Generate CREATE TABLE SQL statement from a Model.
//  | Go Type                                        | PostgreSQL Data Type |
//  |------------------------------------------------|----------------------|
//  | int8 / int16 / int32 / uint8 / uint16 / uint32 | integer              |
//  | int64 / uint64 / int / uint                    | bigint               |
//  | time.Time                                      | timestamptz          |
//  | float32 / float64 / decimal.Decimal            | numeric              |
//  | bool                                           | boolean              |
//  | other                                          | text                 |
// You can use "dataType" tag to customize the data type. "NOT NULL" is added
// if the struct field is not a pointer. You can also set SQL statements before
// or after this statement by defining "BeforeCreateSchema() string" (for
// example the CREATE EXTENSION statement) or "AfterCreateSchema() string" (for
// example the CREATE INDEX statement) function for the struct.
//  db.NewModel(struct {
//  	__TABLE_NAME__ string `users`
//
//  	Id        int
//  	Name      string
//  	Age       *int
//  	CreatedAt time.Time
//  	DeletedAt *time.Time `dataType:"timestamptz"`
//  	FullName  string     `jsonb:"meta"`
//  	NickName  string     `jsonb:"meta"`
//  }{}).Schema()
//  // CREATE TABLE users (
//  //         id SERIAL PRIMARY KEY,
//  //         name text DEFAULT ''::text NOT NULL,
//  //         age bigint DEFAULT 0,
//  //         created_at timestamptz DEFAULT NOW() NOT NULL,
//  //         deleted_at timestamptz,
//  //         meta jsonb DEFAULT '{}'::jsonb NOT NULL
//  // );
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
	if m.structType != nil {
		n := reflect.New(m.structType).Interface()
		if a, ok := n.(interface{ BeforeCreateSchema() string }); ok {
			out = a.BeforeCreateSchema() + "\n\n" + out
		}
		if a, ok := n.(interface{ AfterCreateSchema() string }); ok {
			out += "\n" + a.AfterCreateSchema() + "\n"
		}
	}
	return out
}

// Generate DROP TABLE ("DROP TABLE IF EXISTS <table_name>;") SQL statement from a Model.
func (m Model) DropSchema() string {
	return "DROP TABLE IF EXISTS " + m.tableName + ";\n"
}

// SetOptions sets database connection (see SetConnection()) and/or logger (see
// SetLogger()).
func (m *Model) SetOptions(options ...interface{}) *Model {
	for _, option := range options {
		switch o := option.(type) {
		case DB:
			m.SetConnection(o)
		case logger.Logger:
			m.SetLogger(o)
		}
	}
	return m
}

// Set a database connection for the Model. ErrNoConnection is returned if no
// connection is set.
func (m *Model) SetConnection(db DB) *Model {
	m.connection = db
	return m
}

// Set the logger for the Model. Use logger.StandardLogger if you want to use
// Go's built-in standard logging package. By default, no logger is used, so
// the SQL statements are not printed to the console.
func (m *Model) SetLogger(logger logger.Logger) *Model {
	m.logger = logger
	return m
}

// Permits list of field names of a Model to limit Filter() which fields should
// be allowed for mass updating. If no field names are provided ("Permit()"),
// no fields are permitted.
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

// Permits all available fields except provided of a Model to limit Filter()
// which fields should be allowed for mass updating. If no field names are
// provided ("PermitAllExcept()"), all available fields are permitted.
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

// Returns list of permitted field names.
func (m ModelWithPermittedFields) PermittedFields() (out []string) {
	for _, i := range m.permittedFieldsIdx {
		field := m.modelFields[i]
		out = append(out, field.Name)
	}
	return
}

// MustBind is like Bind but panics if bind operation fails.
func (m ModelWithPermittedFields) MustBind(ctx interface{ Bind(interface{}) error }, target interface{}) Changes {
	c, err := m.Bind(ctx, target)
	if err != nil {
		panic(err)
	}
	return c
}

// Bind data of permitted fields to target structure using echo.Context#Bind
// function. The "target" must be a pointer to struct.
//  // request with ?name=x&age=10
//  func list(c echo.Context) error {
//  	obj := struct {
//  		Name string `query:"name"`
//  		Age  int    `query:"age"`
//  	}{}
//  	m := db.NewModel(obj)
//  	fmt.Println(m.Permit("Name").Bind(c, &obj))
//  	fmt.Println(obj) // "Name" is "x" and "Age" is 0 (default), because only "Name" is permitted to change
//  	// ...
//  }
func (m ModelWithPermittedFields) Bind(ctx interface{ Bind(interface{}) error }, target interface{}) (Changes, error) {
	rt := reflect.TypeOf(target)
	if rt.Kind() != reflect.Ptr {
		return nil, ErrMustBePointer
	}
	rv := reflect.ValueOf(target).Elem()
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

// Filter keeps data of permitted fields set by Permit() from multiple inputs.
// Inputs can be RawChanges (map[string]interface{}) or JSON-encoded data
// (string, []byte or io.Reader), their keys must be fields' JSON names. Input
// can also be a struct. The "Changes" outputs can be arguments for Insert() or
// Update().
//  m := db.NewModel(struct {
//  	Age *int `json:"age"`
//  }{})
//  m.Permit("Age").Filter(
//  	db.RawChanges{
//  		"age": 10,
//  	},
//  	map[string]interface{}{
//  		"age": 20,
//  	},
//  	`{"age": 30}`,
//  	[]byte(`{"age": 40}`),
//  	strings.NewReader(`{"age": 50}`),
//  	struct{ Age int }{60},
//  ) // Age is 60
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
		if m.structType == nil {
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

// Convert RawChanges to Changes.
//  m := db.NewModel(struct {
//  	Age *int `json:"age"`
//  }{})
//  m.Changes(map[string]interface{}{
//  	"age": 99,
//  })
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

// Create a SELECT query statement with all fields of a Model. You can provide
// conditions (like WHERE, ORDER BY, LIMIT) to the statement as the first
// argument. The rest arguments are for any placeholder parameters in the
// statement. If you want to use other data type than the type of struct passed
// in NewModel(), see Select().
//  // put results into a slice
//  var users []models.User
//  db.NewModel(models.User{}, conn).Find().MustQuery(&users)
//
//  // put results into a struct
//  var user models.User
//  db.NewModel(models.User{}, conn).Find("WHERE id = $1", 1).MustQuery(&user)
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

// Select is like Find but you can choose what columns to retrieve.
//  // put results into a slice
//  var names []string
//  db.NewModelTable("users", conn).Select("name", "ORDER BY id ASC").MustQuery(&names)
//
//  // put results into a map
//  var id2name map[int]string
//  db.NewModelTable("users", conn).Select("id, name", "ORDER BY id ASC").MustQuery(&id2name)
//
//  // put results into a slice of custom struct
//  var users []struct {
//  	name string
//  	id   int
//  }
//  db.NewModelTable("users", conn).Select("name, id", "ORDER BY id ASC").MustQuery(&users)
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

// MustCount is like Count but panics if count operation fails.
func (m Model) MustCount(values ...interface{}) int {
	count, err := m.Count(values...)
	if err != nil {
		panic(err)
	}
	return count
}

// Create and execute a SELECT COUNT(*) statement, return number of rows. You
// can provide conditions (like WHERE, ORDER BY, LIMIT) to the statement as the
// first argument. The rest arguments are for any placeholder parameters in
// the statement.
func (m Model) Count(values ...interface{}) (count int, err error) {
	err = m.Select("COUNT(*)", values...).QueryRow(&count)
	return
}

// MustExists is like Exists but panics if existence check operation fails.
// Returns true if record exists, false if not exists.
func (m Model) MustExists(values ...interface{}) bool {
	exists, err := m.Exists(values...)
	if err != nil {
		panic(err)
	}
	return exists
}

// Create and execute a SELECT 1 AS one statement. Returns true if record
// exists, false if not exists.
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

// MustAssign is like Assign but panics if assign operation fails.
func (m Model) MustAssign(i interface{}, lotsOfChanges ...Changes) []Changes {
	out, err := m.Assign(i, lotsOfChanges...)
	if err != nil {
		panic(err)
	}
	return out
}

// Assign changes to target object. Useful if you want to validate your struct.
//  func create(c echo.Context) error {
//  	var user models.User
//  	m := db.NewModel(user, conn)
//  	changes := m.MustAssign(
//  		&user,
//  		m.Permit("Name").Filter(c.Request().Body),
//  	)
//  	if err := c.Validate(user); err != nil {
//  		panic(err)
//  	}
//  	var id int
//  	m.Insert(changes...)("RETURNING id").MustQueryRow(&id)
//  	// ...
//  }
func (m Model) Assign(target interface{}, lotsOfChanges ...Changes) (out []Changes, err error) {
	rt := reflect.TypeOf(target)
	if rt.Kind() != reflect.Ptr {
		err = ErrMustBePointer
		return
	}
	rv := reflect.ValueOf(target).Elem()
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

// Insert builds an INSERT INTO statement with fields and values in the
// changes, returns a function with optional string argument which you can add
// extra clause (like ON CONFLICT or RETURNING) to the statement.
//  var id int
//  m.Insert(changes...)("RETURNING id").MustQueryRow(&id)
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

// Update builds an UPDATE statement with fields and values in the changes,
// returns a function with optional conditions (like WHERE) to the statement as
// the first argument. The rest arguments are for any placeholder parameters in
// the statement.
//  var rowsAffected int
//  m.Update(changes...)("WHERE user_id = $1", 1).MustExecute(&rowsAffected)
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

// Delete builds a DELETE statement. You can add extra clause (like WHERE,
// RETURNING) to the statement as the first argument. The rest arguments are
// for any placeholder parameters in the statement.
//  var ids []int
//  db.NewModelTable("reports", conn).Delete("RETURNING id").MustQuery(&ids)
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

// Helper to add CreatedAt of current time changes.
func (m Model) CreatedAt() Changes {
	return m.Changes(RawChanges{
		"CreatedAt": time.Now().UTC(),
	})
}

// Helper to add UpdatedAt of current time changes.
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

func (c Changes) MarshalJSON() ([]byte, error) {
	data := map[string]interface{}{}
	for field, value := range c {
		data[field.JsonName] = value
	}
	return json.Marshal(data)
}

func (c Changes) String() string {
	j, _ := json.MarshalIndent(c, "", "  ")
	return string(j)
}
