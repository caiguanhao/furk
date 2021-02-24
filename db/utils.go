package db

import (
	"reflect"
	"strings"
	"unicode"
)

var (
	Columnizer func(string) string = DefaultColumnizer
	Pluralizer func(string) string = DefaultPluralizer
)

const (
	tableNameField = "__TABLE_NAME__"
)

// get table name from struct
// struct.TableName() > struct.__TABLE_NAME__ > struct.Name()
func ToTableName(object interface{}) (name string) {
	if o, ok := object.(ModelWithTableName); ok {
		name = o.TableName()
		return
	}
	rt := reflect.TypeOf(object)
	if f, ok := rt.FieldByName(tableNameField); ok {
		name = string(f.Tag)
		if name != "" {
			return
		}
	}
	name = ToColumnName(rt.Name())
	if name == "" { // anonymous struct has no name
		return "error_no_table_name"
	}
	name = Pluralizer(name)
	return
}

// convert to name used in database
func ToColumnName(in string) string {
	return Columnizer(strings.TrimSpace(in))
}

func DefaultColumnizer(in string) string {
	return camelCaseToUnderscore(in)
}

func DefaultPluralizer(in string) string {
	if strings.HasSuffix(in, "y") {
		return in[:len(in)-1] + "ies"
	}
	if strings.HasSuffix(in, "s") || strings.HasSuffix(in, "o") {
		return in + "es"
	}
	return in + "s"
}

func camelCaseToUnderscore(str string) string { // from govalidator
	var output []rune
	var segment []rune
	for _, r := range str {
		// not treat number as separate segment
		if !unicode.IsLower(r) && string(r) != "_" && !unicode.IsNumber(r) {
			output = addSegment(output, segment)
			segment = nil
		}
		segment = append(segment, unicode.ToLower(r))
	}
	output = addSegment(output, segment)
	return string(output)
}

func addSegment(inrune, segment []rune) []rune { // from govalidator
	if len(segment) == 0 {
		return inrune
	}
	if len(inrune) != 0 {
		inrune = append(inrune, '_')
	}
	inrune = append(inrune, segment...)
	return inrune
}
