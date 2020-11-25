package export

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"

	_ "github.com/jinzhu/copier"
	//"github.com/ulule/deepcopier"
)

var colTypeRowReceiverMap = map[string]func() RowReceiverStringer{}

var nullValue = "NULL"
var quotationMark = []byte{'\''}
var doubleQuotationMark = []byte{'"'}

func init() {
	for _, s := range dataTypeString {
		colTypeRowReceiverMap[s] = SQLTypeStringMaker
	}
	for _, s := range dataTypeNum {
		colTypeRowReceiverMap[s] = SQLTypeNumberMaker
	}
	for _, s := range dataTypeBin {
		colTypeRowReceiverMap[s] = SQLTypeBytesMaker
	}
}

var dataTypeString = []string{
	"CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "CHARACTER", "VARCHARACTER",
	"TIMESTAMP", "DATETIME", "DATE", "TIME", "YEAR", "SQL_TSI_YEAR",
	"TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",
	"ENUM", "SET", "JSON",
}

var dataTypeNum = []string{
	"INTEGER", "BIGINT", "TINYINT", "SMALLINT", "MEDIUMINT",
	"INT", "INT1", "INT2", "INT3", "INT8",
	"FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION",
	"DECIMAL", "NUMERIC", "FIXED",
	"BOOL", "BOOLEAN",
}

var dataTypeBin = []string{
	"BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "LONG",
	"BINARY", "VARBINARY",
	"BIT",
}

func getEscapeQuotation(escapeBackSlash bool, escapeQuotation []byte) []byte {
	if escapeBackSlash {
		return nil
	}
	return escapeQuotation
}

func escape(s []byte, bf *bytes.Buffer, escapeQuotation []byte) {
	if len(escapeQuotation) > 0 {
		bf.Write(bytes.ReplaceAll(s, escapeQuotation, append(escapeQuotation, escapeQuotation...)))
		return
	}

	var (
		escape byte
		last   = 0
	)
	// reference: https://gist.github.com/siddontang/8875771
	for i := 0; i < len(s); i++ {
		escape = 0

		switch s[i] {
		case 0: /* Must be escaped for 'mysql' */
			escape = '0'
			break
		case '\n': /* Must be escaped for logs */
			escape = 'n'
			break
		case '\r':
			escape = 'r'
			break
		case '\\':
			escape = '\\'
			break
		case '\'':
			escape = '\''
			break
		case '"': /* Better safe than sorry */
			escape = '"'
			break
		case '\032': /* This gives problems on Win32 */
			escape = 'Z'
		}

		if escape != 0 {
			bf.Write(s[last:i])
			bf.WriteByte('\\')
			bf.WriteByte(escape)
			last = i + 1
		}
	}
	if last == 0 {
		bf.Write(s)
	} else if last < len(s) {
		bf.Write(s[last:])
	}
}

func SQLTypeStringMaker() RowReceiverStringer {
	return &SQLTypeString{}
}

func SQLTypeBytesMaker() RowReceiverStringer {
	return &SQLTypeBytes{}
}

func SQLTypeNumberMaker() RowReceiverStringer {
	return &SQLTypeNumber{}
}

func MakeRowReceiverArr(colTypes []string) RowReceiverArr {
	rowReceiverArr := make([]RowReceiverStringer, len(colTypes))
	for i, colTp := range colTypes {
		recMaker, ok := colTypeRowReceiverMap[colTp]
		if !ok {
			recMaker = SQLTypeStringMaker
		}
		rowReceiverArr[i] = recMaker()
	}
	return RowReceiverArr{
		bound:     false,
		receivers: rowReceiverArr,
	}
}

func MakeRowReceiverClone(colTypes []string, r *RowReceiverArr) RowReceiverArr {
	rowReceiverArr := MakeRowReceiverArr(colTypes).receivers

	for i, v := range r.receivers {
		//ptr := reflect.New(reflect.TypeOf(v))
		//rowReceiverArr[i] = ptr.Elem().Interface().(RowReceiverStringer)
		//a:=new(RowReceiverStringer)
		// = *a
		//copier.Copy(&rowReceiverArr[i], &r.receivers[i])
		switch v.(type) {
		case *SQLTypeString:
			rowReceiverArr[i].(*SQLTypeString).Assign(v.(*SQLTypeString).RawBytes)
		case *SQLTypeBytes:
			rowReceiverArr[i].(*SQLTypeBytes).Assign(r.receivers[i].(*SQLTypeBytes).RawBytes)
		case *SQLTypeNumber:
			rowReceiverArr[i].(*SQLTypeNumber).Assign(r.receivers[i].(*SQLTypeNumber).RawBytes)
		}
		//deepcopier.Copy(v).To(rowReceiverArr[i])
		//copy(rowReceiverArr[i], v)
	}
	return RowReceiverArr{
		bound:     false,
		receivers: rowReceiverArr,
	}
}

func MakeRowReceiver(colTypes []string) RowReceiverStringer {
	rowReceiverArr := make([]RowReceiverStringer, len(colTypes))
	for i, colTp := range colTypes {
		recMaker, ok := colTypeRowReceiverMap[colTp]
		if !ok {
			recMaker = SQLTypeStringMaker
		}
		rowReceiverArr[i] = recMaker()
	}
	return RowReceiverArr{
		bound:     false,
		receivers: rowReceiverArr,
	}
}

type RowReceiverArr struct {
	bound     bool
	receivers []RowReceiverStringer
}

func (r RowReceiverArr) BindAddress(args []interface{}) {
	if r.bound {
		return
	}
	r.bound = true
	for i := range args {
		fmt.Println("r.receivers[i] type:", reflect.TypeOf(r.receivers[i]))
		r.receivers[i].BindAddress(args[i : i+1])
	}
}

func (r RowReceiverArr) WriteToBuffer(bf *bytes.Buffer, escapeBackslash bool) {
	bf.WriteByte('(')
	for i, receiver := range r.receivers {
		receiver.WriteToBuffer(bf, escapeBackslash)
		if i != len(r.receivers)-1 {
			bf.WriteByte(',')
		}
	}
	bf.WriteByte(')')
}

func (r RowReceiverArr) WriteToBufferInCsv(bf *bytes.Buffer, escapeBackslash bool, opt *csvOption) {
	for i, receiver := range r.receivers {
		receiver.WriteToBufferInCsv(bf, escapeBackslash, opt)
		if i != len(r.receivers)-1 {
			bf.Write(opt.separator)
		}
	}
}

type SQLTypeNumber struct {
	SQLTypeString
}

func (s SQLTypeNumber) WriteToBuffer(bf *bytes.Buffer, _ bool) {
	if s.RawBytes != nil {
		bf.Write(s.RawBytes)
	} else {
		bf.WriteString(nullValue)
	}
}

func (s *SQLTypeNumber) Assign(sr sql.RawBytes) {
	s.RawBytes = make(sql.RawBytes, len(sr))
	copy(s.RawBytes, sr)
	//arg[0] = new(sql.RawBytes)
	//ss := make([]byte,len(s.RawBytes))
	//copy(ss, s.RawBytes)
	//arg[0] = &ss
}

func (s SQLTypeNumber) WriteToBufferInCsv(bf *bytes.Buffer, _ bool, opt *csvOption) {
	if s.RawBytes != nil {
		bf.Write(s.RawBytes)
	} else {
		bf.WriteString(opt.nullValue)
	}
}

type SQLTypeString struct {
	sql.RawBytes
}

func (s *SQLTypeString) BindAddress(arg []interface{}) {
	arg[0] = &s.RawBytes
	//arg[0] = new(sql.RawBytes)
	//ss := make([]byte,len(s.RawBytes))
	//copy(ss, s.RawBytes)
	//arg[0] = &ss
}

func (s *SQLTypeString) Assign(sr sql.RawBytes) {
	s.RawBytes = make(sql.RawBytes, len(sr))

	copy(s.RawBytes, sr)
	//arg[0] = new(sql.RawBytes)
	//ss := make([]byte,len(s.RawBytes))
	//copy(ss, s.RawBytes)
	//arg[0] = &ss
}

func (s *SQLTypeString) WriteToBuffer(bf *bytes.Buffer, escapeBackslash bool) {
	if s.RawBytes != nil {
		bf.Write(quotationMark)
		escape(s.RawBytes, bf, getEscapeQuotation(escapeBackslash, quotationMark))
		bf.Write(quotationMark)
	} else {
		bf.WriteString(nullValue)
	}
}

func (s *SQLTypeString) WriteToBufferInCsv(bf *bytes.Buffer, escapeBackslash bool, opt *csvOption) {
	if s.RawBytes != nil {
		bf.Write(opt.delimiter)
		escape(s.RawBytes, bf, getEscapeQuotation(escapeBackslash, opt.delimiter))
		bf.Write(opt.delimiter)
	} else {
		bf.WriteString(opt.nullValue)
	}
}

type SQLTypeBytes struct {
	sql.RawBytes
}

func (s *SQLTypeBytes) BindAddress(arg []interface{}) {
	arg[0] = &s.RawBytes
	//ss := make([]byte,len(s.RawBytes))
	//ss := new(sql.RawBytes)
	//copy(&ss, s.RawBytes)
	//arg[0] = &ss
}

func (s *SQLTypeBytes) Assign(sr sql.RawBytes) {
	s.RawBytes = make(sql.RawBytes, len(sr))
	copy(s.RawBytes, sr)
	//arg[0] = new(sql.RawBytes)
	//ss := make([]byte,len(s.RawBytes))
	//copy(ss, s.RawBytes)
	//arg[0] = &ss
}

func (s *SQLTypeBytes) WriteToBuffer(bf *bytes.Buffer, _ bool) {
	if s.RawBytes != nil {
		fmt.Fprintf(bf, "x'%x'", s.RawBytes)
	} else {
		bf.WriteString(nullValue)
	}
}

func (s *SQLTypeBytes) WriteToBufferInCsv(bf *bytes.Buffer, escapeBackslash bool, opt *csvOption) {
	if s.RawBytes != nil {
		bf.Write(opt.delimiter)
		escape(s.RawBytes, bf, getEscapeQuotation(escapeBackslash, opt.delimiter))
		bf.Write(opt.delimiter)
	} else {
		bf.WriteString(opt.nullValue)
	}
}
