package client

import (
	"bytes"
	"encoding/binary"

	. "github.com/lichunzhu/go-mysql/mysql"
	"github.com/lichunzhu/go-mysql/utils"
	"github.com/pingcap/errors"
)

type OutputResult struct {
	RawBytesBuf    *bytes.Buffer
	FieldResultArr []FieldValue
}

var (
	outputResultChan = make(chan *OutputResult, 80)
)

func OutputResultGet() (data *OutputResult) {
	select {
	case data = <-outputResultChan:
	}
	return data
}

func OutputResultPut(data *OutputResult) {
	select {
	case outputResultChan <- data:
	}
}

type Rows struct {
	*Conn
	*Result
	binary             bool
	err                error
	parseErr           chan error
	RawBytesBufferChan chan *bytes.Buffer
	OutputValueChan    chan *OutputResult
}

func (c *Conn) Query(command string, args ...interface{}) (*Rows, error) {
	if len(args) == 0 {
		return c.execRows(command)
	} else {
		if s, err := c.Prepare(command); err != nil {
			return nil, errors.Trace(err)
		} else {
			var r *Result
			r, err = s.Execute(args...)
			s.Close()
			return &Rows{
				Conn:   c,
				Result: r,
			}, err
		}
	}
}

func (c *Rows) Start() error {

	var data []byte
	result := c.Result
	defer func() {
		close(c.RawBytesBufferChan)
	}()
	go c.KeepParsing()
	for {
		bf, err := c.ReadPacketReuseMemNoCopy()
		if err != nil {
			c.err = err
			return err
		}
		data = bf.Bytes()

		// EOF Packet
		if c.isEOFPacket(data) {
			if c.capability&CLIENT_PROTOCOL_41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				c.status = result.Status
			}
			return nil
		}

		if data[0] == ERR_HEADER {
			c.err = c.handleErrorPacket(data)
			return c.err
		}
		c.RawBytesBufferChan <- bf

		select {
		case c.err = <-c.parseErr:
			return c.err
		default:
		}
	}
}

func (c *Rows) KeepParsing() {
	defer func() {
		close(c.OutputValueChan)
	}()
	var (
		rowData RowData
		err     error
	)
	cnt := 0
	for len(outputResultChan) < cap(outputResultChan) {
		outputResultChan <- &OutputResult{
			FieldResultArr: make([]FieldValue, len(c.Result.Fields)),
		}
	}
	for data := range c.RawBytesBufferChan {
		if err != nil {
			continue
		}
		rowData = data.Bytes()
		// 关键代码2： 这里注释了OutputResultGet方法，每次新建Buffer
		ores := new(OutputResult)
		//ores := OutputResultGet()
		ores.RawBytesBuf = data
		if len(ores.FieldResultArr) < len(c.Result.Fields) {
			cnt += 1
			ores.FieldResultArr = make([]FieldValue, len(c.Result.Fields))
		}
		ores.FieldResultArr, err = rowData.ParsePureText(c.Result.Fields, ores.FieldResultArr)
		if err != nil {
			c.parseErr <- errors.Trace(err)
		}

		c.OutputValueChan <- ores
	}

}

func (c *Rows) FinishReading(result *OutputResult) {
	utils.BytesBufferPut(result.RawBytesBuf)
	//OutputResultPut(result)
}
