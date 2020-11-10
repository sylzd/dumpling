package utils

import (
	"bytes"
	"sync"
)

var (
	bytesBufferPool = sync.Pool{
		New: func() interface{} {
			return &bytes.Buffer{}
		},
	}
	bytesBufferChan = make(chan *bytes.Buffer, 10)
)

func BytesBufferGet() (data *bytes.Buffer) {
	//select {
	//case data = <-bytesBufferChan:
	//default:
	//	data = bytesBufferPool.Get().(*bytes.Buffer)
	//}
	//
	//data.Reset()
	//
	//return data
	// 关键代码4： 直接New新的Buffer，不走chan
	return new(bytes.Buffer)
}

func BytesBufferPut(data *bytes.Buffer) {
	select {
	case bytesBufferChan <- data:
	default:
		bytesBufferPool.Put(data)
	}
}
