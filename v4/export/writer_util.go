package export

import (
	"bytes"
	"context"
	_ "database/sql"
	"fmt"
	"io"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/storage"

	"github.com/pingcap/dumpling/v4/log"
)

const lengthLimit = 1048576

// TODO make this configurable, 5 mb is a good minimum size but on low latency/high bandwidth network you can go a lot bigger
const hardcodedS3ChunkSize = 5 * 1024 * 1024

var pool = sync.Pool{New: func() interface{} {
	return &bytes.Buffer{}
}}

type writerPipe struct {
	input  chan *bytes.Buffer
	closed chan struct{}
	errCh  chan error

	currentFileSize      uint64
	currentStatementSize uint64

	fileSizeLimit      uint64
	statementSizeLimit uint64

	w storage.Writer
}

func newWriterPipe(w storage.Writer, fileSizeLimit, statementSizeLimit uint64) *writerPipe {
	return &writerPipe{
		input:  make(chan *bytes.Buffer, 8),
		closed: make(chan struct{}),
		errCh:  make(chan error, 1),
		w:      w,

		currentFileSize:      0,
		currentStatementSize: 0,
		fileSizeLimit:        fileSizeLimit,
		statementSizeLimit:   statementSizeLimit,
	}
}

func (b *writerPipe) Run(ctx context.Context) {
	defer close(b.closed)
	var errOccurs bool
	for {
		select {
		case s, ok := <-b.input:
			if !ok {
				return
			}
			if errOccurs {
				continue
			}
			err := writeBytes(ctx, b.w, s.Bytes())
			s.Reset()
			pool.Put(s)
			if err != nil {
				errOccurs = true
				b.errCh <- err
			}
		case <-ctx.Done():
			return
		}
	}
}

func (b *writerPipe) AddFileSize(fileSize uint64) {
	b.currentFileSize += fileSize
	b.currentStatementSize += fileSize
}

func (b *writerPipe) Error() error {
	select {
	case err := <-b.errCh:
		return err
	default:
		return nil
	}
}

func (b *writerPipe) ShouldSwitchFile() bool {
	return b.fileSizeLimit != UnspecifiedSize && b.currentFileSize >= b.fileSizeLimit
}

func (b *writerPipe) ShouldSwitchStatement() bool {
	return (b.fileSizeLimit != UnspecifiedSize && b.currentFileSize >= b.fileSizeLimit) ||
		(b.statementSizeLimit != UnspecifiedSize && b.currentStatementSize >= b.statementSizeLimit)
}

func WriteMeta(ctx context.Context, meta MetaIR, w storage.Writer) error {
	log.Debug("start dumping meta data", zap.String("target", meta.TargetName()))

	specCmtIter := meta.SpecialComments()
	for specCmtIter.HasNext() {
		if err := write(ctx, w, fmt.Sprintf("%s\n", specCmtIter.Next())); err != nil {
			return err
		}
	}

	if err := write(ctx, w, meta.MetaSQL()); err != nil {
		return err
	}

	log.Debug("finish dumping meta data", zap.String("target", meta.TargetName()))
	return nil
}

func WriteInsert(pCtx context.Context, tblIR TableDataIR, w storage.Writer, fileSizeLimit, statementSizeLimit uint64) error {
	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return nil
	}

	bf := pool.Get().(*bytes.Buffer)
	if bfCap := bf.Cap(); bfCap < lengthLimit {
		bf.Grow(lengthLimit - bfCap)
	}

	wp := newWriterPipe(w, fileSizeLimit, statementSizeLimit)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wp.Run(ctx)
		wg.Done()
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	specCmtIter := tblIR.SpecialComments()
	for specCmtIter.HasNext() {
		bf.WriteString(specCmtIter.Next())
		bf.WriteByte('\n')
	}
	wp.currentFileSize += uint64(bf.Len())

	var (
		insertStatementPrefix string
		counter               = 0
		escapeBackSlash       = tblIR.EscapeBackSlash()
		err                   error
	)

	selectedField := tblIR.SelectedField()
	// if has generated column
	if selectedField != "" {
		insertStatementPrefix = fmt.Sprintf("INSERT INTO %s %s VALUES\n",
			wrapBackTicks(escapeString(tblIR.TableName())), selectedField)
	} else {
		insertStatementPrefix = fmt.Sprintf("INSERT INTO %s VALUES\n",
			wrapBackTicks(escapeString(tblIR.TableName())))
	}
	fmt.Println(insertStatementPrefix)
	//insertStatementPrefixLen := uint64(len(insertStatementPrefix))
	wp.currentStatementSize = 0
	//bf.WriteString(insertStatementPrefix)
	//wp.AddFileSize(insertStatementPrefixLen)
	var wg1 sync.WaitGroup
	wg1.Add(1)

	rowsChan := make(chan *RowReceiverArr, 8)
	//tmpLock := &sync.Mutex{}

	colTypes := tblIR.ColumnTypes()
	rowPool := sync.Pool{New: func() interface{} {
		return MakeRowReceiverArr(colTypes)
	}}

	rowReceiverClone := func(colTypes []string, r *RowReceiverArr) RowReceiverArr {
		rowReceiverArr := rowPool.Get().(RowReceiverArr).receivers

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

	go func() {
		defer wg1.Done()
		for {
			i, ok := <-rowsChan
			//lastBfSize := bf.Len()
			if !ok {
				//bf.Truncate(lastBfSize - 2)
				//bf.WriteString(";\n")
				break
			}

			//for _, ii := range i.receivers {
			//	switch ii.(type) {
			//	case *SQLTypeBytes:
			//		fmt.Println("args type SQLTypeBytes: ", string(ii.(*SQLTypeBytes).RawBytes))
			//	case *SQLTypeString:
			//		fmt.Println("args type SQLTypeString:", string(ii.(*SQLTypeString).RawBytes))
			//	//case  *sql.RawBytes:
			//	//	fmt.Println("args:", string(*ii.(*sql.RawBytes)))
			//
			//	default:
			//		fmt.Println("args type :", reflect.TypeOf(i))
			//
			//	}
			//}

			fmt.Println("in chan:", i.receivers)

			//switch i.(type) {
			//case RowReceiverArr:
			//	i.(RowReceiverArr).WriteToBuffer(bf, escapeBackSlash)
			//case RowReceiverStringer:
			//	i.(RowReceiverStringer).WriteToBuffer(bf, escapeBackSlash)
			//default:
			//	fmt.Println("i.(type)")
			//}
			//tmpLock.Lock()
			i.WriteToBuffer(bf, escapeBackSlash)
			//tmpLock.Unlock()
			//wp.AddFileSize(uint64(bf.Len()-lastBfSize) + 2) // 2 is for ",\n" and ";\n"
			bf.WriteString(",\n")

			//shouldSwitch := wp.ShouldSwitchStatement()
			//if !shouldSwitch {
			//	bf.WriteString(",\n")
			//} else if (shouldSwitch) || (len(rowsChan) > 0) {
			//	bf.WriteString(";\n")
			//	bf.WriteString(insertStatementPrefix)
			//	wp.AddFileSize(insertStatementPrefixLen)
			//} else {
			//}
			if bf.Len() >= lengthLimit {
				fmt.Println("bf:", bf)
				select {
				case <-pCtx.Done():
					return
				case _ = <-wp.errCh:
					return
				case wp.input <- bf:
					bf = pool.Get().(*bytes.Buffer)
					if bfCap := bf.Cap(); bfCap < lengthLimit {
						bf.Grow(lengthLimit - bfCap)
					}
				}
			}
			//if shouldSwitch {
			//
			//}

		}
	}()
	row0 := MakeRowReceiverArr(tblIR.ColumnTypes())
	for fileRowIter.HasNext() {
		//shouldSwitchChan := make(chan bool,1)
		// 一直读数据
		//row0 := MakeRowReceiverArr(tblIR.ColumnTypes())

		//tmpLock.Lock()
		if err = fileRowIter.Decode(row0); err != nil {
			log.Error("scanning from sql.Row failed", zap.Error(err))
			return err
		}
		//tmpLock.Unlock()
		fmt.Println("row0:", row0.receivers)
		fmt.Println("chan length:", len(rowsChan))
		row := rowReceiverClone(tblIR.ColumnTypes(), &row0)
		fmt.Println("row after:", row.receivers)
		fmt.Println("row0 after:", row0.receivers)
		rowsChan <- &row

		//time.Sleep(100 * time.Nanosecond)

		//lastBfSize := bf.Len()
		//row.WriteToBuffer(bf, escapeBackSlash)
		//counter += 1
		//wp.AddFileSize(uint64(bf.Len()-lastBfSize) + 2) // 2 is for ",\n" and ";\n"
		//tmpLock.Lock()
		fileRowIter.Next()
		//tmpLock.Unlock()
		//LP:		for fileRowIter.HasNext() {
		//			var row = MakeRowReceiver(tblIR.ColumnTypes())
		//
		//			if err = fileRowIter.Decode(row); err != nil {
		//				log.Error("scanning from sql.Row failed", zap.Error(err))
		//				return err
		//			}
		//			fmt.Println("row:",row)
		//			fmt.Println("chan length:",len(rowsChan))
		//			select {
		//			case <-shouldSwitchChan:
		//				fmt.Println("shouldSwitchChan:  111")
		//				break LP
		//			default:
		//				rowsChan <-row
		//			}
		//
		//			//lastBfSize := bf.Len()
		//			//row.WriteToBuffer(bf, escapeBackSlash)
		//			//counter += 1
		//			//wp.AddFileSize(uint64(bf.Len()-lastBfSize) + 2) // 2 is for ",\n" and ";\n"
		//
		//			fileRowIter.Next()
		//
		//			//if bf.Len() >= lengthLimit {
		//			//	select {
		//			//	case <-pCtx.Done():
		//			//		return pCtx.Err()
		//			//	case err := <-wp.errCh:
		//			//		return err
		//			//	case wp.input <- bf:
		//			//		bf = pool.Get().(*bytes.Buffer)
		//			//		if bfCap := bf.Cap(); bfCap < lengthLimit {
		//			//			bf.Grow(lengthLimit - bfCap)
		//			//		}
		//			//	}
		//			//}
		//
		//
		//		}

		if wp.ShouldSwitchFile() {
			break
		}
	}
	fmt.Println("1211212+++++++++")

	close(rowsChan)
	fmt.Println("1211212")
	wg1.Wait()

	log.Debug("dumping table",
		zap.String("table", tblIR.TableName()),
		zap.Int("record counts", counter))
	if bf.Len() > 0 {
		wp.input <- bf
	}
	close(wp.input)
	<-wp.closed
	if err = fileRowIter.Error(); err != nil {
		return err
	}
	return wp.Error()
}

func WriteInsertInCsv(pCtx context.Context, tblIR TableDataIR, w storage.Writer, noHeader bool, opt *csvOption, fileSizeLimit uint64) error {
	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return nil
	}

	bf := pool.Get().(*bytes.Buffer)
	if bfCap := bf.Cap(); bfCap < lengthLimit {
		bf.Grow(lengthLimit - bfCap)
	}

	wp := newWriterPipe(w, fileSizeLimit, UnspecifiedSize)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wp.Run(ctx)
		wg.Done()
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	var (
		row             = MakeRowReceiver(tblIR.ColumnTypes())
		counter         = 0
		escapeBackSlash = tblIR.EscapeBackSlash()
		err             error
	)

	if !noHeader && len(tblIR.ColumnNames()) != 0 {
		for i, col := range tblIR.ColumnNames() {
			bf.Write(opt.delimiter)
			escape([]byte(col), bf, getEscapeQuotation(escapeBackSlash, opt.delimiter))
			bf.Write(opt.delimiter)
			if i != len(tblIR.ColumnTypes())-1 {
				bf.Write(opt.separator)
			}
		}
		bf.WriteByte('\n')
	}
	wp.currentFileSize += uint64(bf.Len())

	for fileRowIter.HasNext() {
		if err = fileRowIter.Decode(row); err != nil {
			log.Error("scanning from sql.Row failed", zap.Error(err))
			return err
		}

		lastBfSize := bf.Len()
		row.WriteToBufferInCsv(bf, escapeBackSlash, opt)
		counter += 1
		wp.currentFileSize += uint64(bf.Len()-lastBfSize) + 1 // 1 is for "\n"

		bf.WriteByte('\n')
		if bf.Len() >= lengthLimit {
			select {
			case <-pCtx.Done():
				return pCtx.Err()
			case err := <-wp.errCh:
				return err
			case wp.input <- bf:
				bf = pool.Get().(*bytes.Buffer)
				if bfCap := bf.Cap(); bfCap < lengthLimit {
					bf.Grow(lengthLimit - bfCap)
				}
			}
		}

		fileRowIter.Next()
		if wp.ShouldSwitchFile() {
			break
		}
	}

	log.Debug("dumping table",
		zap.String("table", tblIR.TableName()),
		zap.Int("record counts", counter))
	if bf.Len() > 0 {
		wp.input <- bf
	}
	close(wp.input)
	<-wp.closed
	if err = fileRowIter.Error(); err != nil {
		return err
	}
	return wp.Error()
}

func write(ctx context.Context, writer storage.Writer, str string) error {
	_, err := writer.Write(ctx, []byte(str))
	if err != nil {
		// str might be very long, only output the first 200 chars
		outputLength := len(str)
		if outputLength >= 200 {
			outputLength = 200
		}
		log.Error("writing failed",
			zap.String("string", str[:outputLength]),
			zap.Error(err))
	}
	return err
}

func writeBytes(ctx context.Context, writer storage.Writer, p []byte) error {
	_, err := writer.Write(ctx, p)
	if err != nil {
		// str might be very long, only output the first 200 chars
		outputLength := len(p)
		if outputLength >= 200 {
			outputLength = 200
		}
		log.Error("writing failed",
			zap.ByteString("string", p[:outputLength]),
			zap.String("writer", fmt.Sprintf("%#v", writer)),
			zap.Error(err))
	}
	return err
}

func buildFileWriter(ctx context.Context, s storage.ExternalStorage, path string) (storage.Writer, func(ctx context.Context), error) {
	fullPath := s.URI() + path
	uploader, err := s.CreateUploader(ctx, path)
	if err != nil {
		log.Error("open file failed",
			zap.String("path", fullPath),
			zap.Error(err))
		return nil, nil, err
	}
	writer := storage.NewUploaderWriter(uploader, hardcodedS3ChunkSize)
	log.Debug("opened file", zap.String("path", fullPath))
	tearDownRoutine := func(ctx context.Context) {
		err := writer.Close(ctx)
		if err == nil {
			return
		}
		log.Error("close file failed",
			zap.String("path", fullPath),
			zap.Error(err))
	}
	return writer, tearDownRoutine, nil
}

func buildInterceptFileWriter(s storage.ExternalStorage, path string) (storage.Writer, func(context.Context)) {
	var writer storage.Writer
	fullPath := s.URI() + path
	fileWriter := &InterceptFileWriter{}
	initRoutine := func(ctx context.Context) error {
		uploader, err := s.CreateUploader(ctx, path)
		if err != nil {
			log.Error("open file failed",
				zap.String("path", fullPath),
				zap.Error(err))
			return err
		}
		w := storage.NewUploaderWriter(uploader, hardcodedS3ChunkSize)
		writer = w
		log.Debug("opened file", zap.String("path", fullPath))
		fileWriter.Writer = writer
		return err
	}
	fileWriter.initRoutine = initRoutine

	tearDownRoutine := func(ctx context.Context) {
		if writer == nil {
			return
		}
		log.Debug("tear down lazy file writer...")
		err := writer.Close(ctx)
		if err != nil {
			log.Error("close file failed", zap.String("path", fullPath))
		}
	}
	return fileWriter, tearDownRoutine
}

type LazyStringWriter struct {
	initRoutine func() error
	sync.Once
	io.StringWriter
	err error
}

func (l *LazyStringWriter) WriteString(str string) (int, error) {
	l.Do(func() { l.err = l.initRoutine() })
	if l.err != nil {
		return 0, fmt.Errorf("open file error: %s", l.err.Error())
	}
	return l.StringWriter.WriteString(str)
}

// InterceptFileWriter is an interceptor of os.File,
// tracking whether a StringWriter has written something.
type InterceptFileWriter struct {
	storage.Writer
	sync.Once
	initRoutine func(context.Context) error
	err         error

	SomethingIsWritten bool
}

func (w *InterceptFileWriter) Write(ctx context.Context, p []byte) (int, error) {
	w.Do(func() { w.err = w.initRoutine(ctx) })
	if len(p) > 0 {
		w.SomethingIsWritten = true
	}
	if w.err != nil {
		return 0, fmt.Errorf("open file error: %s", w.err.Error())
	}
	return w.Writer.Write(ctx, p)
}

func (w *InterceptFileWriter) Close(ctx context.Context) error {
	return w.Writer.Close(ctx)
}

func wrapBackTicks(identifier string) string {
	if !strings.HasPrefix(identifier, "`") && !strings.HasSuffix(identifier, "`") {
		return wrapStringWith(identifier, "`")
	}
	return identifier
}

func wrapStringWith(str string, wrapper string) string {
	return fmt.Sprintf("%s%s%s", wrapper, str, wrapper)
}
