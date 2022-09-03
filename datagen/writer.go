package datagen

import (
	"context"
	"io"
	"os"
	"sync"

	"github.com/feliixx/mgodatagen/datagen/generators"

	"github.com/gosuri/uiprogress"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

const (
	mongodbOutput = "mongodb"
	stdoutOutput  = "stdout"
)

type writer interface {
	write(collections []Collection, seed uint64) error
}

func newWriter(options *Options, logger io.Writer) (writer, error) {
	switch options.Output {
	case mongodbOutput:
		return newMongoWriter(options, logger)
	case stdoutOutput:
		return newFileWriter(options, logger, os.Stdout), nil
	default:
		f, err := tryToCreateFile(options.Output)
		if err != nil {
			return nil, err
		}
		return newFileWriter(options, logger, f), nil
	}
}

type rawChunk struct {
	documents  [][]byte
	nbToInsert int
}

var (
	// initial length of the slices allocated by the pool to hold
	// collections documents. This value is updated for every collection
	size int
	// use a sync.Pool to reduce memory consumption
	// also reduce the nb of items to send to the channel
	pool = sync.Pool{
		New: func() any {
			list := make([][]byte, 1000)
			for i := range list {
				list[i] = make([]byte, size)
			}
			return &rawChunk{
				documents: list,
			}
		},
	}
)

func setPoolSliceSize(docLen int) {
	margin := docLen * 10 / 100
	size = docLen + margin
	if size%2 != 0 {
		size++
	}
}

type baseWriter struct {
	progressBar *uiprogress.Bar
	logger      io.Writer

	batchSize  int
	mapRef     map[int][][]byte
	mapRefType map[int]bsontype.Type
}

func (b *baseWriter) generateDocument(ctx context.Context, tasks chan<- *rawChunk, nbDoc int, docGenerator *generators.DocumentGenerator) {

	// generate a document, and use it's length to adjust the initial
	// size of the slices in the pool 
	docBytes := docGenerator.Generate()
	setPoolSliceSize(len(docBytes))

	count := 0
Loop:
	for count < nbDoc {

		select {
		case <-ctx.Done(): // if an error occurred in one of the 'inserting' goroutines, close the channel
			break Loop
		default:
		}

		rc := pool.Get().(*rawChunk)

		rc.nbToInsert = b.batchSize
		if nbDoc-count < b.batchSize {
			rc.nbToInsert = nbDoc - count
		}

		for i := 0; i < rc.nbToInsert; i++ {

			// if doc is not large enough, allocate a new one.
			// Otherwise, reslice it.
			// Checking the cap of the slice instead of its length
			// allows to avoid a few more allocations
			if cap(rc.documents[i]) < len(docBytes) {
				rc.documents[i] = make([]byte, len(docBytes))
			} else {
				rc.documents[i] = rc.documents[i][:len(docBytes)]
			}
			copy(rc.documents[i], docBytes)

			docBytes = docGenerator.Generate()
		}

		count += rc.nbToInsert
		if b.progressBar != nil {
			b.progressBar.Set(b.progressBar.Current() + rc.nbToInsert)
		}

		tasks <- rc
	}
	close(tasks)
}
