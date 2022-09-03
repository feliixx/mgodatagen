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

// use a sync.Pool to reduce memory consumption
// also reduce the nb of items to send to the channel
var pool = sync.Pool{
	New: func() any {
		list := make([][]byte, 1000)
		for i := range list {
			// use 256 bytes as default buffer size, because it's close to the
			// average bson document size out there (mongodb-go-driver use the
			// same value internally)
			list[i] = make([]byte, 256)
		}
		return &rawChunk{
			documents: list,
		}
	},
}

type baseWriter struct {
	progressBar *uiprogress.Bar
	logger      io.Writer

	batchSize  int
	mapRef     map[int][][]byte
	mapRefType map[int]bsontype.Type
}

func (b *baseWriter) generateDocument(ctx context.Context, tasks chan<- *rawChunk, nbDoc int, docGenerator *generators.DocumentGenerator) {

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
			docBytes := docGenerator.Generate()

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
		}

		count += rc.nbToInsert
		if b.progressBar != nil {
			b.progressBar.Set(b.progressBar.Current() + rc.nbToInsert)
		}

		tasks <- rc
	}
	close(tasks)
}
