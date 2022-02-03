package datagen

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gosuri/uiprogress"
	"github.com/gosuri/uiprogress/util/strutil"
	"github.com/olekukonko/tablewriter"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/feliixx/mgodatagen/datagen/generators"
)

type mongoWriter struct {
	*basicGenerator
	logger io.Writer

	session    *mongo.Client
	version    []int
	indexFirst bool
	indexOnly  bool
	append     bool
	numWorker  int

	mapRef     map[int][][]byte
	mapRefType map[int]bsontype.Type
}

func newMongoWriter(options *Options, logger io.Writer) (writer, error) {

	session, version, err := connectToDB(&options.Connection, logger)
	if err != nil {
		return nil, err
	}

	return &mongoWriter{
		basicGenerator: &basicGenerator{
			batchSize: options.BatchSize,
		},
		logger:     logger,
		session:    session,
		version:    version,
		indexFirst: options.IndexFirst,
		indexOnly:  options.IndexOnly,
		append:     options.Append,
		numWorker:  options.NumInsertWorker,
		mapRef:     make(map[int][][]byte),
		mapRefType: make(map[int]bsontype.Type),
	}, nil
}

func (w *mongoWriter) write(collections []Collection, seed uint64) (err error) {

	defer w.session.Disconnect(context.Background())

	// build all generators / aggregators before generating the collection, so we can
	// return any config error faster.
	// That way, if the config contains an error in the n-th collection, we don't have to
	// wait for the n-1 first collections to be generated to get the error
	for i := 0; i < len(collections); i++ {

		ci := generators.NewCollInfo(collections[i].Count, w.version, seed, w.mapRef, w.mapRefType)

		collections[i].docGenerator, err = ci.NewDocumentGenerator(collections[i].Content)
		if err != nil {
			return fmt.Errorf("fail to create DocumentGenerator for collection '%s'\n%v", collections[i].Name, err)
		}
		collections[i].aggregators, err = ci.NewAggregatorSlice(collections[i].Content)
		if err != nil {
			return fmt.Errorf("fail to create Aggregator for collection '%s'\n%v", collections[i].Name, err)
		}
	}

	for _, collection := range collections {
		err = w.generate(&collection)
		if err != nil {
			return err
		}
	}
	w.printStats(collections)

	return nil
}

func (w *mongoWriter) generate(collection *Collection) error {

	var steps = []struct {
		name     string
		size     int
		stepFunc func(dtg *mongoWriter, collection *Collection) error
	}{
		{
			name:     "creating",
			size:     2,
			stepFunc: (*mongoWriter).createCollection,
		},
		{
			name:     "generating",
			size:     collection.Count,
			stepFunc: (*mongoWriter).fillCollection,
		},
		{
			name:     "aggregating",
			size:     collection.Count * 10 / 100,
			stepFunc: (*mongoWriter).updateWithAggregators,
		},
		{
			name:     "indexing",
			size:     collection.Count * 10 / 100,
			stepFunc: (*mongoWriter).ensureIndex,
		},
	}

	if w.indexFirst {
		steps[1], steps[3] = steps[3], steps[1]
	}

	progress := uiprogress.New()
	progress.SetOut(w.logger)
	progress.SetRefreshInterval(50 * time.Millisecond)
	progress.Start()
	defer progress.Stop()

	total := 0
	bounds := make(sort.IntSlice, 0, len(steps))
	for _, s := range steps {
		total += s.size
		bounds = append(bounds, total-1)
	}

	w.progressBar = progress.AddBar(total).AppendCompleted().PrependFunc(func(b *uiprogress.Bar) string {

		current := b.Current()
		stepName := "done"
		if current != total {
			stepName = steps[bounds.Search(current)].name
		}
		return strutil.Resize(fmt.Sprintf("collection %s: %s", collection.Name, stepName), 35)
	})

	for _, s := range steps {
		err := s.stepFunc(w, collection)
		if err != nil {
			return err
		}
		w.progressBar.Set(w.progressBar.Current() + s.size)
	}
	return nil
}

// create a collection with specific options
func (w *mongoWriter) createCollection(coll *Collection) error {
	c := w.session.Database(coll.DB).Collection(coll.Name)

	if w.append || w.indexOnly {
		return nil
	}
	err := c.Drop(context.Background())
	if err != nil {
		return fmt.Errorf("fail to drop collection '%s'\ncause  %v", coll.Name, err)
	}

	createCommand := bson.D{
		bson.E{Key: "create", Value: coll.Name},
	}
	if coll.CompressionLevel != "" {
		createCommand = append(createCommand, bson.E{Key: "storageEngine", Value: bson.M{"wiredTiger": bson.M{"configString": "block_compressor=" + coll.CompressionLevel}}})
	}
	err = w.session.Database(coll.DB).RunCommand(context.Background(), createCommand).Err()
	if err != nil {
		return fmt.Errorf("coulnd't create collection with compression level '%s'\n  cause: %v", coll.CompressionLevel, err)
	}

	if coll.ShardConfig.ShardCollection != "" {
		nm := coll.DB + "." + coll.Name
		if coll.ShardConfig.ShardCollection != nm {
			return fmt.Errorf("wrong value for 'shardConfig.shardCollection', should be <database>.<collection>: found '%s', expected '%s'", coll.ShardConfig.ShardCollection, nm)
		}
		if len(coll.ShardConfig.Key) == 0 {
			return fmt.Errorf("wrong value for 'shardConfig.key', can't be null and must be an object like {'_id': 'hashed'}, found: %v", coll.ShardConfig.Key)
		}
		err := w.session.Database("admin").RunCommand(context.Background(), bson.D{bson.E{Key: "enableSharding", Value: coll.DB}}).Err()
		if err != nil {
			return fmt.Errorf("fail to enable sharding on db '%s'\n  cause: %v", coll.DB, err)
		}
		// as the collection is empty, no need to create the indexes on the sharded key before creating the collection,
		// because it will be created automatically by mongodb. See https://docs.mongodb.com/manual/core/sharding-shard-key/#shard-key-indexes
		// for details
		err = runMgoCompatCommand(context.Background(), w.session, "admin", coll.ShardConfig)
		if err != nil {
			return fmt.Errorf("fail to shard collection '%s' in db '%s'\n  cause: %v", coll.Name, coll.DB, err)
		}
	}
	return nil
}

func (w *mongoWriter) fillCollection(coll *Collection) error {

	if w.indexOnly {
		return nil
	}

	nbInsertingGoRoutines := runtime.NumCPU()
	if w.numWorker > 0 {
		nbInsertingGoRoutines = w.numWorker
	}

	taskBufferSize := 10
	// for really small insert, use only one goroutine and reduce the buffered channel size
	if coll.Count <= 10000 {
		nbInsertingGoRoutines = 1
		taskBufferSize = 1
	}

	tasks := make(chan *rawChunk, taskBufferSize)
	errs := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	for i := 0; i < nbInsertingGoRoutines; i++ {
		wg.Add(1)
		go w.insertDocumentFromChannel(ctx, cancel, &wg, coll, tasks, errs)
	}
	w.generateDocument(ctx, tasks, coll.Count, coll.docGenerator)

	wg.Wait()

	select {
	case err, ok := <-errs:
		if ok {
			return err
		}
	default:
	}
	return nil
}

func (w *mongoWriter) insertDocumentFromChannel(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, coll *Collection, tasks <-chan *rawChunk, errs chan error) {

	defer wg.Done()

	c := w.session.Database(coll.DB).Collection(coll.Name)

	insertOpts := options.InsertMany()
	// if indexfirst mode is set, specify that writes are unordered so failed
	// insert will not block the process. This is useful in the case of an
	// index with 'unique' constraint on two or more fields. There is currently
	// no way to specify 'maxDistinctValue' on a combination of top level fields,
	// and we can't guarantee that there will be no duplicate in generated collection,
	// so the only option left is to ignore insert that fail because of duplicates
	// writes
	if w.indexFirst {
		insertOpts.SetOrdered(false)
	}

	for t := range tasks {
		// if an error occurs in one of the goroutine, 'return' is called which trigger
		// wg.Done() ==> the goroutine stops
		select {
		case <-ctx.Done():
			return
		default:
		}

		docs := make([]interface{}, 0, t.nbToInsert)
		for _, doc := range t.documents[:t.nbToInsert] {
			docs = append(docs, doc)
		}

		_, err := c.InsertMany(ctx, docs, insertOpts)
		if !w.indexFirst && err != nil {
			// if the bulk insert fails, push the error to the error channel
			// so that we can use it from the main thread
			select {
			case errs <- fmt.Errorf("exception occurred during bulk insert\n  cause: %v\n Try to set a smaller batch size with -b | --batchsize option", err):
				// cancel the context to terminate goroutine and stop the feeding of the
				// buffered channel
				cancel()
			default:
			}
			return
		}
		pool.Put(t)
	}
}

// Update documents with pre-computed aggregations
func (w *mongoWriter) updateWithAggregators(coll *Collection) error {

	if w.indexOnly {
		return nil
	}

	if len(coll.aggregators) == 0 {
		return nil
	}

	// aggregation might be very long, so make sure the connection won't timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	tasks := make(chan [2]bson.M, w.batchSize)
	errs := make(chan error)
	collection := w.session.Database(coll.DB).Collection(coll.Name)

	// run updates in a new goroutine
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		for t := range tasks {
			_, err := collection.UpdateMany(ctx, t[0], t[1])
			if err != nil {
				errs <- fmt.Errorf("exception occurred during update\n  cause: %v", err)
				return
			}
		}
	}()

	var aggregationError error
Loop:
	for _, aggregator := range coll.aggregators {

		localVar := aggregator.LocalVar()
		var distinct struct {
			Values []interface{}
		}
		result := w.session.Database(coll.DB).RunCommand(ctx, bson.D{
			bson.E{Key: "distinct", Value: coll.Name},
			bson.E{Key: "key", Value: localVar},
		})
		if err := result.Err(); err != nil {
			aggregationError = fmt.Errorf("fail to get distinct values for local field '%s'\n  cause: %v", localVar, err)
			break Loop
		}
		if err := result.Decode(&distinct); err != nil {
			aggregationError = fmt.Errorf("fail to decode distinct values for local field '%s'\n  cause: %v", localVar, err)
			break Loop
		}

		for _, value := range distinct.Values {
			select {
			case aggregationError = <-errs:
				break Loop
			default:
			}

			update, aggregationError := aggregator.Update(w.session, value)
			if aggregationError != nil {
				break Loop
			}
			tasks <- update
		}
	}
	close(tasks)
	wg.Wait()
	return aggregationError
}

func (w *mongoWriter) ensureIndex(coll *Collection) error {

	if len(coll.Indexes) == 0 {
		return nil
	}

	_, err := w.session.Database(coll.DB).Collection(coll.Name).Indexes().DropAll(context.Background())
	if err != nil {
		return fmt.Errorf("error while dropping index for collection '%s'\n  cause: %v", coll.Name, err)
	}
	// avoid timeout when building indexes
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	err = runMgoCompatCommand(ctx, w.session, coll.DB, bson.D{
		bson.E{Key: "createIndexes", Value: coll.Name},
		bson.E{Key: "indexes", Value: coll.Indexes},
	})
	if err != nil {
		return fmt.Errorf("error while building indexes for collection '%s'\n cause: %v", coll.Name, err)
	}
	return nil
}

func (w *mongoWriter) printStats(collections []Collection) {

	if w.logger == io.Discard {
		return
	}

	rows := make([][]string, 0, len(collections))

	for _, coll := range collections {

		result := w.session.Database(coll.DB).RunCommand(context.Background(), bson.D{
			bson.E{Key: "collStats", Value: coll.Name},
			bson.E{Key: "scale", Value: 1024},
		})

		var stats struct {
			Count      int    `bson:"count"`
			AvgObjSize int    `bson:"avgObjSize"`
			IndexSizes bson.M `bson:"indexSizes"`
		}
		err := result.Decode(&stats)
		if err != nil {
			fmt.Fprintf(w.logger, "fail to parse stats result\n  cause: %v", err)
		}

		indexes := make([]string, 0, len(stats.IndexSizes))
		for name, size := range stats.IndexSizes {
			indexes = append(indexes, fmt.Sprintf("%s  %v kB", name, size))
		}
		rows = append(rows, []string{
			coll.Name,
			strconv.Itoa(stats.Count),
			strconv.Itoa(stats.AvgObjSize),
			strings.Join(indexes, "\n"),
		})
	}

	fmt.Fprintf(w.logger, "\n")
	table := tablewriter.NewWriter(w.logger)
	table.SetHeader([]string{"collection", "count", "avg object size", "indexes"})
	table.AppendBulk(rows)
	table.Render()
}
