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
	"go.mongodb.org/mongo-driver/bson/mgocompat"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/feliixx/mgodatagen/datagen/generators"
)

type dtg struct {
	out        io.Writer
	bar        *uiprogress.Bar
	session    *mongo.Client
	version    []int
	mapRef     map[int][][]byte
	mapRefType map[int]bsontype.Type
	Options
}

func (d *dtg) generate(collection *Collection) error {

	var steps = []struct {
		name     string
		size     int
		stepFunc func(dtg *dtg, collection *Collection) error
	}{
		{
			name:     "creating",
			size:     2,
			stepFunc: (*dtg).createCollection,
		},
		{
			name:     "generating",
			size:     collection.Count,
			stepFunc: (*dtg).fillCollection,
		},
		{
			name:     "aggregating",
			size:     collection.Count * 10 / 100,
			stepFunc: (*dtg).updateWithAggregators,
		},
		{
			name:     "indexing",
			size:     collection.Count * 10 / 100,
			stepFunc: (*dtg).ensureIndex,
		},
	}

	if d.Options.IndexFirst {
		steps[1], steps[3] = steps[3], steps[1]
	}

	progress := uiprogress.New()
	progress.SetOut(d.out)
	progress.SetRefreshInterval(50 * time.Millisecond)
	progress.Start()
	defer progress.Stop()

	total := 0
	bounds := make(sort.IntSlice, 0, len(steps))
	for _, s := range steps {
		total += s.size
		bounds = append(bounds, total-1)
	}

	d.bar = progress.AddBar(total).AppendCompleted().PrependFunc(func(b *uiprogress.Bar) string {

		current := b.Current()
		stepName := "done"
		if current != total {
			stepName = steps[bounds.Search(current)].name
		}
		return strutil.Resize(fmt.Sprintf("collection %s: %s", collection.Name, stepName), 35)
	})

	for _, s := range steps {
		err := s.stepFunc(d, collection)
		if err != nil {
			return err
		}
		d.bar.Set(d.bar.Current() + s.size)
	}
	return nil
}

// create a collection with specific options
func (d *dtg) createCollection(coll *Collection) error {
	c := d.session.Database(coll.DB).Collection(coll.Name)

	if d.Append || d.IndexOnly {
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
	err = d.session.Database(coll.DB).RunCommand(context.Background(), createCommand).Err()
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
		err := d.session.Database("admin").RunCommand(context.Background(), bson.D{bson.E{Key: "enableSharding", Value: coll.DB}}).Err()
		if err != nil {
			return fmt.Errorf("fail to enable sharding on db '%s'\n  cause: %v", coll.DB, err)
		}
		// as the collection is empty, no need to create the indexes on the sharded key before creating the collection,
		// because it will be created automatically by mongodb. See https://docs.mongodb.com/manual/core/sharding-shard-key/#shard-key-indexes
		// for details
		err = d.runMgoCompatCommand(context.Background(), "admin", coll.ShardConfig)
		if err != nil {
			return fmt.Errorf("fail to shard collection '%s' in db '%s'\n  cause: %v", coll.Name, coll.DB, err)
		}
	}
	return nil
}

type rawChunk struct {
	documents  [][]byte
	nbToInsert int
}

// use a sync.Pool to reduce memory consumption
// also reduce the nb of items to send to the channel
var pool = sync.Pool{
	New: func() interface{} {
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

func (d *dtg) fillCollection(coll *Collection) error {

	if d.IndexOnly {
		return nil
	}

	nbInsertingGoRoutines := runtime.NumCPU()
	if d.NumInsertWorker > 0 {
		nbInsertingGoRoutines = d.NumInsertWorker
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
		go d.insertDocumentFromChannel(ctx, cancel, &wg, coll, tasks, errs)
	}
	d.generateDocument(ctx, tasks, coll.Count, coll.docGenerator)

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

func (d *dtg) insertDocumentFromChannel(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, coll *Collection, tasks <-chan *rawChunk, errs chan error) {

	defer wg.Done()

	c := d.session.Database(coll.DB).Collection(coll.Name)

	insertOpts := options.InsertMany()
	// if indexfirst mode is set, specify that writes are unordered so failed
	// insert will not block the process. This is usefull in the case of an
	// index with 'unique' constraint on two or more fields. There is currently
	// no way to specify 'maxDistinctValue' on a combinaison of top level fields,
	// and we can't garantee that there will be no duplicate in generated collection,
	// so the only option left is to ignore insert that fail because of duplicates
	// writes
	if d.Options.IndexFirst {
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
		if !d.Options.IndexFirst && err != nil {
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

func (d *dtg) generateDocument(ctx context.Context, tasks chan<- *rawChunk, nbDoc int, docGenerator *generators.DocumentGenerator) {

	count := 0
	for count < nbDoc {

		select {
		case <-ctx.Done(): // if an error occurred in one of the 'inserting' goroutines, close the channel
			break
		default:
		}

		rc := pool.Get().(*rawChunk)

		rc.nbToInsert = d.BatchSize
		if nbDoc-count < d.BatchSize {
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
		d.bar.Set(d.bar.Current() + rc.nbToInsert)

		tasks <- rc
	}
	close(tasks)
}

// Update documents with pre-computed aggregations
func (d *dtg) updateWithAggregators(coll *Collection) error {

	if d.IndexOnly {
		return nil
	}

	if len(coll.aggregators) == 0 {
		return nil
	}

	// aggregation might be very long, so make sure the connection won't timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	tasks := make(chan [2]bson.M, d.BatchSize)
	errs := make(chan error)
	collection := d.session.Database(coll.DB).Collection(coll.Name)

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
		result := d.session.Database(coll.DB).RunCommand(ctx, bson.D{
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

			update, aggregationError := aggregator.Update(d.session, value)
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

func (d *dtg) ensureIndex(coll *Collection) error {

	if len(coll.Indexes) == 0 {
		return nil
	}

	_, err := d.session.Database(coll.DB).Collection(coll.Name).Indexes().DropAll(context.Background())
	if err != nil {
		return fmt.Errorf("error while dropping index for collection '%s'\n  cause: %v", coll.Name, err)
	}
	// avoid timeout when building indexes
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	err = d.runMgoCompatCommand(ctx, coll.DB, bson.D{
		bson.E{Key: "createIndexes", Value: coll.Name},
		bson.E{Key: "indexes", Value: coll.Indexes},
	})
	if err != nil {
		return fmt.Errorf("error while building indexes for collection '%s'\n cause: %v", coll.Name, err)
	}
	return nil
}

func (d *dtg) printStats(collections []Collection) {

	if d.Options.Quiet {
		return
	}

	rows := make([][]string, 0, len(collections))

	for _, coll := range collections {

		result := d.session.Database(coll.DB).RunCommand(context.Background(), bson.D{
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
			fmt.Fprintf(d.out, "fail to parse stats result\n  cause: %v", err)
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

	fmt.Fprintf(d.out, "\n")
	table := tablewriter.NewWriter(d.out)
	table.SetHeader([]string{"collection", "count", "avg object size", "indexes"})
	table.AppendBulk(rows)
	table.Render()
}

func (d *dtg) runMgoCompatCommand(ctx context.Context, db string, cmd interface{}) error {
	// With the default registry, index.Collation is kept event when it's empty,
	// and it make the command fail
	// to fix this, marshal the command to a bson.Raw with the mgocompat registry
	// providing the same behavior that the old mgo driver
	mgoRegistry := mgocompat.NewRespectNilValuesRegistryBuilder().Build()
	_, cmdBytes, err := bson.MarshalValueWithRegistry(mgoRegistry, cmd)
	if err != nil {
		return fmt.Errorf("fait to generate mgocompat command\n  cause: %v", err)
	}
	return d.session.Database(db).RunCommand(ctx, cmdBytes).Err()
}
