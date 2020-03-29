package generators

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Aggregator is a type of generator that use another collection
// to compute aggregation on it
type Aggregator interface {
	// returns the aggregator query
	Query() bson.M
	// if the query use a `local` field prefixed with "$$",
	// this method should return the name of this field
	LocalVar() string
	// returns an update operation to run
	//
	// for example:
	//
	//  { "_id": 1 }, { "$set": { "newField": ["a", "c", "f"] } }
	Update(session *mongo.Client, value interface{}) ([2]bson.M, error)
}

type baseAggregator struct {
	key        string
	query      bson.M
	collection string
	database   string
	localVar   string
}

func (a baseAggregator) Query() bson.M    { return a.query }
func (a baseAggregator) LocalVar() string { return a.localVar }

type countAggregator struct {
	baseAggregator
}

func (a *countAggregator) Update(session *mongo.Client, value interface{}) ([2]bson.M, error) {
	query := bson.M{}
	if a.query != nil {
		query = createQuery(a.query, value)
	}

	count, err := session.Database(a.database).Collection(a.collection).CountDocuments(context.Background(), query)
	if err != nil {
		return [2]bson.M{}, fmt.Errorf("couldn't count documents for key%v: %v", a.key, err)
	}
	return [2]bson.M{{a.localVar: value}, {"$set": bson.M{a.key: count}}}, nil
}

type valueAggregator struct {
	baseAggregator
	field string
}

func (a *valueAggregator) Update(session *mongo.Client, value interface{}) ([2]bson.M, error) {
	query := createQuery(a.query, value)

	var distinct struct {
		Values []interface{}
	}
	result := session.Database(a.database).RunCommand(context.Background(), bson.D{
		bson.E{Key: "distinct", Value: a.collection},
		bson.E{Key: "key", Value: a.field},
		bson.E{Key: "query", Value: query}},
	)
	if err := result.Err(); err != nil {
		return [2]bson.M{}, fmt.Errorf("aggregation failed (get distinct values) for field %v: %v", a.key, err)
	}
	if err := result.Decode(&distinct); err != nil {
		return [2]bson.M{}, fmt.Errorf("aggregation failed (decode distinct values) for field %v: %v", a.key, err)
	}
	return [2]bson.M{{a.localVar: value}, {"$set": bson.M{a.key: distinct.Values}}}, nil
}

type boundAggregator struct {
	baseAggregator
	field string
}

func (a *boundAggregator) Update(session *mongo.Client, value interface{}) ([2]bson.M, error) {

	query := createQuery(a.query, value)
	query[a.field] = bson.M{"$ne": nil}

	pipeline := []bson.M{{"$match": query},
		{"$sort": bson.M{a.field: 1}},
		{"$limit": 1},
		{"$project": bson.M{"min": "$" + a.field}}}

	cursor, err := session.Database(a.database).Collection(a.collection).Aggregate(context.Background(), pipeline)
	if err != nil {
		return [2]bson.M{}, fmt.Errorf("aggregation failed (lower bound) for field %v: %v", a.key, err)
	}
	cursor.Next(context.Background())
	var result bson.M
	cursor.Decode(&result)
	cursor.Close(context.Background())

	bound := bson.M{}
	bound["m"] = result["min"]
	pipeline = []bson.M{{"$match": query},
		{"$sort": bson.M{a.field: -1}},
		{"$limit": 1},
		{"$project": bson.M{"max": "$" + a.field}}}
	cursor, err = session.Database(a.database).Collection(a.collection).Aggregate(context.Background(), pipeline)
	if err != nil {
		return [2]bson.M{}, fmt.Errorf("aggregation failed (higher bound) for field %v: %v", a.key, err)
	}
	cursor.Next(context.Background())
	cursor.Decode(&result)
	cursor.Close(context.Background())

	bound["M"] = result["max"]
	return [2]bson.M{{a.localVar: value}, {"$set": bson.M{a.key: bound}}}, nil
}

func createQuery(formatQuery bson.M, value interface{}) bson.M {
	q := bson.M{}
	for k, v := range formatQuery {
		if s := fmt.Sprintf("%v", v); strings.Contains(s, "$$") {
			q[k] = value
		} else {
			q[k] = v
		}
	}
	return q
}
