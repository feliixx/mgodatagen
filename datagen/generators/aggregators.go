package generators

import (
	"fmt"
	"strings"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
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
	Update(session *mgo.Session, value interface{}) ([2]bson.M, error)
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

func (a *countAggregator) Update(session *mgo.Session, value interface{}) ([2]bson.M, error) {
	command := bson.D{{Name: "count", Value: a.collection}}
	if a.query != nil {
		query := createQuery(a.query, value)
		command = append(command, bson.DocElem{Name: "query", Value: query})
	}

	var update [2]bson.M
	var r struct {
		N int32 `bson:"n"`
	}
	err := session.DB(a.database).Run(command, &r)
	if err != nil {
		return update, fmt.Errorf("couldn't count documents for key%v: %v", a.key, err)
	}
	update = [2]bson.M{{a.localVar: value}, {"$set": bson.M{a.key: r.N}}}
	return update, nil
}

type valueAggregator struct {
	baseAggregator
	field string
}

func (a *valueAggregator) Update(session *mgo.Session, value interface{}) ([2]bson.M, error) {
	query := createQuery(a.query, value)

	var update [2]bson.M
	var result struct {
		Values []interface{} `bson:"values"`
	}
	err := session.DB(a.database).Run(bson.D{
		{Name: "distinct", Value: a.collection},
		{Name: "key", Value: a.field},
		{Name: "query", Value: query}}, &result)

	if err != nil {
		return update, fmt.Errorf("aggregation failed (distinct values) for field %v: %v", a.key, err)
	}
	update = [2]bson.M{{a.localVar: value}, {"$set": bson.M{a.key: result.Values}}}
	return update, nil
}

type boundAggregator struct {
	baseAggregator
	field string
}

func (a *boundAggregator) Update(session *mgo.Session, value interface{}) ([2]bson.M, error) {

	query := createQuery(a.query, value)
	query[a.field] = bson.M{"$ne": nil}

	var update [2]bson.M
	var res bson.M
	pipeline := []bson.M{{"$match": query},
		{"$sort": bson.M{a.field: 1}},
		{"$limit": 1},
		{"$project": bson.M{"min": "$" + a.field}}}
	err := session.DB(a.database).C(a.collection).Pipe(pipeline).One(&res)
	if err != nil {
		return update, fmt.Errorf("aggregation failed (lower bound) for field %v: %v", a.key, err)
	}

	bound := bson.M{}
	bound["m"] = res["min"]
	pipeline = []bson.M{{"$match": query},
		{"$sort": bson.M{a.field: -1}},
		{"$limit": 1},
		{"$project": bson.M{"max": "$" + a.field}}}
	err = session.DB(a.database).C(a.collection).Pipe(pipeline).One(&res)
	if err != nil {
		return update, fmt.Errorf("aggregation failed (higher bound) for field %v: %v", a.key, err)
	}
	bound["M"] = res["max"]
	update = [2]bson.M{{a.localVar: value}, {"$set": bson.M{a.key: bound}}}
	return update, nil
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
