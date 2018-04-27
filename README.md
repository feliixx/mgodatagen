[![Linux and macOS Build Status](https://travis-ci.org/feliixx/mgodatagen.svg?branch=master)](https://travis-ci.org/feliixx/mgodatagen)
[![Windows Build Status](https://ci.appveyor.com/api/projects/status/ll70glgrjib8x8k5/branch/master?svg=true)](https://ci.appveyor.com/project/feliixx/mgodatagen)
[![Go Report Card](https://goreportcard.com/badge/github.com/feliixx/mgodatagen)](https://goreportcard.com/report/github.com/feliixx/mgodatagen)
[![codecov](https://codecov.io/gh/feliixx/mgodatagen/branch/master/graph/badge.svg)](https://codecov.io/gh/feliixx/mgodatagen)
[![GoDoc](https://godoc.org/github.com/feliixx/mgodatagen?status.svg)](http://godoc.org/github.com/feliixx/mgodatagen)

# mgodatagen 

A small CLI tool to quickly generate millions of pseudo-random BSON documents and insert them into a Mongodb instance. Quickly test new data structure or how your application responds when your database grows! 

Try it online: [**mongoplayground**](https://mongoplayground.net/)

## Features

- Support all bson types listed in [MongoDB bson types](https://docs.mongodb.com/manual/reference/bson-types/)
- Generate *real* data using [faker](https://github.com/manveru/faker)
- Create referenced fields accross collections
- Aggregate data accross collections
- Create sharded collection
- Create collections in multiple databases
- Cross-plateform. Tested on Unix / OSX / windows

![Demo](demo.gif)



## installation

Download the binary from the [release page](https://github.com/feliixx/mgodatagen/releases)

or 

Build from source: 

First, make sure that go is installed on your machine (see [install go](https://golang.org/doc/install) for details ). Then, use `go get`:

```
go get -u "github.com/feliixx/mgodatagen"
```

## Options

Several options are available (use `mgodatagen --help` to print this): 

```
Usage:
  mgodatagen

template:
      --new=<filename>         create an empty configuration file

configuration:
  -f, --file=<configfile>      JSON config file. This field is required
  -i, --indexonly              if present, mgodatagen will just try to rebuild index
  -s, --shortname              if present, JSON keys in the documents will be reduced
                               to the first two letters only ('name' => 'na')
  -a, --append                 if present, append documents to the collection without
                               removing older documents or deleting the collection
  -n, --numWorker=<nb>         number of concurrent workers inserting documents
                               in database. Default is number of CPU+1
  -b, --batchsize=<size>       bulk insert batch size (default: 1000)

connection infos:
  -h, --host=<hostname>        mongodb host to connect to (default: 127.0.0.1)
      --port=<port>            server port (default: 27017)
  -u, --username=<username>    username for authentification
  -p, --password=<password>    password for authentification

general:
      --help                   show this help message
  -v, --version                print the tool version and exit
  -q, --quiet                  quieter output

```

Only the configuration file need to be specified ( -f | --file flag). A basic usage of mgodatagen would be 

```
./mgodatagen -f config.json 
```

If no host/port is specified, mgodatagen tries to connect to **`mongodb://127.0.0.1:27017`**. 



# Configuration file

The config file is an array of JSON documents, where each documents holds the configuration 
for a collection to create 

See **MongodB documentation** for details on parameters: 

 - shardConfig: [**shardCollection**](https://docs.mongodb.com/manual/reference/command/shardCollection/)
 - indexes: [**indexes**](https://docs.mongodb.com/manual/reference/method/db.collection.createIndex/)
 - collation: [**collation**](https://docs.mongodb.com/manual/reference/bson-type-comparison-order/#collation)

```JSON5

[
  // first collection to create 
  {  
   // REQUIRED FIELDS
   // 
   "database": <string>,              // required, database name
   "collection": <string>,            // required, collection name
   "count": <int>,                    // required, number of document to insert in the collection 
   "content": {                       // required, the actual schema to generate documents   
     "fieldName1": <generator>,       // optional, see Generator below
     "fieldName2": <generator>,       
     ...
   },
   // OPTIONAL FIELDS
   //
   // compression level (for WiredTiger engine only)
   // possible values:
   // - none
   // - snappy
   // - zlib 
   "compressionLevel": <string>,      // optional, default: snappy

   // configuration for sharded collection
   "shardConfig": {                   // optional 
      "shardCollection": <string>.<string>, // required. <database>.<collection>
      "key": <object>,                // required, shard key, eg: {"_id": "hashed"}
      "unique": <boolean>,            // optional, default: false
      "numInitialChunks": <int>       // optional 

      "collation": {                  // optional 
        "locale": <string>,
        "caseLevel": <boolean>,
        "caseFirst": <string>,
        "strength": <int>,
        "numericOrdering": <boolean>,
        "alternate": <string>,
        "maxVariable": <string>,
        "backwards": <boolean>
      }
   },

   // list of index to build
   "indexes": [                       // optional  
      {
         "name": <string>,            // required, index name
         "key": <object>,             // required, index key, eg: {"name": 1}
         "sparse": <boolean>,         // optional, default: false
         "unique": <boolean>,         // optional, default: false
         "background": <boolean>,     // optional, default: false
         "bits": <int>,               // optional, for 2d indexes only, default: 26
         "min": <double>,             // optional, for 2d indexes only, default: -180.0
         "max": <double>,             // optional, for 2d index only, default: 180.0
         "bucketSize": <double>,      // optional, for geoHaystack indexes only
         "expireAfterSeconds": <int>, // optional, for TTL indexes only
         "weights": <string>,         // optional, for text indexes only 
         "defaultLanguage": <string>, // optional, for text index only 
         "languageOverride": <string>,// optional, for text index only
         "textIndexVersion": <int>,   // optional, for text index only
         "partialFilterExpression": <object>, // optional 

         "collation": {               // optional 
           "locale": <string>,
           "caseLevel": <boolean>,
           "caseFirst": <string>,
           "strength": <int>,
           "numericOrdering": <boolean>,
           "alternate": <string>,
           "maxVariable": <string>,
           "backwards": <boolean>                
         }
   ]
  },
  // second collection to create 
  {
    ...
  }
]
```

### Example

A set of sample config files can be found in **datagen/testdata/**. To use it, 
make sure that you have a mongodb instance running (on 127.0.0.1:27017 for example)
and run 

```
./mgodatagen -f datagen/testdata/ref.json
```

This will insert 1000 random documents in collections `test` and `link` of database 
`datagen_it_test` with the structure defined in the config file. 


# Generator types  


Generators have a common structure: 

```JSON5
"fieldName": {                 // required, field name in generated document
  "type": <string>,            // required, type of the field 
  "nullPercentage": <int>,     // optional, int between 0 and 100. Percentage of documents 
                               // that will have this field
  "maxDistinctValue": <int>,   // optional, maximum number of distinct values for this field
  "typeParam": ...             // specific parameters for this type
}
```

List of main `<generator>` types: 

- [string](#string)
- [int](#int)
- [long](#long)
- [double](#double)
- [decimal](#decimal)
- [boolean](#boolean)
- [objectId](#objectid)
- [array](#array)
- [object](#object)
- [binary](#binary) 
- [date](#date) 

List of custom `<generator>` types: 

- [position](#position)
- [constant](#constant)
- [autoincrement](#autoincrement)
- [reference](#ref)
- [fromArray](#fromarray)
- [countAggregator](#countAggregator)
- [valueAggregator](#valueAggregator)
- [boundAggregator](#boundAggregator)

List of [Faker](https://github.com/manveru/faker) `<generator>` types: 

- [CellPhoneNumber](#faker)
- [City](#faker)
- [CityPrefix](#faker)
- [CitySuffix](#faker)
- [CompanyBs](#faker)
- [CompanyCatchPhrase](#faker)
- [CompanyName](#faker)
- [CompanySuffix](#faker)
- [Country](#faker)
- [DomainName](#faker)
- [DomainSuffix](#faker)
- [DomainWord](#faker)
- [Email](#faker)
- [FirstName](#faker)
- [FreeEmail](#faker)
- [JobTitle](#faker)
- [LastName](#faker)
- [Name](#faker)
- [NamePrefix](#faker)
- [NameSuffix](#faker)
- [PhoneNumber](#faker)
- [PostCode](#faker)
- [SafeEmail](#faker)
- [SecondaryAddress](#faker)
- [State](#faker)
- [StateAbbr](#faker)
- [StreetAddress](#faker)
- [StreetName](#faker)
- [StreetSuffix](#faker)
- [URL](#faker)
- [UserName](#faker)



### String

Generate random string of a certain length. String is composed of char within this list: 
`abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_`

```JSON5
"fieldName": {
    "type": "string",          // required
    "nullPercentage": <int>,   // optional 
    "maxDistinctValue": <int>, // optional
    "unique": <bool>,          // optional, see details below 
    "minLength": <int>,        // required,  must be >= 0 
    "maxLength": <int>         // required,  must be >= minLength
}
```

#### Unique String

if `unique` is set to true, the field will only contains unique strings. Unique strings 
have a **fixed length**, `minLength` is taken as length for the string. 
There is  `64^x`  possible unique string for strings of length `x`. This number has to 
be inferior or equal to the number of documents you want to generate. 
For example, if you want unique strings of length 3, the is `64 * 64 * 64 = 262144` possible 
strings

They will look like 

```
"aaa",
"aab",
"aac",
"aad",
...
```

### Int 

Generate random int within bounds. 

```JSON5
"fieldName": {
    "type": "int",             // required
    "nullPercentage": <int>,   // optional 
    "maxDistinctValue": <int>, // optional
    "minInt": <int>,           // required
    "maxInt": <int>            // required, must be >= minInt
}
```

### Long 

Generate random long within bounds. 

```JSON5
"fieldName": {
    "type": "long",            // required
    "nullPercentage": <int>,   // optional 
    "maxDistinctValue": <int>, // optional
    "minLong": <long>,         // required
    "maxLong": <long>          // required, must be >= minLong
}
```

### Double

Generate random double within bounds. 

```JSON5
"fieldName": {
    "type": "double",          // required
    "nullPercentage": <int>,   // optional
    "maxDistinctValue": <int>, // optional 
    "minDouble": <double>,     // required
    "maxDouble": <double>      // required, must be >= minDouble
}
```

### Decimal

Generate random decimal128

```JSON5
"fieldName": {
    "type": "decimal",         // required
    "nullPercentage": <int>,   // optional
    "maxDistinctValue": <int>, // optional 
}
```

### Boolean

Generate random boolean

```JSON5
"fieldName": {
    "type": "boolean",         // required
    "nullPercentage": <int>,   // optional 
    "maxDistinctValue": <int>  // optional
}
```

### ObjectId

Generate random and unique objectId

```JSON5
"fieldName": {
    "type": "objectId",        // required
    "nullPercentage": <int>,   // optional
    "maxDistinctValue": <int>  // optional 
}
```

### Array

Generate a random array of bson object 

```JSON5
"fieldName": {
    "type": "array",             // required
    "nullPercentage": <int>,     // optional
    "maxDistinctValue": <int>,   // optional
    "size": <int>,               // required, size of the array 
    "arrayContent": <generator>  // genrator use to create element to fill the array.
                                 // can be of any type scpecified in generator types
}
```

### Object

Generate random nested object

```JSON5
"fieldName": {
    "type": "object",                    // required
    "nullPercentage": <int>,             // optional
    "maxDistinctValue": <int>,           // optional
    "objectContent": {                   // required, list of generator used to 
       "nestedFieldName1": <generator>,  // generate the nested document 
       "nestedFieldName2": <generator>,
       ...
    }
}
```

### Binary 

Generate random binary data of length within bounds

```JSON5
"fieldName": {
    "type": "binary",           // required
    "nullPercentage": <int>,    // optional 
    "maxDistinctValue": <int>,  // optional
    "minLength": <int>,         // required,  must be >= 0 
    "maxLength": <int>          // required,  must be >= minLength
}
```

### Date 

Generate a random date (stored as [`ISODate`](https://docs.mongodb.com/manual/reference/method/Date/) ) 

`startDate` and `endDate` are string representation of a Date following RFC3339: 

**format**: "yyyy-MM-ddThh:mm:ss+00:00"


```JSON5
"fieldName": {
    "type": "date",            // required
    "nullPercentage": <int>,   // optional 
    "maxDistinctValue": <int>, // optional
    "startDate": <string>,     // required
    "endDate": <string>        // required,  must be >= startDate
}
```

### Position

Generate a random GPS position in Decimal Degrees ( WGS 84), 
eg : [40.741895, -73.989308]

```JSON5
"fieldName": {
    "type": "position",         // required
    "nullPercentage": <int>     // optional 
    "maxDistinctValue": <int>   // optional
}
```

### Constant

Add the same value to each document 

```JSON5
"fieldName": {
    "type": "constant",       // required
    "nullPercentage": <int>,  // optional
    "constVal": <object>      // required, an be of any type including object and array
                              // eg: {"k": 1, "v": "val"} 
}
```

### Autoincrement

Create an autoincremented field (type `<long>` or `<int>`)

```JSON5
"fieldName": {
    "type": "autoincrement",  // required
    "nullPercentage": <int>,  // optional
    "autoType": <string>,     // required, can be `int` or `long`
    "startLong": <long>,      // start value if autoType = long
    "startInt": <int>       // start value if autoType = int
}
```

### Ref

If a field reference an other field in an other collection, you can use a ref generator. 

generator in first collection: 

```JSON5
"fieldName":{  
    "type":"ref",               // required
    "nullPercentage": <int>,    // optional
    "maxDistinctValue": <int>,  // optional
    "id": <int>,                // required, generator id used to link
                                // field between collections
    "refContent": <generator>   // required
}
```

generator in other collections: 

```JSON5
"fieldName": {
    "type": "ref",              // required
    "nullPercentage": <int>,    // optional
    "maxDistinctValue": <int>,  // optional
    "id": <int>                 // required, same id as previous generator 
}
```

### FromArray

Randomly pick value from an array as value for the field. Currently, object in the 
array have to be of the same type 


```JSON5
"fieldName": {
    "type": "fromArray",      // required
    "nullPercentage": <int>,  // optional   
    "in": [                   // required. Can't be empty. An array of object of 
      <object>,               // any type, including object and array. 
      <object>
      ...
    ]
}
```
### CountAggregator

Count documents from `<database>.<collection>` matching a specific query. To use a 
variable of the document in the query, prefix it with "$$"

For the moment, the query can't be empty or null



```JSON5
"fieldName": {
  "type": "countAggregator", // required
  "database": <string>,      // required, db to use to perform aggregation
  "collection": <string>,    // required, collection to use to perform aggregation
  "query": <object>          // required, query that selects which documents to count in the collection 
}
```
**Example:**

Assuming that the collection `first` contains: 

```JSON5
{"_id": 1, "field1": 1, "field2": "a" }
{"_id": 2, "field1": 1, "field2": "b" }
{"_id": 3, "field1": 2, "field2": "c" }
```

and that the generator for collection `second` is: 

```JSON5
{
  "database": "test",
  "collection": "second",
  "count": 2,
  "content": {
    "_id": {
      "type": "autoincrement",
      "autoType": "int"
      "startInt": 0
    },
    "count": {
      "type": "countAggregator",
      "database": "test",
      "collection": "first",
      "query": {
        "field1": "$$_id"
      }
    }
  }
}
```

The collection `second` will contain: 

```JSON5
{"_id": 1, "count": 2}
{"_id": 2, "count": 1}
```

### ValueAggregator 

Get distinct values for a specific field for documents from 
`<database>.<collection>` matching a specific query. To use a variable of 
the document in the query, prefix it with "$$"

For the moment, the query can't be empty or null

```JSON5
"fieldName": {
  "type": "valueAggregator", // required
  "database": <string>,      // required, db to use to perform aggregation
  "collection": <string>,    // required, collection to use to perform aggregation
  "key": <string>,           // required, the field for which to return distinct values. 
  "query": <object>          // required, query that specifies the documents from which 
                             // to retrieve the distinct values
}
```

**Example**: 

Assuming that the collection `first` contains: 

```JSON5
{"_id": 1, "field1": 1, "field2": "a" }
{"_id": 2, "field1": 1, "field2": "b" }
{"_id": 3, "field1": 2, "field2": "c" }
```

and that the generator for collection `second` is: 

```JSON5
{
  "database": "test",
  "collection": "second",
  "count": 2,
  "content": {
    "_id": {
      "type": "autoincrement",
      "autoType": "int"
      "startInt": 0
    },
    "count": {
      "type": "valueAggregator",
      "database": "test",
      "collection": "first",
      "key": "field2",
      "values": {
        "field1": "$$_id"
      }
    }
  }
}
```

The collection `second` will contain: 

```JSON5
{"_id": 1, "values": ["a", "b"]}
{"_id": 2, "values": ["c"]}
```


### BoundAggregator 

Get lower ang higher values for a specific field for documents from 
`<database>.<collection>` matching a specific query. To use a variable of 
the document in the query, prefix it with "$$"

For the moment, the query can't be empty or null

```JSON5
"fieldName": {
  "type": "valueAggregator", // required
  "database": <string>,      // required, db to use to perform aggregation
  "collection": <string>,    // required, collection to use to perform aggregation
  "key": <string>,           // required, the field for which to return distinct values. 
  "query": <object>          // required, query that specifies the documents from which 
                             // to retrieve lower/higer value
}
```

**Example**: 

Assuming that the collection `first` contains: 

```JSON5
{"_id": 1, "field1": 1, "field2": "0" }
{"_id": 2, "field1": 1, "field2": "10" }
{"_id": 3, "field1": 2, "field2": "20" }
{"_id": 4, "field1": 2, "field2": "30" }
{"_id": 5, "field1": 2, "field2": "15" }
{"_id": 6, "field1": 2, "field2": "200" }
```

and that the generator for collection `second` is: 

```JSON5
{
  "database": "test",
  "collection": "second",
  "count": 2,
  "content": {
    "_id": {
      "type": "autoincrement",
      "autoType": "int"
      "startInt": 0
    },
    "count": {
      "type": "valueAggregator",
      "database": "test",
      "collection": "first",
      "key": "field2",
      "values": {
        "field1": "$$_id"
      }
    }
  }
}
```

The collection `second` will contain: 

```JSON5
{"_id": 1, "values": {"m": 0, "M": 10}}
{"_id": 2, "values": {"m": 15, "M": 200}}
```

where `m` is the min value, and `M` the max value

### Faker

Generate 'real' data using [Faker library](https://github.com/manveru/faker)

```JSON5
"fieldName": {
    "type": "faker",             // required
    "nullPercentage": <int>,     // optional
    "maxDistinctValue": <int>,   // optional
    "method": <string>           // faker method to use, for example: City / Email...
}
```

If you're building large datasets (1000000+ items) you should avoid faker generators 
and use main or custom generators instead, as faker generator are way slower. 

Currently, only `"en"` locale is available  
