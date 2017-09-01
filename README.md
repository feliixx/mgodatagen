[![Linux and macOS Build Status](https://travis-ci.org/feliixx/mgodatagen.svg?branch=master)](https://travis-ci.org/feliixx/mgodatagen)
[![Windows Build Status](https://ci.appveyor.com/api/projects/status/ll70glgrjib8x8k5/branch/master?svg=true)](https://ci.appveyor.com/project/feliixx/mgodatagen)
[![Go Report Card](https://goreportcard.com/badge/github.com/feliixx/mgodatagen)](https://goreportcard.com/report/github.com/feliixx/mgodatagen)
[![GoDoc](https://godoc.org/github.com/feliixx/mgodatagen?status.svg)](http://godoc.org/github.com/feliixx/mgodatagen)

# mgodatagen 

A small CLI tool to quickly generate millions of pseudo-random BSON documents and insert them into a Mongodb instance. Quickly test new data structure or how your application responds when your database grows! 

## Features

- Support all bson types listed in [MongoDB bson types](https://docs.mongodb.com/manual/reference/bson-types/)
- Support for index building 
- Support for sharded collection
- Support for multi-database insertion
- Cross-plateform. Tested on Unix / OSX / windows

![Demo](demo.gif)



## installation

Download the binary from [release page](https://github.com/feliixx/mgodatagen/releases)

or 

Build from source: 

First, make sure that go is installed on your machine (see [install go](https://golang.org/doc/install) for details ). Then, use `go get`:

```
go get -v "github.com/feliixx/mgodatagen"
```

## Options

Several options are available (you can see the list from `mgodatagen --help`): 

```
Usage:
  mgodatagen

configuration:
  -f, --file=<configfile>      JSON config file. This field is required
  -i, --indexonly              If present, mgodatagen will just try to rebuild index
  -s, --shortname              If present, JSON keys in the documents will be reduced
                               to the first two letters only ('name' => 'na')

connection infos:
  -h, --host=<hostname>        mongodb host to connect to (default: 127.0.0.1)
      --port=<port>            server port (default: 27017)
  -u, --username=<username>    username for authentification
  -p, --password=<password>    password for authentification

general:
      --help                   show this help message
  -v, --version                print the tool version and exit
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
   // main fields 
   "database": "test",                // required, database name
   "collection": "test",              // required, collection name
   "count": 100000,                   // required, number of document to insert in the collection 
   "content": {                       // required, the actual schema to generate documents   
     "fieldName1": <generator>,       // required
     "fieldName2": <generator>,       // required, see Generator below
     ...
   },
   // optional configuration
   // Compression level: can be : none, snappy, zlib (for WiredTiger engine only)
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

Gnerators have a common structure: 

```JSON5
"fieldName": {                 // required, field name in generated document
  "type": <string>,            // required, type of the field 
  "nullPercentage": <int>,     // optional, int between 0 and 100. Percentage of documents 
                               // using this field
  "maxDistinctValue": <int>,   // optional, maximum number of distinct values for this field
  "typeParam": ...             // parameters for this type
}
```

### Example

A set of sample config files can be found in the **samples** directory. To use it, 
make sure that you have a mongodb instance running (on 127.0.0.1:27017 for example)
and run 

```
./mgodatagen -f samples/config.json
```

This will insert 100000 random documents in collections `test` and `link` of database 
`test` with the structure defined in the config file. 


# Generator types  

List of main <generator> types: 

- [string](#string)
- [int](#int)
- [long](#long)
- [double](#double)
- [boolean](#boolean)
- [objectId](#objectid)
- [array](#array)
- [object](#object)
- [binary](#binary) 
- [date](#date) 

List of custom <generator> types: 

- [position](#position)
- [constant](#constant)
- [autoincrement](#autoincrement)
- [ref](#ref)
- [fromArray](#fromarray)

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
    "constVal": <object>      // required. Can be of any type including object and array
                              // eg: {"k": 1, "v": "val"} 
}
```

### Autoincrement

Create an autoincremented field (type <long>)

```JSON5
"fieldName": {
    "type": "autoincrement",  // required
    "nullPercentage": <int>,  // optional
    "counter": <long>         // start value 
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

Randomly pick value from an array as value for the field


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
