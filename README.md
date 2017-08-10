[![Linux and macOS Build Status](https://travis-ci.org/feliixx/mgodatagen.svg?branch=master)](https://travis-ci.org/feliixx/mgodatagen)
[![Windows Build Status](https://ci.appveyor.com/api/projects/status/ll70glgrjib8x8k5/branch/master?svg=true)](https://ci.appveyor.com/project/feliixx/mgodatagen)
[![Go Report Card](https://goreportcard.com/badge/github.com/feliixx/mgodatagen)](https://goreportcard.com/report/github.com/feliixx/mgodatagen)

# mgodatagen 

A small CLI tool to quickly generate millions of pseudo-random BSON documents and insert them into a Mongodb instance. Test how your application responds when your database grows

## Features

- Support all bson types listed in [MongoDB bson types](https://docs.mongodb.com/manual/reference/bson-types/)
- Support for index building 
- Support for sharded collection
- Support for multi-database insertion
- Cross-plateform. Tested on Unix / OSX / windows

## installation

Download the binary from [release page](https://github.com/feliixx/mgodatagen/releases)

or 

Build from source: 

First, make sure that go is installed on your machine (see [install go](https://golang.org/doc/install) for details ). Then, clone the repository and run `go build` :

```
git clone https://github.com/feliixx/mgodatagen.git
cd mgodatagen
go build 
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

```
[{  
   "database": "test",                // required 
   "name":"test",                     // required
   "count":100000,                    // required, number of document to insert in the collection 
   "compressionLevel": <string>,      // optional, for WT engine only: none, snappy, zlib, default: snappy

   "shardConfig": {                   // optional, configuration if the collection has to be sharded 
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
   "indexes": [                       // optional, list of index to build  
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
   ],
   "content": {                       // required   
   	  "fieldName1": <generator>,      // required
   	  "fieldName2": <generator>,      // required
   	  ...
   }
}]
```

Gnerators have a common structure: 

```
"fieldName": {                 // required, field name in generated document
  "type": <string>,            // required, type of the field 
  "nullPercentage": <int>,     // optional, int between 0 and 100. Percentage of documents 
                               // using this field
  "typeParam": ...             // parameters for this type
}
```

A set of sample config files can be found in the **samples** directory 
# Generator types  

list main available <generator> types: 

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

list og custom types: 

- [position](#position)
- [constant](#constant)
- [autoincrement](#autoincrement)
- [ref](#ref)
- [fromArray](#fromarray)

### String

Generate random string of a certain length. String is composed of char within this list: 
`abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_`

```
"fieldName": {
    "type": "string",        // required
    "nullPercentage": <int>, // optional 
    "minLength": <int>,      // required,  must be >= 0 
    "maxLength": <int>       // required,  must be >= minLength
}
```

### Int 

Generate random int within bounds. 

```
"fieldName": {
    "type": "int",           // required
    "nullPercentage": <int>, // optional 
    "minInt": <int>,         // required
    "maxInt": <int>          // required, must be >= minInt
}
```

### Long 

Generate random long within bounds. 

```
"fieldName": {
    "type": "long",          // required
    "nullPercentage": <int>, // optional 
    "minLong": <long>,       // required
    "maxLong": <long>        // required, must be >= minLong
}
```

### Double

Generate random double within bounds. 

```
"fieldName": {
    "type": "double",        // required
    "nullPercentage": <int>, // optional 
    "minDouble": <double>,   // required
    "maxDouble": <double>    // required, must be >= minDouble
}
```

### Boolean

Generate random boolean

```
"fieldName": {
    "type": "boolean",       // required
    "nullPercentage": <int>, // optional 
}
```

### ObjectId

Generate random and unique objectId

```
"fieldName": {
    "type": "objectId",      // required
    "nullPercentage": <int>, // optional 
}
```

### Array

Generate a random array of bson object 

```
"fieldName": {
    "type": "array",             // required
    "nullPercentage": <int>,     // optional
    "size": <int>,               // required, size of the array 
    "arrayContent": <generator>  // genrator use to create element to fill the array.
                                 // can be of any type scpecified in generator types
}
```

### Object

Generate random nested object

```
"fieldName": {
    "type": "object",                    // required
    "nullPercentage": <int>,             // optional
    "objectContent": {                   // required, list of generator used to 
       "nestedFieldName1": <generator>,  // generate the nested document 
       "nestedFieldName2": <generator>,
       ...
    }
}
```

### Binary 

Generate random binary data of length within bounds

```
"fieldName": {
    "type": "binary",        // required
    "nullPercentage": <int>, // optional 
    "minLength": <int>,      // required,  must be >= 0 
    "maxLength": <int>       // required,  must be >= minLength
}
```

### Date 

Generate a random date (stored as [`ISODate`](https://docs.mongodb.com/manual/reference/method/Date/) ) 

`startDate` and `endDate` are string representation of a Date following RFC3339: 

**format**: "yyyy-MM-ddThh:mm:ss+00:00"


```
"fieldName": {
    "type": "date",          // required
    "nullPercentage": <int>, // optional 
    "startDate": <string>,   // required
    "endDate": <string>      // required,  must be >= startDate
}
```

### Position

Generate a random GPS position in Decimal Degrees ( WGS 84), 
eg : [40.741895, -73.989308]

```
"fieldName": {
    "type": "position",         // required
    "nullPercentage": <int>     // optional 
}
```

### Constant

Add the same value to each document 

```
"fieldName": {
    "type": "constant",       // required
    "nullPercentage": <int>,  // optional
    "constVal": <object>      // required. Can be of any type including object and array
                              // eg: {"k": 1, "v": "val"} 
}
```

### Autoincrement

Create an autoincremented field (type <long>)

```
"fieldName": {
    "type": "autoincrement",  // required
    "nullPercentage": <int>,  // optional
    "counter": <long>         // start value 
}
```

### Ref

If a field reference an other field in an other collection, you can use a ref generator. 

generator in first collection: 

```
"fieldName":{  
    "type":"ref",               // required
    "nullPercentage": <int>,    // optional
    "id": <int>,                // required, generator id used to link
                                // field between collections
    "refContent": <generator>   // required
}
```

generator in other collections: 

```
"fieldName": {
    "type": "ref",            // required
    "nullPercentage": <int>,  // optional
    "id": <int>               // required, same id as previous generator 
}
```

### FromArray

Randomly pick value from an array as value for the field


```
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
