package datagen

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/mgocompat"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const defaultTimeout = 10 * time.Second

func connectToDB(conn *Connection, logger io.Writer) (*mongo.Client, []int, error) {

	if conn.Timeout == 0 {
		conn.Timeout = defaultTimeout
	}

	opts := createClientOptions(conn)
	fmt.Fprintf(logger, "connecting to %s", opts.GetURI())

	session, err := mongo.Connect(context.Background(), opts)
	if err != nil {
		return nil, nil, fmt.Errorf("connection failed\n  cause: %v", err)
	}

	err = session.Ping(context.Background(), readpref.Primary())
	if err != nil {
		return nil, nil, fmt.Errorf("connection failed\n  cause: %v", err)
	}

	result := session.Database("admin").RunCommand(context.Background(), bson.M{"buildInfo": 1})
	var buildInfo struct {
		Version string
	}
	err = result.Decode(&buildInfo)
	if err != nil {
		buildInfo.Version = "3.4.0"
	}
	fmt.Fprintf(logger, "\nMongoDB server version %s\n\n", buildInfo.Version)

	version := strings.Split(buildInfo.Version, ".")
	versionInt := make([]int, len(version))
	for i := range version {
		v, _ := strconv.Atoi(version[i])
		versionInt[i] = v
	}

	var shardConfig struct {
		Shards []bson.M
	}
	// if it's a sharded cluster, print the list of shards. Don't bother with the error
	// if cluster is not sharded / user not allowed to run command against admin db
	result = session.Database("admin").RunCommand(context.Background(), bson.M{"listShards": 1})
	err = result.Decode(&shardConfig)
	if err == nil && result.Err() == nil {

		// starting in MongoDB 5.0, topology time appears in shard list. Remove
		// it for tests
		for i := range shardConfig.Shards {
			delete(shardConfig.Shards[i], "topologyTime")
		}

		shardList, err := json.MarshalIndent(shardConfig.Shards, "", "  ")
		if err == nil {
			fmt.Fprintf(logger, "shard list: %v\n", string(shardList))
		}
	}
	return session, versionInt, nil
}

func createClientOptions(conn *Connection) *options.ClientOptions {

	connOpts := options.Client().
		ApplyURI(fmt.Sprintf("mongodb://%s:%s", conn.Host, conn.Port)).
		SetConnectTimeout(conn.Timeout).
		SetServerSelectionTimeout(conn.Timeout).
		SetRetryWrites(false) // this is only needed for sharded cluster, it default to false on standalone instance

	if conn.URI != "" {
		connOpts.ApplyURI(conn.URI)
		return connOpts // return to avoid UserName / Password / AuthMechanism is set
	}
	if conn.UserName == "" && conn.Password == "" && conn.AuthMechanism == "" {
		return connOpts
	}

	var credentials options.Credential
	if conn.UserName != "" && conn.Password != "" {
		credentials.Username = conn.UserName
		credentials.Password = conn.Password
	}
	if conn.AuthMechanism != "" {
		credentials.AuthMechanism = conn.AuthMechanism
	}

	if conn.TLSCAFile != "" || conn.TLSCertKeyFile != "" {
		connOpts.ApplyURI(fmt.Sprintf("mongodb://%s:%s/?tlsCAFile=%s&tlsCertificateKeyFile=%s", conn.Host, conn.Port, conn.TLSCAFile, conn.TLSCertKeyFile))
	}

	return connOpts.SetAuth(credentials)
}

func runMgoCompatCommand(ctx context.Context, session *mongo.Client, db string, cmd interface{}) error {
	// With the default registry, index.Collation is kept event when it's empty,
	// and it make the command fail
	// to fix this, marshal the command to a bson.Raw with the mgocompat registry
	// providing the same behavior that the old mgo driver
	mgoRegistry := mgocompat.NewRespectNilValuesRegistryBuilder().Build()
	_, cmdBytes, err := bson.MarshalValueWithRegistry(mgoRegistry, cmd)
	if err != nil {
		return fmt.Errorf("fait to generate mgocompat command\n  cause: %v", err)
	}
	return session.Database(db).RunCommand(ctx, cmdBytes).Err()
}
