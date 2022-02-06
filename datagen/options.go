package datagen

import "time"

// General struct that stores global options from command line args
type General struct {
	Help    bool `long:"help" description:"show this help message"`
	Version bool `short:"v" long:"version" description:"print the tool version and exit"`
	Quiet   bool `short:"q" long:"quiet" description:"quieter output"`
}

// Connection struct that stores info on connection from command line args
type Connection struct {
	URI            string `long:"uri" value-name:"<uri>" description:"connection string URI. If present, takes precedence over all\nother options. For detail on URI format, see\n https://docs.mongodb.com/manual/reference/connection-string/"`
	Host           string `short:"h" long:"host" value-name:"<hostname>" description:"mongodb host to connect to" default:"127.0.0.1"`
	Port           string `long:"port" value-name:"<port>" description:"server port" default:"27017"`
	UserName       string `short:"u" long:"username" value-name:"<username>" description:"username for authentification"`
	Password       string `short:"p" long:"password" value-name:"<password>" description:"password for authentification"`
	AuthMechanism  string `long:"authenticationMechanism" value-name:"<mechanism>" description:"authentication mechanism\n for now only PLAIN and MONGODB-X509 are supported"`
	TLSCertKeyFile string `long:"tlsCertificateKeyFile" value-name:"<path>" description:"PEM certificate/key file for TLS"`
	TLSCAFile      string `long:"tlsCAFile" value-name:"<path>" description:"Certificate Authority file for TLS"`
	Timeout        time.Duration
}

// Configuration struct that stores info on config file from command line args
type Configuration struct {
	ConfigFile      string `short:"f" long:"file" value-name:"<configfile>" description:"JSON config file. This field is required\n"`
	Append          bool   `short:"a" long:"append" description:"if present, append documents to the collection without\n removing older documents or deleting the collection"`
	IndexOnly       bool   `short:"i" long:"indexonly" description:"if present, mgodatagen will just try to rebuild index"`
	IndexFirst      bool   `short:"x" long:"indexfirst" description:"if present, mgodatagen will create index before\n inserting documents"`
	NumInsertWorker int    `short:"n" long:"numWorker" value-name:"<nb>" description:"number of concurrent workers inserting documents\n in database. Default is number of CPU"`
	BatchSize       int    `short:"b" long:"batchsize" value-name:"<size>" description:"bulk insert batch size" default:"1000"`
	Seed            uint64 `short:"s" long:"seed" value-name:"<seed>" description:"specific seed to use. Passing the same seed garentees\n the same output for evey run with the same config.\n Has to be in [1, 18446744073709551615]"`
	Output          string `short:"o" long:"output" value-name:"<output>" description:"where documents should be written. Options are:\n - mongodb (default)\n - stdout\n - filename"`
	PrettyPrint     bool   `long:"prettyprint" description:"if present, indent the output. Only for stdout or file\n output"`
}

// Template struct that stores info on config file to generate
type Template struct {
	New string `long:"new" value-name:"<filename>" description:"create an empty configuration file"`
}

// Options struct to store flags from CLI
type Options struct {
	Template      `group:"template"`
	Configuration `group:"configuration"`
	Connection    `group:"connection infos"`
	General       `group:"general"`
}
