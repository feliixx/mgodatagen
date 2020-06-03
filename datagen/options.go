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
	ConfigFile      string `short:"f" long:"file" value-name:"<configfile>" description:"JSON config file. This field is required"`
	IndexOnly       bool   `short:"i" long:"indexonly" description:"if present, mgodatagen will just try to rebuild index"`
	IndexFirst      bool   `short:"x" long:"indexfirst" description:"if present, mgodatagen will create index before\n inserting documents"`
	Append          bool   `short:"a" long:"append" description:"if present, append documents to the collection without\n removing older documents or deleting the collection"`
	NumInsertWorker int    `short:"n" long:"numWorker" value-name:"<nb>" description:"number of concurrent workers inserting documents\n in database. Default is number of CPU"`
	BatchSize       int    `short:"b" long:"batchsize" value-name:"<size>" description:"bulk insert batch size" default:"1000"`
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
