package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go"
	tc "github.com/anonymouse64/websockets-mqtt-visualizer/tomlconfigurator"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/eknkc/basex"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	flags "github.com/jessevdk/go-flags"
	toml "github.com/pelletier/go-toml"
)

// allowEverything is a simple helper function for development to allow
// disabling the default websockets behavior of disallowing upgrading http
// requests to websockets if the Host and the Origin server don't match
func allowEverything(*http.Request) bool {
	return true
}

var upgrader = websocket.Upgrader{}

const maxInitialConnectTries = 10

type mqttConfig struct {
	Port              int    `toml:"port"`
	Scheme            string `toml:"scheme"`
	Host              string `toml:"host"`
	Path              string `toml:"path"`
	ClientCertFile    string `toml:"clientcert"`
	ClientKeyFile     string `toml:"clientkey"`
	ClientUsername    string `toml:"username"`
	ClientPassword    string `toml:"password"`
	ServerCertPEMFile string `toml:"serverpemcert"`
	ClientID          string `toml:"clientid"`
	TopicQoS          int    `toml:"topicqos"`
	Topic             string `toml:"topic"`
}

// for configuring the graph for a data source
type dataGraphConfig struct {
	Label     string `toml:"label"`
	AxisLabel string `toml:"axislabel"`
	Key       string `toml:"key"`
}

type websocketsConfig struct {
	Port                 int             `toml:"port"`
	Host                 string          `toml:"host"`
	Path                 string          `toml:"path"`
	HTMLPath             string          `toml:"htmlpath"`
	DisableCheckOrigin   bool            `toml:"checkorigin"`
	WebSocketsInsecureJS bool            `toml:"insecurews"`
	RightData            dataGraphConfig `toml:"rightdata"`
	LeftData             dataGraphConfig `toml:"leftdata"`
	GraphLabel           string          `toml:"graphlabel"`
}

// for configuring azure amqp event hub data source
type azureConfig struct {
	ConnectionString string `toml:"connstring"`
}

// ServerConfig holds all of the config values
type ServerConfig struct {
	WebSocketsConfig websocketsConfig `toml:"websockets"`
	MQTTConfig       mqttConfig       `toml:"mqtt"`
	AzureAMQPConfig  azureConfig      `toml:"azureamqp"`
	ServerSources    []string         `toml:"sources"`
}

// Config is the current server config
var Config *ServerConfig

// validMQTTScheme checks if the scheme is supported/valid
func validMQTTScheme(scheme string) bool {
	mqttSchemes := map[string]bool{
		"tcps": true,
		"tcp":  true,
		"tls":  true,
		"ssl":  true,
		"":     true,
	}
	ok, _ := mqttSchemes[scheme]
	return ok
}

// validServerSource checks if the source is supported/valid
func validServerSource(source string) bool {
	serverSources := map[string]bool{
		"mqtt":      true,
		"azureamqp": true,
		"":          true,
	}
	ok, _ := serverSources[source]
	return ok
}

// trueForAll is a map function - returns true only if the func applied to all
// elements of the array returns true
// it implements short-circuiting, returning false on the first instance of
// checker returning false
func trueForAll(checker func(string) bool, checkees []string) bool {
	for _, checkee := range checkees {
		if !checker(checkee) {
			return false
		}
	}
	return true
}

// sliceContainsString is a helper function to check if a given string is in a
// list of strings
func sliceContainsString(slice []string, theString string) bool {
	for _, s := range slice {
		if s == theString {
			return true
		}
	}
	return false
}

// Validate checks various properties in the config to make sure they're correct
func (s *ServerConfig) Validate() error {
	switch {
	case !trueForAll(validServerSource, s.ServerSources):
		// TODO: get specific failure here for better UI
		return fmt.Errorf("server sources has invalid element in %v", s.ServerSources)
	// check that ports are greater than 0
	case s.WebSocketsConfig.Port < 1:
		return fmt.Errorf("http port %d is invalid", s.WebSocketsConfig.Port)
	case sliceContainsString(s.ServerSources, "mqtt") && s.MQTTConfig.Port < 1:
		return fmt.Errorf("mqtt port %d is invalid", s.MQTTConfig.Port)
	case sliceContainsString(s.ServerSources, "mqtt") && !validMQTTScheme(s.MQTTConfig.Scheme):
		return fmt.Errorf("mqtt scheme %s is invalid", s.MQTTConfig.Scheme)
	case sliceContainsString(s.ServerSources, "azureamqp") && s.AzureAMQPConfig.ConnectionString == "":
		return fmt.Errorf("azure event hub connection string must be specified to use with source \"azureamqp\"")
	default:
		return nil
	}
}

// MarshalTOML marshals the config into bytes
func (s *ServerConfig) MarshalTOML() ([]byte, error) {
	return toml.Marshal(*s)
}

// UnmarshalTOML unmarshals the toml bytes into the config
func (s *ServerConfig) UnmarshalTOML(bytes []byte) error {
	return toml.Unmarshal(bytes, s)
}

// SetDefault sets default values for the config - just use a localhost
// mqtt broker
func (s *ServerConfig) SetDefault() error {
	s.WebSocketsConfig.Port = 3000
	s.WebSocketsConfig.Host = "0.0.0.0"
	s.WebSocketsConfig.Path = "/"
	s.WebSocketsConfig.RightData = dataGraphConfig{
		Label:     "Ambient Humidity",
		AxisLabel: "Humidity (%)",
		Key:       "ambient.humidity",
	}
	s.WebSocketsConfig.LeftData = dataGraphConfig{
		Label:     "Ambient Temperature",
		AxisLabel: "Temperature (C)",
		Key:       "ambient.temperature",
	}
	s.MQTTConfig.Port = 1883
	s.MQTTConfig.Host = "localhost"
	s.MQTTConfig.Topic = "my/topic"
	s.MQTTConfig.TopicQoS = 1
	s.ServerSources = []string{"mqtt"}
	return nil
}

func init() {
	// initialize a default config here globally because then it simplifies
	// the various command Execute() methods
	Config = &ServerConfig{}
	Config.SetDefault()
}

// Command is the command for application management
type Command struct {
	Start        StartCmd  `command:"start" description:"Start the server"`
	Config       ConfigCmd `command:"config" description:"Change or get config values"`
	ConfigFile   string    `short:"c" long:"config-file" description:"Configuration file to use" required:"yes"`
	DebugLogging bool      `short:"d" long:"debug" description:"Turn on debug logging"`
}

// The current input command
var currentCmd Command

// ConfigCmd is for a set of commands working with the config file
// programmatically
type ConfigCmd struct {
	Check      CheckConfigCmd  `command:"check" description:"Check a configuration file"`
	SnapUpdate UpdateConfigCmd `command:"update" description:"Update the configuration"`
	Set        SetConfigCmd    `command:"set" description:"Set values in the configuration file"`
	Get        GetConfigCmd    `command:"get" description:"Get values from the configuration file"`
}

// UpdateConfigCmd is a command for updating a config file from snapd/snapctl
// environment values
type UpdateConfigCmd struct{}

// Execute of UpdateConfigCmd will update a config file using values from
// snapd / snapctl
func (cmd *UpdateConfigCmd) Execute(args []string) error {
	err := tc.LoadTomlConfigurator(currentCmd.ConfigFile, Config)
	if err != nil {
		return err
	}

	// Get all keys of the toml
	keys, err := tc.TomlKeys(Config)
	if err != nil {
		return err
	}

	// Get all the values of these keys from snapd
	snapValues, err := getSnapKeyValues(keys)
	if err != nil {
		return err
	}

	// Write the values into the config
	err = tc.SetTomlConfiguratorKeyValues(snapValues, Config)
	if err != nil {
		return err
	}

	// Finally write out the config to the config file file
	return tc.WriteTomlConfigurator(currentCmd.ConfigFile, Config)
}

// getSnapKeyValues queries snapctl for all key values at once as JSON, and
// returns the corresponding values
func getSnapKeyValues(keys []string) (map[string]interface{}, error) {
	// get all values from snap at once as a json document
	snapCmd := exec.Command("snapctl", append([]string{"get", "-d"}, keys...)...)
	out, err := snapCmd.CombinedOutput()
	if err != nil {
		return nil, err
	}

	// Unmarshal the json into the map, and return it
	returnMap := make(map[string]interface{})
	err = json.Unmarshal(out, &returnMap)
	if err != nil {
		return nil, err
	}

	return returnMap, nil
}

// SetConfigCmd is a command for setting config values in the config file
type SetConfigCmd struct {
	Args struct {
		Key   string `positional-arg-name:"key"`
		Value string `positional-arg-name:"value"`
	} `positional-args:"yes" required:"yes"`
}

// Execute of SetConfigCmd will set config values from the command line inside
//  the config file
func (cmd *SetConfigCmd) Execute(args []string) error {
	var val interface{}
	// assume the value is a single valid json value to parse it
	err := json.Unmarshal([]byte(cmd.Args.Value), &val)
	if err != nil {
		return err
	}

	// load the toml configuration so we can manipulate it
	err = tc.LoadTomlConfigurator(currentCmd.ConfigFile, Config)
	if err != nil {
		return err
	}

	// try to set the value into the toml file using the key
	err = tc.SetTomlConfiguratorKeyVal(Config, cmd.Args.Key, val)
	if err != nil {
		return err
	}

	// finally write the configuration back out to the file
	return tc.WriteTomlConfigurator(currentCmd.ConfigFile, Config)
}

// GetConfigCmd is a command for getting config values from the config file
type GetConfigCmd struct {
	Args struct {
		Key string `positional-arg-name:"key"`
	} `positional-args:"yes" required:"yes"`
}

// Execute of GetConfigCmd will print off config values from the command line
// as specified in the config file
func (cmd *GetConfigCmd) Execute(args []string) (err error) {
	// load the toml configuration so we can manipulate it
	err = tc.LoadTomlConfigurator(currentCmd.ConfigFile, Config)
	if err != nil {
		return err
	}

	// try to set the value into the toml file using the key
	val, err := tc.GetTomlConfiguratorKeyVal(Config, cmd.Args.Key)
	if err != nil {
		return err
	}

	// Get the key from the tree
	fmt.Println(val)
	return
}

// CheckConfigCmd is a command for verifying a config file is valid and optionally
// creating a new one if the specified config file doesn't exist
type CheckConfigCmd struct {
	WriteNewFile bool `short:"w" long:"write-new" description:"Whether to write a new config if the specified file doesn't exist"`
}

// Execute of CheckConfigCmd checks if the specified config file exists,
// and if it doesn't and "write-new" is set, creates a default config file
func (cmd *CheckConfigCmd) Execute(args []string) (err error) {
	// first check if the specified file exists
	if _, err = os.Stat(currentCmd.ConfigFile); os.IsNotExist(err) {
		// file doesn't exist
		if cmd.WriteNewFile {
			// write out a new file
			return tc.WriteTomlConfigurator(currentCmd.ConfigFile, Config)
		}
		return fmt.Errorf(
			"config file %s doesn't exist",
			currentCmd.ConfigFile,
		)
	}
	// otherwise the file exists, so load it to check it
	return tc.LoadTomlConfigurator(currentCmd.ConfigFile, Config)
}

// StartCmd command for creating an application
type StartCmd struct{}

// command parser
var parser = flags.NewParser(&currentCmd, flags.Default)

// empty - the command execution happens in *.Execute methods
func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	_, err := parser.Parse()
	if err != nil {
		os.Exit(1)
	}
}

// Execute of StartCmd will start running the web server
func (cmd *StartCmd) Execute(args []string) (err error) {
	// load the configuration for the server
	err = tc.LoadTomlConfigurator(currentCmd.ConfigFile, Config)
	if err != nil {
		return err
	}

	if currentCmd.DebugLogging {
		log.Println("debug logging turned on")
	}

	// mqtt logs for debugging the mqtt connections
	// MQTT.DEBUG = log.New(os.Stderr, "DEBUG    ", log.Ltime)
	// MQTT.WARN = log.New(os.Stderr, "WARNING  ", log.Ltime)
	// MQTT.CRITICAL = log.New(os.Stderr, "CRITICAL ", log.Ltime)
	// MQTT.ERROR = log.New(os.Stderr, "ERROR    ", log.Ltime)

	// make an internal broker to pass messages from the sources go routines to
	// all of the http/websockets go routines
	channelBroker := NewBroker()
	go channelBroker.Start(context.Background())

	// figure out which sources we need to forward from and setup the corresponding
	// publisher go routines
	// fmt.Println(Config.ServerSources)
	// fmt.Println(Config.AzureAMQPConfig)
	for _, source := range Config.ServerSources {
		if !validServerSource(source) {
			log.Fatalf("invalid server source %s", source)
		}
		switch source {
		case "azureamqp":
			// TODO: add more connection configurations options to create the
			// hub connection in more generic ways
			hub, err := eventhub.NewHubFromConnectionString(Config.AzureAMQPConfig.ConnectionString)
			if err != nil {
				log.Fatalf("error connecting to hub with connection string: %s\n", err)
			}

			if currentCmd.DebugLogging {
				log.Println("created aqmp hub connection")
			}

			// TODO: use better contexts appropriately here

			// TODO: allow configuration of partitions from the config file
			// for now just try to listen on all partitions
			// get the partitions IDs from the hub's runtime info
			runtimeInfo, err := hub.GetRuntimeInformation(context.Background())
			if err != nil {
				log.Fatalf("error getting runtime info for hub %v: %s\n", hub, err)
			}

			// receive messages on each partition in the background, with
			// one go routine per partition
			for _, partitionID := range runtimeInfo.PartitionIDs {
				go func() {
					// from the Receive docs:
					// If Receive encounters an initial error setting up the connection, an error will be returned.
					// as such, if err here is non-nil, we kill the server
					// immediately
					// as it means we were unable to setup the connection
					// TODO: provide a context here that lets us handle this
					// connection getting disconnected/failing after initial
					// setup so it can be recovered from
					listenHandler, err := hub.Receive(
						context.Background(),
						partitionID,
						makeAzureAMQPMessageBroadcastFunc(channelBroker, partitionID),
						eventhub.ReceiveWithLatestOffset(),
					)
					if err != nil {
						log.Fatalf("error initially receiving from event hub: %v", err)
					}
					// err was nil, so now we wait for the listenHandler to be
					// done, which will only ever happen if an internal error
					// happens after the initial connection was setup
					// as such, this error isn't immediately fatal for the server
					// and we leave open the possibility for this connection
					// to be re-setup, etc. later on using the contexts
					select {
					case <-listenHandler.Done():
						log.Printf("listenhandler for azure amqp connection partition %s failed: %v\n", partitionID, listenHandler.Err())
					}
				}()
			}
		case "mqtt":
			// build an mqtt client out of the configuration
			client, err := buildMQTTClient(Config.MQTTConfig)
			if err != nil {
				log.Fatalf("failed to build mqtt client: %s\n", err)
			}
			if currentCmd.DebugLogging {
				log.Println("mqtt client initialized")
			}

			// make an mqtt connection to the broker - trying every 3 seconds, and
			// giving up and dying after 10 unsuccessful tries
			initialConnectTries := 0
			for {
				if token := client.Connect(); token.Wait() && token.Error() != nil {
					log.Printf("couldn't connect to broker: %v\n", token.Error())
					log.Println("sleeping for 3 seconds")
					time.Sleep(time.Second * 3)
					initialConnectTries++
					if initialConnectTries == maxInitialConnectTries {
						log.Fatalf("failed to connect to broker after %d tries\n", initialConnectTries)
					}
				} else {
					// connected successfully
					break
				}
			}
			if currentCmd.DebugLogging {
				log.Println("mqtt client connected")
			}

			// subscribe to the specified mqtt topic with a message handler
			// tied to the created broker
			if token := client.Subscribe(Config.MQTTConfig.Topic,
				byte(Config.MQTTConfig.TopicQoS),
				makeMQTTMessageBroadcastFunc(channelBroker),
			); token.Wait() && token.Error() != nil {
				log.Fatalf(
					"couldn't subscribe to topic %s: %v\n",
					Config.MQTTConfig.Topic,
					token.Error(),
				)
			}

			if currentCmd.DebugLogging {
				log.Printf("mqtt client subscribed to %s\n", Config.MQTTConfig.Topic)
				log.Printf("http server listening on %s:%d\n", Config.WebSocketsConfig.Host, Config.WebSocketsConfig.Port)
			}
		}
	}

	// if we are supposed to disable checking the origin to ensure that
	// http connections to the websockets endpoint have the same Host as
	// Origin headers, then set that up now
	if Config.WebSocketsConfig.DisableCheckOrigin {
		upgrader.CheckOrigin = allowEverything
	}

	// setup the handler function to forward all source messages to the
	// websockets http clients
	http.HandleFunc(Config.WebSocketsConfig.Path, makeSourceForwardMessageFunc(channelBroker))

	// handle the index.js file specifically so that we can generate the
	// template of it
	http.HandleFunc("/javascripts/index.js", serveJSTemplate)
	// all other files are statically generated
	http.Handle("/", http.FileServer(http.Dir(Config.WebSocketsConfig.HTMLPath)))

	// listen on the configured host and port
	return http.ListenAndServe(
		fmt.Sprintf("%s:%d", Config.WebSocketsConfig.Host, Config.WebSocketsConfig.Port),
		nil,
	)
}

// a simple struct for templating the javascript file which plots the data
// in a graph
type tmplStruct struct {
	WebsocketsScheme string
	RightJSKey       string
	LeftJSKey        string
	RightLabel       string
	LeftLabel        string
	RightAxisLabel   string
	LeftAxisLabel    string
	GraphLabel       string
}

// serveJSTemplate reads the index.js file as a template and generates specific
// javascript values for the graph handling using the config file
func serveJSTemplate(w http.ResponseWriter, r *http.Request) {
	fp := filepath.Join("templates", filepath.Clean(r.URL.Path))
	// Return a 404 if the template doesn't exist
	info, err := os.Stat(fp)
	if err != nil {
		if os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}
	}

	// Return a 404 if the request is for a directory
	if info.IsDir() {
		http.NotFound(w, r)
		return
	}

	tmpl, err := template.ParseFiles(fp)
	if err != nil {
		// Log the detailed error
		log.Println(err.Error())
		// Return a generic "Internal Server Error" message
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// copy config settings into the struct
	tmplData := tmplStruct{
		RightJSKey:     Config.WebSocketsConfig.RightData.Key,
		LeftJSKey:      Config.WebSocketsConfig.LeftData.Key,
		RightLabel:     Config.WebSocketsConfig.RightData.Label,
		LeftLabel:      Config.WebSocketsConfig.LeftData.Label,
		RightAxisLabel: Config.WebSocketsConfig.RightData.AxisLabel,
		LeftAxisLabel:  Config.WebSocketsConfig.LeftData.AxisLabel,
		GraphLabel:     Config.WebSocketsConfig.GraphLabel,
	}

	if Config.WebSocketsConfig.WebSocketsInsecureJS {
		tmplData.WebsocketsScheme = "ws://"
	} else {
		tmplData.WebsocketsScheme = "wss://"
	}

	if err := tmpl.Execute(w, tmplData); err != nil {
		log.Println(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// makeSourceForwardMessageFunc returns a lambda function which uses the broker
// to create a new subscription channel for every http request, and thus
// receives all source messages and forwards them to the http client (which is
// upgraded to a websockets client)
func makeSourceForwardMessageFunc(msgBroker *Broker) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// upgrade the HTTP request to a websockets connection
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Print("upgrade:", err)
			return
		}
		defer c.Close()

		// get a subscription channel
		subChannel, err := msgBroker.Subscribe(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Printf("error subscribing to internal broadcaster: %s\n", err)
			return
		}

		// forward any messages from the channel and send on the
		// websockets connection, assuming JSON content and adding a current
		// timestamp
		for msg := range subChannel {
			if msgPayload, ok := msg.([]byte); ok {
				// get the message payload as JSON
				msgObj := make(map[string]interface{})
				err = json.Unmarshal(msgPayload, &msgObj)
				if err != nil {
					// invalid message, drop it
					log.Printf("error parsing json message: %v\n", err)
					continue
				}

				// add the current time to the object and then wrap it back
				// up into a json string payload
				msgObj["time"] = time.Now().Format(time.RFC3339)
				msgBytes, err := json.Marshal(msgObj)
				if err != nil {
					// invalid message, drop it
					log.Printf("error building json message: %v\n", err)
					continue
				}

				// write the message to the websockets client
				err = c.WriteMessage(websocket.TextMessage, msgBytes)
				if err != nil {
					log.Printf("error writing message %s\n", err)
					break
				}
			} else {
				log.Printf("error, invalid message sent on internal broker channel: %v\n", msg)
			}
		}
	}
}

// makeMQTTMessageBroadcastFunc returns a lambda function which publishes all mqtt messages
// received to the broker
func makeMQTTMessageBroadcastFunc(channelBroadcaster *Broker) MQTT.MessageHandler {
	return func(client MQTT.Client, msg MQTT.Message) {
		// TODO: figure out how to get a context here
		channelBroadcaster.Publish(context.TODO(), msg.Payload())
		if currentCmd.DebugLogging {
			opts := client.OptionsReader()
			log.Printf("client %s got message %s\n", opts.ClientID(), string(msg.Payload()))
		}
	}
}

// simple handler that publishes the Data from the event on
// the internal broadcaster channel to all http/websockets clients
func makeAzureAMQPMessageBroadcastFunc(channelBroadcaster *Broker, partitionID string) func(context.Context, *eventhub.Event) error {
	return func(c context.Context, event *eventhub.Event) error {
		channelBroadcaster.Publish(c, event.Data)
		if currentCmd.DebugLogging {
			log.Printf("amqp client got message on partition %s of %s\n", partitionID, string(event.Data))
		}
		return nil
	}
}

// generateRandomClientID generates a random clientID based on a UUID, encoded
// in base62 to always be less 23 characters or less to be MQTT 3.1.1 compliant
func generateRandomClientID() string {
	uuidBytes := uuid.New()
	encoder, err := basex.NewEncoding("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	if err != nil {
		// if this fails either "basex" is broken or something else is
		// critically broken
		panic("can't build base62 encoder for random client ID's")
	}
	return encoder.Encode(uuidBytes[:])
}

func buildMQTTClient(config mqttConfig) (MQTT.Client, error) {
	// Make an options struct for the mqtt client
	connOpts := MQTT.NewClientOptions()

	// always use a clean session
	connOpts.SetCleanSession(true)

	// always reconnect
	connOpts.SetAutoReconnect(true)

	conf := &tls.Config{}
	mqttScheme := config.Scheme

	// build the URL - first we need to check if the configured scheme is
	// the empty string, in which case we assume insecure
	switch config.Scheme {
	case "":
		fallthrough
	case "tcp":
		// insecure mode - don't try to use use any tls certificates
		mqttScheme = "tcp"
	case "tls":
		fallthrough
	case "tcps":
		fallthrough
	case "ssl":
		// secure mode - ensure that we always perform host verification
		conf.InsecureSkipVerify = false

		// load server/client certificate if they are specified and exist
		if config.ClientCertFile != "" || config.ClientKeyFile != "" {
			// is non-empty string so assume it is supposed to exist
			// get the client certificate pair and load them
			clientCert, err := tls.LoadX509KeyPair(config.ClientCertFile, config.ClientKeyFile)
			if err != nil {
				return nil, fmt.Errorf(
					"couldn't make client cert from certfile %s and keyfile %s: %v",
					config.ClientCertFile,
					config.ClientKeyFile,
					err,
				)
			}

			// add the client certificate to the tls configuration
			conf.Certificates = []tls.Certificate{clientCert}
		}

		// check if there's an additional server cert to use for verification
		if config.ServerCertPEMFile != "" {
			brokerCACert, err := ioutil.ReadFile(config.ServerCertPEMFile)
			if err != nil {
				return nil, fmt.Errorf("couldn't open broker ca cert file at %s: %v", config.ServerCertPEMFile, err)
			}

			certPool := x509.NewCertPool()
			certPool.AppendCertsFromPEM(brokerCACert)
			conf.RootCAs = certPool
		}

		connOpts.SetTLSConfig(conf)
	default:
		// somehow got an invalid tls configuration
		return nil, fmt.Errorf("invalid mqtt scheme %s", config.Scheme)
	}

	// set the username + password if it's set in the config
	if config.ClientUsername != "" {
		connOpts.SetUsername(config.ClientUsername)
		if config.ClientPassword != "" {
			connOpts.SetPassword(config.ClientPassword)
		}
	}

	// build the broker URL and add that
	connOpts.AddBroker(fmt.Sprintf("%s://%s:%d%s",
		mqttScheme,
		config.Host,
		config.Port,
		config.Path,
	))

	// if the client ID for the MQTT client is empty, then generate a random
	// one
	if config.ClientID == "" {
		connOpts.SetClientID(generateRandomClientID())
	} else {
		connOpts.SetClientID(config.ClientID)
	}

	// create the client
	return MQTT.NewClient(connOpts), nil
}

// most of this code is adapted from https://stackoverflow.com/a/49877632/10102404

// Broker is an object for maintaining subscriptions such that multiple
// clients can simultaneously listen for messages on a channel safely
type Broker struct {
	stopCh    chan struct{}
	publishCh chan interface{}
	subCh     chan chan interface{}
	unsubCh   chan chan interface{}
}

// NewBroker returns a pointer to a new broker initialized with suitable
// internal channels
func NewBroker() *Broker {
	return &Broker{
		stopCh:    make(chan struct{}),
		publishCh: make(chan interface{}, 1),
		subCh:     make(chan chan interface{}, 1),
		unsubCh:   make(chan chan interface{}, 1),
	}
}

// Start starts the broker listening - note this should be run inside it's own
// go routine
func (b *Broker) Start(ctx context.Context) {
	subs := map[chan interface{}]struct{}{}
	for {
		select {
		case <-b.stopCh:
			return
		case msgCh := <-b.subCh:
			subs[msgCh] = struct{}{}
		case msgCh := <-b.unsubCh:
			delete(subs, msgCh)
		case msg := <-b.publishCh:
			for msgCh := range subs {
				// msgCh is buffered, use non-blocking send to protect the broker:
				select {
				case msgCh <- msg:
				default:
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// Stop closes all channels and effectively cancels all subscriptions
func (b *Broker) Stop() {
	close(b.stopCh)
}

// Subscribe returns a channel that a single client can use to listen to all
// messages published
func (b *Broker) Subscribe(ctx context.Context) (chan interface{}, error) {
	msgCh := make(chan interface{}, 5)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case b.subCh <- msgCh:
		return msgCh, nil
	}
}

// Publish sends a new message to be delivered to all subscribers
func (b *Broker) Publish(ctx context.Context, msg interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.publishCh <- msg:
		return nil
	}
}

// Unsubscribe closes and deletes a channel that was subscribed
func (b *Broker) Unsubscribe(ctx context.Context, msgCh chan interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.unsubCh <- msgCh:
		close(msgCh)
		return nil
	}
}
