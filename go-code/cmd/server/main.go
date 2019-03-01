package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"

	tc "github.com/anonymouse64/websockets-mqtt-visualizer/tomlconfigurator"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/eknkc/basex"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	flags "github.com/jessevdk/go-flags"
	toml "github.com/pelletier/go-toml"
)

var upgrader = websocket.Upgrader{}

const maxInitialConnectTries = 10

type mqttConfig struct {
	Port           int    `toml:"port"`
	Scheme         string `toml:"scheme"`
	Host           string `toml:"host"`
	Path           string `toml:"path"`
	ClientCertFile string `toml:"clientcert"`
	ClientKeyFile  string `toml:"clientkey"`
	ServerCertFile string `toml:"servercert"`
	ClientID       string `toml:"clientid"`
	TopicQoS       int    `toml:"topicqos"`
	Topic          string `toml:"topic"`
}

type websocketsConfig struct {
	Port int    `toml:"port"`
	Host string `toml:"host"`
	Path string `toml:"path"`
}

// ServerConfig holds all of the config values
type ServerConfig struct {
	WebSocketsConfig websocketsConfig `toml:"websockets"`
	MQTTConfig       mqttConfig       `toml:"mqtt"`
}

// Config is the current server config
var Config *ServerConfig

// validMQTTScheme checks if the specific scheme is valid or not
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

// Validate checks various properties in the config to make sure they're usable
func (s *ServerConfig) Validate() error {
	switch {
	// check that ports are greater than 0
	case s.WebSocketsConfig.Port < 1:
		return fmt.Errorf("http port %d is invalid", s.WebSocketsConfig.Port)
	case s.MQTTConfig.Port < 1:
		return fmt.Errorf("mqtt port %d is invalid", s.MQTTConfig.Port)
	case !validMQTTScheme(s.MQTTConfig.Scheme):
		return fmt.Errorf("mqtt scheme %s is invalid", s.MQTTConfig.Scheme)
	default:
		return nil
	}
}

// MarshalTOML marshals the config into bytes
func (s *ServerConfig) MarshalTOML() ([]byte, error) {
	return toml.Marshal(*s)
}

// UnmarshalTOML marshals the config into bytes
func (s *ServerConfig) UnmarshalTOML(bytes []byte) error {
	return toml.Unmarshal(bytes, s)
}

// SetDefault sets default values for the config
func (s *ServerConfig) SetDefault() error {
	s.WebSocketsConfig.Port = 3000
	s.WebSocketsConfig.Host = "0.0.0.0"
	s.WebSocketsConfig.Path = "/"
	s.MQTTConfig.Port = 1883
	s.MQTTConfig.Host = "localhost"
	s.MQTTConfig.Topic = "my/topic"
	s.MQTTConfig.TopicQoS = 1
	return nil
}

func init() {
	Config = &ServerConfig{}
	Config.SetDefault()
}

// Command is the command for application management
type Command struct {
	Start      StartCmd  `command:"start" description:"Start the server"`
	Config     ConfigCmd `command:"config" description:"Change or get config values"`
	ConfigFile string    `short:"c" long:"config-file" description:"Configuration file to use" required:"yes"`
}

// The current input command
var currentCmd Command

// ConfigCmd is for a set of commands working with the config file programmatically
type ConfigCmd struct {
	Check      CheckConfigCmd  `command:"check" descripttion:"Check a configuration file"`
	SnapUpdate UpdateConfigCmd `command:"update" description:"Update the configuration"`
	Set        SetConfigCmd    `command:"set" description:"Set values in the configuration file"`
	Get        GetConfigCmd    `command:"get" description:"Get values from the configuration file"`
}

// UpdateConfigCmd is a command for updating a config file from snapd/snapctl environment values
type UpdateConfigCmd struct{}

// Execute of UpdateConfigCmd will update a config file using values from snapd / snapctl
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

// getSnapKeyValues queries snapctl for all key values at once as JSON, and returns the corresponding values
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

// Execute of SetConfigCmd will set config values from the command line inside the config file
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

// Execute of GetConfigCmd will print off config values from the command line as specified in the config file
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
		return fmt.Errorf("config file %s doesn't exist", currentCmd.ConfigFile)
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

	// mqtt logs for debugging the mqtt connections
	// MQTT.DEBUG = log.New(os.Stderr, "DEBUG    ", log.Ltime)
	// MQTT.WARN = log.New(os.Stderr, "WARNING  ", log.Ltime)
	// MQTT.CRITICAL = log.New(os.Stderr, "CRITICAL ", log.Ltime)
	// MQTT.ERROR = log.New(os.Stderr, "ERROR    ", log.Ltime)

	// defaultCertsDir := "/snap/test-mqtts-server/current/certs"

	// build an mqtt client out of the configuration
	client, err := buildMQTTClient(Config.MQTTConfig)
	if err != nil {
		log.Fatalf("failed to build mqtt client: %s\n", err)
	}
	log.Println("build client")
	// make an mqtt connection to the broker - giving up and dying after 10
	// unsuccessful tries, every 3 seconds
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

	log.Println("client connected")

	// make an internal broker to pass messages from the mqtt go routine to
	// all of the http/websockets go routines
	channelBroker := NewBroker()
	go channelBroker.Start()

	// subscribe to the specified mqtt topic with a message handler
	// tied to the created broker
	if token := client.Subscribe(Config.MQTTConfig.Topic,
		byte(Config.MQTTConfig.TopicQoS),
		makeOnMessageFunc(channelBroker),
	); token.Wait() && token.Error() != nil {
		log.Fatalf(
			"couldn't subscribe to topic %s: %v\n",
			Config.MQTTConfig.Topic,
			token.Error(),
		)
	}
	log.Println("client subscribed")

	// listen on all interfaces, upgrading all http traffic to websockets and
	// forwarding the clients all mqtt messages
	http.HandleFunc(Config.WebSocketsConfig.Path, makeForwardMQTTMessageFunc(channelBroker))
	return http.ListenAndServe(
		fmt.Sprintf("%s:%d", Config.WebSocketsConfig.Host, Config.WebSocketsConfig.Port),
		nil,
	)
}

// makeForwardMQTTMessageFunc returns a lambda function which uses the broker
// to create a new subscription channel for the http request, and thus
// receives all mqtt messages and forwards them to the http client (which is
// upgraded to a websockets client)
func makeForwardMQTTMessageFunc(msgBroker *Broker) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// upgrade the HTTP request to a websockets connection
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Print("upgrade:", err)
			return
		}
		defer c.Close()

		// get a subscription channel
		subChannel := msgBroker.Subscribe()

		// forward any messages from the MQTT channel and send on the
		// websockets connection
		for msg := range subChannel {
			if mqttMsg, ok := msg.(MQTT.Message); ok {
				err = c.WriteMessage(websocket.TextMessage, mqttMsg.Payload())
				if err != nil {
					log.Printf("error writing message %s\n", err)
					break
				}
			}
		}
	}
}

// makeOnMessageFunc returns a lambda function which publishes all mqtt messages
// received to the broker
func makeOnMessageFunc(channelBroker *Broker) MQTT.MessageHandler {
	return func(client MQTT.Client, msg MQTT.Message) {
		opts := client.OptionsReader()
		log.Printf("client %s got message %s\n", opts.ClientID(), string(msg.Payload()))
		channelBroker.Publish(msg)
	}
}

// generateRandomClientID generates a random clientID based on a UUID, encoded
// in base62 to always be less 23 characters or less to be MQTT 3.1.1 compliant
func generateRandomClientID() string {
	uuidBytes := uuid.New()
	encoder, err := basex.NewEncoding("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	if err != nil {
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
		if config.ServerCertFile != "" {
			brokerCACert, err := ioutil.ReadFile(config.ServerCertFile)
			if err != nil {
				return nil, fmt.Errorf("couldn't open broker ca cert file at %s: %v", config.ServerCertFile, err)
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
