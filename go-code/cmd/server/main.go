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

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/eknkc/basex"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	flags "github.com/jessevdk/go-flags"
)

var upgrader = websocket.Upgrader{}

const maxInitialConnectTries = 10

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
func (cmd *UpdateConfigCmd) Execute(args []string) (err error) {
	// List all toml keys from the config struct using the config file specified
	tree, err := TomlConfigTree(currentCmd.ConfigFile)
	if err != nil {
		return err
	}

	// Get all the values of these keys from snapd
	snapValues, err := getSnapKeyValues(TomlConfigKeys(tree))
	if err != nil {
		return err
	}

	cfg, err := SetTreeValues(snapValues, tree)
	if err != nil {
		return err
	}

	// Finally write out the config to the file
	err = WriteConfig(currentCmd.ConfigFile, cfg)
	if err != nil {
		return err
	}

	return
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
// TODO: not implemented yet
func (cmd *SetConfigCmd) Execute(args []string) (err error) {
	// Load the config file
	err = LoadConfig(currentCmd.ConfigFile)
	if err != nil {
		return
	}

	// Now get the keys into the struct to set the values in the struct

	return
}

// GetConfigCmd is a command for getting config values from the config file
type GetConfigCmd struct {
	Args struct {
		Key string `positional-arg-name:"key"`
	} `positional-args:"yes" required:"yes"`
}

// Execute of GetConfigCmd will print off config values from the command line as specified in the config file
// TODO: not implemented yet
func (cmd *GetConfigCmd) Execute(args []string) (err error) {
	tree, err := TomlConfigTree(currentCmd.ConfigFile)
	if err != nil {
		return err
	}

	// Get the key from the tree
	fmt.Println(tree.Get(cmd.Args.Key))
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
			// write out a new file then
			return WriteConfig(currentCmd.ConfigFile, nil)
		} else {
			return fmt.Errorf("config file %s doesn't exist", currentCmd.ConfigFile)
		}
	}
	// otherwise the file exists, so load it
	return LoadConfig(currentCmd.ConfigFile)
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
	err = LoadConfig(currentCmd.ConfigFile)
	if err != nil {
		return err
	}

	// mqtt logs for debugging the mqtt connections
	// MQTT.DEBUG = log.New(os.Stderr, "DEBUG    ", log.Ltime)
	// MQTT.WARN = log.New(os.Stderr, "WARNING  ", log.Ltime)
	// MQTT.CRITICAL = log.New(os.Stderr, "CRITICAL ", log.Ltime)
	// MQTT.ERROR = log.New(os.Stderr, "ERROR    ", log.Ltime)

	// defaultCertsDir := "/snap/test-mqtts-server/current/certs"

	// Make an options struct for the mqtt client
	connOpts := MQTT.NewClientOptions()

	// always use a clean session
	connOpts.SetCleanSession(true)

	// always reconnect
	connOpts.SetAutoReconnect(true)

	conf := &tls.Config{}
	mqttScheme := Config.MQTTConfig.Scheme

	// build the URL - first we need to check if the configured scheme is
	// the empty string, in which case we assume insecure
	switch Config.MQTTConfig.Scheme {
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
		if Config.MQTTConfig.ClientCertFile != "" || Config.MQTTConfig.ClientKeyFile != "" {
			// is non-empty string so assume it is supposed to exist
			// get the client certificate pair and load them
			clientCert, err := tls.LoadX509KeyPair(Config.MQTTConfig.ClientCertFile, Config.MQTTConfig.ClientKeyFile)
			if err != nil {
				log.Fatalf("couldn't make client cert: %v\n", clientCert, err)
			}

			// add the client certificate to the tls configuration
			conf.Certificates = []tls.Certificate{clientCert}
		}

		// check if there's an additional server cert to use for verification
		if Config.MQTTConfig.ServerCertFile != "" {
			brokerCACert, err := ioutil.ReadFile(Config.MQTTConfig.ServerCertFile)
			if err != nil {
				log.Fatalf("couldn't open broker ca cert file at %s: %v\n", Config.MQTTConfig.ServerCertFile, err)
			}

			certPool := x509.NewCertPool()
			certPool.AppendCertsFromPEM(brokerCACert)
			conf.RootCAs = certPool
		}

		connOpts.SetTLSConfig(conf)
	default:
		// somehow got an invalid tls configuration
		log.Fatalf("invalid mqtt scheme %s\n", Config.MQTTConfig.Scheme)
	}

	// build the broker URL and add that
	connOpts.AddBroker(fmt.Sprintf("%s://%s:%d%s",
		mqttScheme,
		Config.MQTTConfig.Host,
		Config.MQTTConfig.Port,
		Config.MQTTConfig.Path,
	))

	// if the client ID for the MQTT client is empty, then generate a random
	// one
	if Config.MQTTConfig.ClientID == "" {
		connOpts.SetClientID(generateRandomClientID())
	} else {
		connOpts.SetClientID(Config.MQTTConfig.ClientID)
	}

	// create the client
	client := MQTT.NewClient(connOpts)

	// make an mqtt connection to the broker - giving up and dying after 10
	// unsuccessful tries, every 3 seconds
	initialConnectTries := 0
	for {
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			log.Printf("couldn't connect to broker: %v\n", token.Error())
			log.Println("sleeping for 3 seconds")
			time.Sleep(time.Second * 3)
			initialConnectTries++
			if initialConnectTries > maxInitialConnectTries {
				log.Fatalf("failed to connect to broker after %d tries\n", initialConnectTries)
			}
		} else {
			// connected successfully
			break
		}
	}

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

	// listen on all interfaces, upgrading all http traffic to websockets and
	// forwarding the clients all mqtt messages
	http.HandleFunc(Config.WebSocketsConfig.Path, makeForwardMQTTMessageFunc(channelBroker))
	return http.ListenAndServe(
		fmt.Sprintf("%s:%d", Config.WebSocketsConfig.Host, Config.WebSocketsConfig.Port),
		nil,
	)
}

// makeOnMessageFunc returns a lambda function which publishes all mqtt messages
// recieved to the broker
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
