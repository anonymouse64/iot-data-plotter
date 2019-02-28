package main

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	toml "github.com/pelletier/go-toml"
)

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
var Config = defaultConfig()

// LoadConfig will read in the file, loading the config, and perform validation on the config
func LoadConfig(file string) error {
	// open the file for reading
	f, err := os.Open(file)
	if err != nil {
		return err
	}

	// make a new decoder with the file
	// and decode the file into the global config
	err = toml.NewDecoder(f).Decode(Config)
	if err != nil {
		return err
	}

	// validate the config
	return checkConfig(Config)
}

// WriteConfig will write out the specified config to the file
func WriteConfig(file string, userconfig *ServerConfig) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	// Check the specified config, if it is nil, then
	// use the global one
	var cfgToUse *ServerConfig
	if userconfig == nil {
		// use the global one
		cfgToUse = Config
	} else {
		cfgToUse = userconfig
	}

	err = checkConfig(Config)
	if err != nil {
		return err
	}

	// encode the config to the file
	return toml.NewEncoder(f).Encode(*cfgToUse)
}

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

// checks various properties in the config to make sure they're usable
func checkConfig(cfg *ServerConfig) error {
	switch {
	// check that ports are greater than 0
	case cfg.WebSocketsConfig.Port < 1:
		return fmt.Errorf("http port %d is invalid", cfg.WebSocketsConfig.Port)
	case cfg.MQTTConfig.Port < 1:
		return fmt.Errorf("mqtt port %d is invalid", cfg.MQTTConfig.Port)
	case !validMQTTScheme(cfg.MQTTConfig.Scheme):
		return fmt.Errorf("mqtt scheme %s is invalid", cfg.MQTTConfig.Scheme)
	default:
		return nil
	}
}

// WriteTomlTreeKeyVal takes a toml tree and writes a key directly into it
// handling proper casting of floats to ints
// NOTE it doesn't check to see if the key is in the tree
func WriteTomlTreeKeyVal(tree *toml.Tree, key string, val interface{}) {
	// before setting the value, we need to check if the type of this key is an integer
	// because if the key is an integer value, when we are provided the interface{}, the
	// value we go to assign might actually be a float, because when we parse the values
	// from snapd, all numbers are interpreted as floats, so we have to convert the float
	// inside the interface{} to an int before assigning
	srcType := reflect.TypeOf(val).Kind()
	if srcType == reflect.Float32 || srcType == reflect.Float64 {
		// the source value is a float, check if the destination type is a
		// integer, in which case we should attempt to cast it before assigning
		// if any of these panic, that's fine because then the user provided an invalid value
		// for this field
		dstType := reflect.TypeOf(tree.Get(key)).Kind()
		floatVal := reflect.ValueOf(val).Float()
		switch dstType {
		case reflect.Int:
			tree.Set(key, int(floatVal))
		case reflect.Int8:
			tree.Set(key, int8(floatVal))
		case reflect.Int16:
			tree.Set(key, int16(floatVal))
		case reflect.Int32:
			tree.Set(key, int32(floatVal))
		case reflect.Int64:
			tree.Set(key, int64(floatVal))
		case reflect.Uint:
			tree.Set(key, uint(floatVal))
		case reflect.Uint8:
			tree.Set(key, uint8(floatVal))
		case reflect.Uint16:
			tree.Set(key, uint16(floatVal))
		case reflect.Uint32:
			tree.Set(key, uint32(floatVal))
		case reflect.Uint64:
			tree.Set(key, uint64(floatVal))
		default:
			// not an integer type, so just assign as is
			tree.Set(key, val)
		}
	} else {
		tree.Set(key, val)
	}
}

func WriteTomlFileKeyVal(tomlFile string, key string, val interface{}) error {
	tree, err := TomlConfigTree(tomlFile)
	if err != nil {
		return err
	}
	allKeys := TomlConfigKeys(tree)
	// check to make sure that this key exists
	if !stringInSlice(strings.TrimSpace(key), allKeys) {
		return fmt.Errorf("invalid key %s", key)
	}
	// write the key into the tree
	WriteTomlTreeKeyVal(tree, key, val)
	// sync the tree into a config struct
	f, err := os.OpenFile(tomlFile, os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = tree.WriteTo(f)
	return err
}

// TomlConfigTree is a simple wrapper function to get the toml tree from a config file
func TomlConfigTree(tomlFile string) (*toml.Tree, error) {
	return toml.LoadFile(tomlFile)
}

// TomlConfigKeys returns all toml keys in the config struct
func TomlConfigKeys(tree *toml.Tree) []string {
	leaveNames := make([]string, 0, 100)
	recurseLeaves(tree, "", &leaveNames)
	return leaveNames
}

// recurseLeaves follows all leaves of a toml.Tree, getting all possible key values
// that can be used
func recurseLeaves(tree *toml.Tree, prefix string, leaves *[]string) {
	// Iterate over all branches of this tree, checking if each branch is a leaf
	// or a subtree, recursing on subtrees
	for _, branchName := range tree.Keys() {
		branch := tree.Get(branchName)
		if subtree, ok := branch.(*toml.Tree); !ok {
			// This branch is a leaf - add it to the list of leaves
			leavesSlice := *leaves
			*leaves = append(leavesSlice, prefix+"."+branchName)
		} else {
			// This branch has more leaves - recurse into it
			if prefix == "" {
				// Don't include the prefix - this is the first call
				recurseLeaves(subtree, branchName, leaves)
			} else {
				// Include the prefix - this is a recursed call
				recurseLeaves(subtree, prefix+"."+branchName, leaves)
			}
		}
	}
}

// SetTreeValues takes a toml.Tree and a map of key values to update in the tree
func SetTreeValues(valmap map[string]interface{}, tree *toml.Tree) (*ServerConfig, error) {
	allKeys := TomlConfigKeys(tree)
	// iterate over the values, setting them inside the tree
	for key, val := range valmap {
		// check to make sure that this key exists
		if !stringInSlice(strings.TrimSpace(key), allKeys) {
			return nil, fmt.Errorf("invalid key %s", key)
		}
		WriteTomlTreeKeyVal(tree, key, val)
	}

	// marshal the tree to toml bytes, then unmarshal the bytes into the struct
	var cfg ServerConfig
	treeString, err := tree.ToTomlString()
	if err != nil {
		return nil, err
	}
	err = toml.Unmarshal([]byte(treeString), &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// default values for the config
func defaultConfig() *ServerConfig {
	return &ServerConfig{
		WebSocketsConfig: websocketsConfig{
			Port: 3000,
			Host: "0.0.0.0",
			Path: "/",
		},
		// TODO: change this to sensible defaults
		MQTTConfig: mqttConfig{
			Port:     1883,
			Host:     "localhost",
			TopicQoS: 1,
			Topic:    "my/topic",
		},
	}
}

// copied from https://stackoverflow.com/a/15323988/10102404
func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
