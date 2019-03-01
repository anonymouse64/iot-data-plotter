package tomlconfigurator

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"

	toml "github.com/pelletier/go-toml"
)

// TomlConfigurator is an interface that makes it easy to handle configuration
// files, including getting specific keys,
type TomlConfigurator interface {
	SetDefault() error
	MarshalTOML() ([]byte, error)
	UnmarshalTOML([]byte) error
	Validate() error
}

// LoadTomlConfigurator loads a toml file into the TomlConfigurator
func LoadTomlConfigurator(file string, t TomlConfigurator) error {
	// open the file for reading
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}

	// make a new decoder with the file
	// and decode the file into the global config
	err = t.UnmarshalTOML(bytes)
	if err != nil {
		return err
	}

	// validate the config
	return t.Validate()
}

// WriteTomlConfigurator writes out the TomlConfigurator to a file
func WriteTomlConfigurator(file string, t TomlConfigurator) error {
	// Validate the TOML first, then write it out
	err := t.Validate()
	if err != nil {
		return err
	}

	// encode the config to the file
	bytes, err := t.MarshalTOML()
	return ioutil.WriteFile(file, bytes, 0644)
}

// setTomlTreeKeyVal takes a toml tree and writes a key directly into it
// handling proper casting of floats to ints
// NOTE it doesn't check to see if the key is in the tree
func setTomlTreeKeyVal(tree *toml.Tree, key string, val interface{}) {
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

// SetTomlConfiguratorKeyVal sets a key in the TomlConfigurator to the
// concrete go value behind the interface{} that is val
func SetTomlConfiguratorKeyVal(t TomlConfigurator, key string, val interface{}) error {
	tree, err := tomlTree(t)
	if err != nil {
		return err
	}
	allKeys := tomlConfigKeys(tree)
	// check to make sure that this key exists
	if !stringInSlice(strings.TrimSpace(key), allKeys) {
		return fmt.Errorf("invalid key %s", key)
	}
	// write the key into the tree
	setTomlTreeKeyVal(tree, key, val)

	return loadTreeIntoTomlConfigurator(tree, t)
}

// GetTomlConfiguratorKeyVal gets the value of a specific key from the TomlConfigurator
// and returns it as an interface{}
func GetTomlConfiguratorKeyVal(t TomlConfigurator, key string) (interface{}, error) {
	tree, err := tomlTree(t)
	if err != nil {
		return nil, err
	}

	// Get the key from the tree
	return tree.Get(key), nil
}

func tomlTree(t TomlConfigurator) (*toml.Tree, error) {
	// there isn't a direct method to go from a struct to a toml.Tree, so
	// instead we need to go from the struct -> toml bytes -> toml.Tree
	// it's not super efficient, but it does make sure we still encapsulate
	// details of the configuration and where it came from from the caller
	tomlBytes, err := t.MarshalTOML()
	if err != nil {
		return nil, err
	}
	return toml.LoadBytes(tomlBytes)
}

func loadTreeIntoTomlConfigurator(tree *toml.Tree, t TomlConfigurator) error {
	// turn the toml Tree into a string, then write Unmarshal that string
	// back into the TomlConfigurator's internal struct
	treeString, err := tree.ToTomlString()
	if err != nil {
		return err
	}
	return t.UnmarshalTOML([]byte(treeString))
}

// TomlKeys returns a list of all keys from the TomlConfigurator, such that
// nested maps have "." as the delimiter between the parent section and the key
func TomlKeys(t TomlConfigurator) ([]string, error) {
	tree, err := tomlTree(t)
	if err != nil {
		return nil, err
	}
	// now get the keys from the tree
	return tomlConfigKeys(tree), nil
}

// tomlConfigKeys returns all toml keys in the config struct
func tomlConfigKeys(tree *toml.Tree) []string {
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

// SetTomlConfiguratorKeyValues takes a map of keys + concrete go values (stored as
// interface{}) and sets the values in the TomlConfigurator struct
func SetTomlConfiguratorKeyValues(valmap map[string]interface{}, t TomlConfigurator) error {
	tree, err := tomlTree(t)
	if err != nil {
		return err
	}

	allKeys := tomlConfigKeys(tree)
	// iterate over the values, setting them inside the tree
	for key, val := range valmap {
		// check to make sure that this key exists
		if !stringInSlice(strings.TrimSpace(key), allKeys) {
			return fmt.Errorf("invalid key %s", key)
		}
		setTomlTreeKeyVal(tree, key, val)
	}

	return loadTreeIntoTomlConfigurator(tree, t)
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
