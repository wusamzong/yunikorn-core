package custom

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"

	"gopkg.in/yaml.v3"
)

type AppConfig struct {
	ApplicationID string `yaml:"applicationId"`
	PodCount      int    `yaml:"podcount"`
	Dependency    []struct {
		Idx    string `yaml:"idx"`
		Weight int    `yaml:"weight"`
	} `yaml:"dependency"`
}

type TasksConfig struct {
	Tasks []TaskConfig `yaml:"tasks"`
}

type TaskConfig struct {
	Name     string      `yaml:"name"`
	Children []ChildName `yaml:"children"`
	Data     int         `yaml:"data"`
}

type ChildName struct {
	Name string
}

type NodesConfig struct {
	Nodes []NodeConfig `yaml:"nodes"`
}

type NodeConfig struct {
	Name  string `yaml:"name"`
	Links []link `yaml:"link"`
}

type link struct {
	Name      string `yaml:"name"`
	Bandwidth string `yaml:"bandwidth"`
}

var (
	configFile = map[string]string{
		"app-test": "/home/lab/document/01-yunikorn/yunikorn-core/config/test-workflow-config.yaml",
		"app":      "/home/lab/document/01-yunikorn/yunikorn-core/config/workflow-config.yaml",
		"task":     "/home/lab/document/01-yunikorn/yunikorn-core/config/task.yaml",
		"node":     "/home/lab/document/01-yunikorn/yunikorn-core/config/node.yaml",
	}
)

func LoadTestAppConfig() AppConfig {
	yamlFile, err := ioutil.ReadFile(configFile["app-test"])
	if err != nil {
		log.Fatalf("Error reading YAML file: %v", err)
	}

	var config AppConfig
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		log.Fatalf("Error unmarshaling YAML: %v", err)
	}
	return config
}

func GetEdgeDataSize(config AppConfig, from, to string)float64{
	for _, dependency := range config.Dependency{
		if dependency.Idx == fmt.Sprintf("%s-%s", from, to){
			return float64(dependency.Weight) 
		}
	}
	return 0.0
}

func LoadAppConfig() AppConfig {
	yamlFile, err := ioutil.ReadFile(configFile["app"])
	if err != nil {
		log.Fatalf("Error reading YAML file: %v", err)
	}

	var config AppConfig
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		log.Fatalf("Error unmarshaling YAML: %v", err)
	}
	return config
}

func LoadTaskConfig() TasksConfig {
	yamlFile, err := ioutil.ReadFile(configFile["task"])
	if err != nil {
		log.Fatalf("Error reading YAML file: %v", err)
	}

	var config TasksConfig
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		log.Fatalf("Error unmarshaling YAML: %v", err)
	}

	return config
}

func LoadNodeConfig() NodesConfig {
	yamlFile, err := ioutil.ReadFile(configFile["node"])
	if err != nil {
		log.Fatalf("Error reading YAML file: %v", err)
	}

	var config NodesConfig
	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		log.Fatalf("Error unmarshaling YAML: %v", err)
	}

	return config
}
