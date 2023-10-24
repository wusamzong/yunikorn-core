package custom
import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v3"
)

type AppConfig struct {
	ApplicationID string `yaml:"applicationId"`
	PodCount      int    `yaml:"podcount"`
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
		"app":  "/home/lab/document/01-yunikorn/yunikorn-core/config/workflow-config.yaml",
		"task": "/home/lab/document/01-yunikorn/yunikorn-core/config/task.yaml",
		"node": "/home/lab/document/01-yunikorn/yunikorn-core/config/node.yaml",
	}
)

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
