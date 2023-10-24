package drheft

import (
	"testing"
)

func TestLoadConfig(t *testing.T) {
	loadAppConfig()
	loadTaskConfig()
	loadNodeConfig()
	
}
