package custom

import (
	"testing"
)

func TestLoadConfig(t *testing.T) {
	LoadAppConfig()
	LoadTaskConfig()
	LoadNodeConfig()
	
}
