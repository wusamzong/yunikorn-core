package custom

import (
	"testing"
)

func TestLoadConfig(t *testing.T) {
	LoadTestAppConfig()
	LoadAppConfig()
	LoadTaskConfig()
	LoadNodeConfig()

}
