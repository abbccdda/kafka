package performance

import (
	"encoding/json"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJsonUnmarshalPerformanceTestConfig(t *testing.T) {
	logger = common.InitLogger("performance-tests unit tests")
	jsonConfig := `
{
	"test_type": "ProgressiveWorkload",
    "test_name": "test",
    "test_parameters": {
      "workload_type": "Produce",
      "step_duration_ms": 100,
      "partition_count": 10,
      "step_cooldown_ms": 60000,
      "start_throughput_mbs": 10,
      "end_throughput_mbs": 20,
      "throughput_increase_per_step_mbs": 5,
      "message_size_bytes": 1000
    }
}
`
	config := PerformanceTestConfig{}
	err := json.Unmarshal([]byte(jsonConfig), &config)
	assert.Nil(t, err)
	assert.Equal(t, PROGRESSIVE_WORKLOAD_TEST_TYPE, config.Type)
	assert.Equal(t, "test", config.Name)
	assert.NotNil(t, config.Parameters)
}
