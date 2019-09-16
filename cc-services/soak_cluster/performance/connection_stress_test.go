package performance

import (
	"encoding/json"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestConnectionStressValidateWorksCorrectly(t *testing.T) {
	negativeDuration := ConnectionStress{
		Name:     "NegativeDuration",
		Duration: common.Duration(-100),
	}
	err := negativeDuration.validate()
	assert.NotNil(t, err)
}

func TestConnectionStressCreatesCorrectNumberOfTasks(t *testing.T) {
	fourHours, _ := time.ParseDuration("4h")
	cs := ConnectionStress{
		Name:                    "TestConnectionStressCreatesAsManyTasksAsAgentNodes",
		Duration:                common.Duration(fourHours),
		TargetConnectionsPerSec: 100,
		NumThreads:              10,
		Fanout:                  1,
		Action:                  "FETCH_METADATA",
		SlowStartPerStepMs:      0,
		startTime:               time.Now(),
		endTime:                 time.Now().Add(fourHours),
	}
	for agentNodes := 1; agentNodes < 9; agentNodes++ {
		for fanout := 1; fanout < 10; fanout++ {
			cs.Fanout = fanout
			tasks, err := cs.CreateTest(agentNodes, "localhost:9092")
			assert.Nil(t, err)
			assert.Equal(t, agentNodes*cs.Fanout, len(tasks))
		}
	}
	cs.TasksPerStep = 100
	for agentNodes := 1; agentNodes < 9; agentNodes++ {
		for fanout := 1; fanout < 10; fanout++ {
			cs.Fanout = fanout
			tasks, err := cs.CreateTest(agentNodes, "localhost:9092")
			assert.Nil(t, err)
			assert.Equal(t, cs.TasksPerStep*cs.Fanout, len(tasks))
		}
	}
}

func TestConnectionStressSlowStart(t *testing.T) {
	fourHours, _ := time.ParseDuration("4h")
	cs := ConnectionStress{
		Name:                    "TestConnectionStressCreatesAsManyTasksAsAgentNodes",
		Duration:                common.Duration(fourHours),
		TargetConnectionsPerSec: 100,
		NumThreads:              10,
		Fanout:                  1,
		Action:                  "FETCH_METADATA",
		SlowStartPerStepMs:      1000,
		startTime:               time.Now(),
		endTime:                 time.Now().Add(fourHours),
	}
	tasks, err := cs.CreateTest(9, "localhost:9092")
	assert.Nil(t, err)
	for index, element := range tasks {
		var result map[string]interface{}
		err = json.Unmarshal([]byte(element.Spec), &result)
		assert.Nil(t, err)
		startMs := uint64(result["startMs"].(float64))
		durationMs := uint64(result["durationMs"].(float64))
		assert.Equal(t, startMs, common.TimeToUnixMilli(cs.startTime)+cs.SlowStartPerStepMs*uint64(index))
		assert.Equal(t, durationMs, uint64(time.Duration(cs.Duration)/time.Millisecond)-cs.SlowStartPerStepMs*uint64(index))
	}
}
