package performance

import (
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"github.com/stretchr/testify/assert"
	"testing"
)

var topicSpec = &trogdor.TopicSpec{
	NumPartitions:     30,
	ReplicationFactor: 3,
	TopicName:         "topic",
}
var clientNodes = []string{
	"a", "b", "c",
}

func TestWorkloadCreateWorkloadCreatesCorrectNumberOfTasks(t *testing.T) {
	logger = common.InitLogger("performance-tests unit tests")
	expectedSteps := 11
	agentCount := 10

	workload := Workload{
		Name:                  "test",
		Type:                  PRODUCE_WORKLOAD_TYPE,
		PartitionCount:        10,
		StepDurationMs:        10,
		StepCooldownMs:        0,
		StartThroughputMbs:    10,
		EndThroughputMbs:      20,
		ThroughputIncreaseMbs: 1,
	}
	tasks, err := workload.CreateWorkload(agentCount, "a")
	assert.Nil(t, err)
	assert.Equal(t, len(tasks), expectedSteps*agentCount)

	workload = Workload{
		Name:                  "test",
		Type:                  PRODUCE_WORKLOAD_TYPE,
		PartitionCount:        10,
		StepDurationMs:        10,
		StepCooldownMs:        0,
		StartThroughputMbs:    1,
		EndThroughputMbs:      5,
		ThroughputIncreaseMbs: 3,
	}
	tasks, err = workload.CreateWorkload(agentCount, "a")
	assert.Nil(t, err)
	assert.Equal(t, len(tasks), 3*agentCount)
}

func TestStepTasksInvalidWorkloadTypeShouldReturnErr(t *testing.T) {
	step := Step{
		throughputMbs: 10,
		startMs:       10,
		endMs:         20,
		workload: &Workload{
			Type: "aaa_invalid",
		},
	}
	_, err := step.tasks(topicSpec, clientNodes, "localhost:9092")
	assert.NotNil(t, err)
}

func TestStepTasksCreatesAsManyTasksAsAgentNodes(t *testing.T) {
	step := Step{
		throughputMbs: 10,
		startMs:       10,
		endMs:         20,
		workload: &Workload{
			Type: PRODUCE_WORKLOAD_TYPE,
		},
	}
	agentNodes := []string{
		"a", "b", "c",
	}

	tasks, err := step.tasks(topicSpec, agentNodes, "localhost:9092")
	assert.Nil(t, err)
	assert.Equal(t, len(agentNodes), len(tasks))

	agentNodes = []string{"a"}
	tasks, err = step.tasks(topicSpec, agentNodes, "localhost:9092")
	assert.Nil(t, err)
	assert.Equal(t, len(agentNodes), len(tasks))

	agentNodes = []string{"a", "a", "a", "a", "a", "a"}
	tasks, err = step.tasks(topicSpec, agentNodes, "localhost:9092")
	assert.Nil(t, err)
	assert.Equal(t, len(agentNodes), len(tasks))
}
