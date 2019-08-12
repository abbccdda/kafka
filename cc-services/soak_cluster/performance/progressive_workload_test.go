package performance

import (
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var topicSpec = &trogdor.TopicSpec{
	TopicName: "topic",
	PartitionsSpec: &trogdor.PartitionsSpec{
		NumPartitions:        uint64(30),
		ReplicationFactor:    3,
		PartitionsSpecConfig: adminConfig.ToPartitionSpecConfig(),
	},
}
var clientNodes = []string{
	"a", "b", "c",
}

func TestWorkloadCreateWorkloadCreatesCorrectNumberOfTasks(t *testing.T) {
	logger = common.InitLogger("performance-tests unit tests")
	expectedSteps := 11
	agentCount := 10
	startTime := time.Now()

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
	expectedDuration := time.Duration(expectedSteps*10) * time.Millisecond
	workload.SetStartTime(startTime)
	workload.SetEndTime(startTime.Add(expectedDuration))
	tasks, err := workload.CreateTest(agentCount, "a")
	assert.Nil(t, err)
	assert.Equal(t, len(tasks), expectedSteps*agentCount)

	expectedSteps = 3
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
	expectedDuration = time.Duration(expectedSteps*10) * time.Millisecond
	workload.SetStartTime(startTime)
	workload.SetEndTime(startTime.Add(expectedDuration))
	tasks, err = workload.CreateTest(agentCount, "a")
	assert.Nil(t, err)
	assert.Equal(t, len(tasks), expectedSteps*agentCount)
}

func TestStepTasksInvalidWorkloadTypeShouldReturnErr(t *testing.T) {
	step := Step{
		throughputMbs: 10,
		startTime:     time.Now(),
		endTime:       time.Now().Add(time.Duration(1000)),
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
		startTime:     time.Now(),
		endTime:       time.Now().Add(time.Duration(1000)),
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
