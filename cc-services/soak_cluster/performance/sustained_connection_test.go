package performance

import (
	"encoding/json"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSustainedConnectionValidateWorksCorrectly(t *testing.T) {
	sc := SustainedConnection{
		Name:     "NegativeDuration",
		Duration: common.Duration(-100),
	}
	err := sc.validate()
	assert.NotNil(t, err)

	sc = SustainedConnection{
		Name: "NoTopicConsumer",
		ConsumerConnectionCount: 1,
	}
	err = sc.validate()
	assert.NotNil(t, err)

	sc = SustainedConnection{
		Name: "TopicConsumer",
		ConsumerConnectionCount: 1,
		TopicName:               "Topic",
	}
	err = sc.validate()
	assert.Nil(t, err)

	sc = SustainedConnection{
		Name: "NoTopicProducer",
		ProducerConnectionCount: 1,
	}
	err = sc.validate()
	assert.NotNil(t, err)

	sc = SustainedConnection{
		Name: "TopicProducer",
		ProducerConnectionCount: 1,
		TopicName:               "Topic",
	}
	err = sc.validate()
	assert.Nil(t, err)

}

func TestSustainedConnectionCreatesCorrectNumberOfTasks(t *testing.T) {
	fourHours, _ := time.ParseDuration("4h")
	cs := SustainedConnection{
		Name:               "TestSustainedConnectionSlowStart",
		Duration:           common.Duration(fourHours),
		Fanout:             1,
		TopicName:          "Topic",
		SlowStartPerStepMs: 1000,
		startTime:          time.Now(),
		endTime:            time.Now().Add(fourHours),
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

func TestSustainedConnectionSlowStart(t *testing.T) {
	fourHours, _ := time.ParseDuration("4h")
	sc := SustainedConnection{
		Name:               "TestSustainedConnectionSlowStart",
		Duration:           common.Duration(fourHours),
		Fanout:             1,
		TopicName:          "Topic",
		SlowStartPerStepMs: 1000,
		startTime:          time.Now(),
		endTime:            time.Now().Add(fourHours),
	}
	tasks, err := sc.CreateTest(9, "localhost:9092")
	assert.Nil(t, err)
	for index, element := range tasks {
		var result map[string]interface{}
		err = json.Unmarshal([]byte(element.Spec), &result)
		assert.Nil(t, err)
		startMs := uint64(result["startMs"].(float64))
		durationMs := uint64(result["durationMs"].(float64))
		assert.Equal(t, startMs, common.TimeToUnixMilli(sc.startTime)+sc.SlowStartPerStepMs*uint64(index))
		assert.Equal(t, durationMs, uint64(time.Duration(sc.Duration)/time.Millisecond)-sc.SlowStartPerStepMs*uint64(index))
	}
}
