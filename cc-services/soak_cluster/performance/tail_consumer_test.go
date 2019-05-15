package performance

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestValidateWorksCorrectly(t *testing.T) {
	testNameAndTopicsDefined := TailConsumer{
		ProduceTestName: "test",
		Topics:          []string{"a"},
		Duration:        time.Duration(100),
	}
	err := testNameAndTopicsDefined.validate()
	assert.NotNil(t, err)

	// we currently support one only due to trogdor client limitations
	multipleTopicsDefined := TailConsumer{
		Topics:   []string{"a", "b"},
		Duration: time.Duration(100),
	}
	err = multipleTopicsDefined.validate()
	assert.NotNil(t, err)

	noTopicsNorProduceTestDefined := TailConsumer{
		Duration: time.Duration(100),
	}
	err = noTopicsNorProduceTestDefined.validate()
	assert.NotNil(t, err)

	negativeDuration := TailConsumer{
		Topics:   []string{"a"},
		Duration: time.Duration(-100),
	}
	err = negativeDuration.validate()
	assert.NotNil(t, err)

	validDefinition := TailConsumer{
		Topics:   []string{"a"},
		Duration: time.Duration(100),
	}
	err = validDefinition.validate()
	assert.Nil(t, err)

	validDefinition2 := TailConsumer{
		ProduceTestName: "a",
		Duration:        time.Duration(100),
	}
	err = validDefinition2.validate()
	assert.Nil(t, err)
}

// CreateTest should create as many ConsumeBench Trogdor task specifications as Trogdor agents given, so they're spread out.
// The fanout parameter should be a multiplier of these tasks
func TestCreateTestReturnsCorrectNumberOfTasks(t *testing.T) {
	trogdorAgentCounts := []int{1, 2, 3, 4, 5, 6, 10}
	fanoutCounts := []int{1, 2, 3, 4, 5, 6, 10}
	for _, trogdorAgents := range trogdorAgentCounts {
		for _, fanout := range fanoutCounts {
			expectedTaskCount := trogdorAgents * fanout
			consumerTest := TailConsumer{
				Fanout:    fanout,
				Duration:  time.Duration(100),
				Topics:    []string{"a"},
				startTime: time.Now(),
				endTime:   time.Now(),
			}
			err := consumerTest.validate()
			assert.Nil(t, err)

			tasks, err := consumerTest.CreateTest(trogdorAgents, "abc")
			assert.Nil(t, err)
			assert.Equal(t, expectedTaskCount, len(tasks))
		}
	}
}

// the test framework should populate the Topics field from the ProduceTestName
func TestCreateTestReturnsErrorIfNoTopicsPopulated(t *testing.T) {
	noTopicsConsumer := TailConsumer{
		ProduceTestName: "a",
		Duration:        time.Duration(100),
	}
	err := noTopicsConsumer.validate()
	assert.Nil(t, err)

	_, err = noTopicsConsumer.CreateTest(3, "abc")
	assert.NotNil(t, err)
}
