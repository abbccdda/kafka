package performance

import (
	"fmt"
	"time"

	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

var simpleSingleTaskJsonConfig = `
{
  "scenario_name": "ExampleTest",
  "test_definitions": [
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
  ]
}
`

var abcdeTaskDefinitionsJson = `
{
    "test_type":"ProgressiveWorkload",
    "test_name":"A",
    "test_parameters":{
        "workload_type":"Produce",
        "step_duration_ms":1000,
        "partition_count":10,
        "step_cooldown_ms":60000,
        "start_throughput_mbs":10,
        "end_throughput_mbs":20,
        "throughput_increase_per_step_mbs":10,
        "message_size_bytes":1000
    }
},
{
    "test_type":"ProgressiveWorkload",
    "test_name":"B",
    "test_parameters":{
        "workload_type":"Produce",
        "step_duration_ms":100,
        "partition_count":10,
        "step_cooldown_ms":60000,
        "start_throughput_mbs":10,
        "end_throughput_mbs":20,
        "throughput_increase_per_step_mbs":5,
        "message_size_bytes":1000
    }
},
{
    "test_type":"ProgressiveWorkload",
    "test_name":"C",
    "test_parameters":{
        "workload_type":"Produce",
        "step_duration_ms":100,
        "partition_count":10,
        "step_cooldown_ms":60000,
        "start_throughput_mbs":10,
        "end_throughput_mbs":20,
        "throughput_increase_per_step_mbs":5,
        "message_size_bytes":1000
    }
},
{
    "test_type":"ProgressiveWorkload",
    "test_name":"D",
    "test_parameters":{
        "workload_type":"Produce",
        "step_duration_ms":100,
        "partition_count":10,
        "step_cooldown_ms":60000,
        "start_throughput_mbs":10,
        "end_throughput_mbs":20,
        "throughput_increase_per_step_mbs":5,
        "message_size_bytes":1000
    }
},
{
    "test_type":"ProgressiveWorkload",
    "test_name":"E",
    "test_parameters":{
        "workload_type":"Produce",
        "step_duration_ms":100,
        "partition_count":10,
        "step_cooldown_ms":60000,
        "start_throughput_mbs":10,
        "end_throughput_mbs":20,
        "throughput_increase_per_step_mbs":5,
        "message_size_bytes":1000
    }
}
`

// Given this task config, the tasks should execute in the following way (assuming start time is 0:00:00)
// E at 0:00:00, B at 0:02:00, A at 0:02:30, C at 0:02:30, D at ~00:14:46 (A's duration is 00:12:16)
var multipleTasksScenarioJson = fmt.Sprintf(`
{
    "scenario_name":"TestCPKAFKA",
    "schedule":{
        "A":{
            "start_delay":"30s",
            "start_after_begin":["B"]
        },
        "B":{
            "start_delay":"2m"
        },
        "C":{
            "start_after_begin":["A", "B"]
        },
        "D":{
            "start_delay":"2m",
            "start_after_end":["C", "A"]
        },
		"E": {}
    },
    "test_definitions":[
        %s
    ]
}
`, abcdeTaskDefinitionsJson)

func TestParseScenarioConfig(t *testing.T) {
	logger = common.InitLogger("performance-tests unit tests")

	config := newScenarioTestConfig()
	err := config.parseJson([]byte(simpleSingleTaskJsonConfig))

	assert.Nil(t, err)
	assert.Equal(t, 1, len(config.TestDefinitions))
	assert.Equal(t, "ExampleTest", config.Name)

	testDefinition := config.TestDefinitions[0]
	assert.Equal(t, testDefinition, config.tests["test"])
	assert.Equal(t, PROGRESSIVE_WORKLOAD_TEST_TYPE, testDefinition.Type)
	assert.Equal(t, "test", testDefinition.Name)
	assert.NotNil(t, testDefinition.Parameters)
}

func TestParseScenarioConfigMultipleTasks(t *testing.T) {
	// Expected schedule (example assuming each task runs for 1m) is:
	// now() = 0:00
	// B = 0:02
	// A = 0:03
	// C = 0:03
	// D = 0:06
	logger = common.InitLogger("performance-tests unit tests")

	config := newScenarioTestConfig()
	err := config.parseJson([]byte(multipleTasksScenarioJson))
	assert.Nil(t, err)

	// test schedule is parsed correctly
	schedA := config.ScheduleDefinition["A"]
	assert.NotNil(t, schedA)
	assert.Equal(t, "30s", schedA.StartDelay)
	assert.Equal(t, []string{"B"}, schedA.StartAfterBegin)
	assert.Equal(t, []string(nil), schedA.StartAfterEnd)
	schedB := config.ScheduleDefinition["B"]
	assert.NotNil(t, schedB)
	assert.Equal(t, "2m", schedB.StartDelay)
	assert.Equal(t, []string(nil), schedB.StartAfterBegin)
	assert.Equal(t, []string(nil), schedB.StartAfterEnd)
	schedC := config.ScheduleDefinition["C"]
	assert.Equal(t, "", schedC.StartDelay)
	assert.Equal(t, []string{"A", "B"}, schedC.StartAfterBegin)
	assert.Equal(t, []string(nil), schedC.StartAfterEnd)
	assert.NotNil(t, schedC)
	schedD := config.ScheduleDefinition["D"]
	assert.NotNil(t, schedD)
	assert.Equal(t, "2m", schedD.StartDelay)
	assert.Equal(t, []string(nil), schedD.StartAfterBegin)
	assert.Equal(t, []string{"C", "A"}, schedD.StartAfterEnd)

	// test definitions are parsed correctly
	assert.Equal(t, 5, len(config.TestDefinitions))
	for _, testName := range []string{"A", "B", "C", "D", "E"} {
		test := config.tests[testName]
		assert.NotNil(t, test)
		assert.Equal(t, testName, test.Name)
		assert.Equal(t, PROGRESSIVE_WORKLOAD_TEST_TYPE, test.Type)
	}
}

func TestScenarioConfigCyclicScheduleThrowsErr(t *testing.T) {
	logger = common.InitLogger("performance-tests unit tests")

	cyclicDepJson := fmt.Sprintf(`
{
	"scenario_name":"TestCPKAFKA",
    "schedule":{
        "A":{
            "start_delay":"30s",
            "start_after_begin":["B"]
        },
        "B":{
            "start_delay":"2m",
			"start_after_end":["D"]
        },
        "C":{
            "start_after_begin":["A", "B"]
        },
        "D":{
            "start_delay":"2m",
            "start_after_end":["C", "A"]
        }
    },
    "test_definitions":[
        %s
	]
}
`, abcdeTaskDefinitionsJson)
	scenarioTestCfg := newScenarioTestConfig()
	err := scenarioTestCfg.parseJson([]byte(cyclicDepJson))
	assert.Nil(t, err)
	err = scenarioTestCfg.CreateSchedules(time.Now())
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "cycle")
}

func TestScenarioConfigCreateSingleTaskSchedulePopulatesStartAndEndTimeAccordingly(t *testing.T) {
	logger = common.InitLogger("performance-tests unit tests")
	scenarioTestCfg := newScenarioTestConfig()
	startTime := time.Now()
	err := scenarioTestCfg.parseJson([]byte(simpleSingleTaskJsonConfig))
	assert.Nil(t, err)
	err = scenarioTestCfg.CreateSchedules(startTime)
	assert.Nil(t, err)

	test := scenarioTestCfg.tests["test"]
	receivedStartTime, _, _ := validateTestTime(t, test)
	assert.Equal(t, startTime, receivedStartTime)
}

func TestScenarioConfigCreateMultipleTasksSchedulePopulatesStartAndEndTimeAccordingly(t *testing.T) {
	logger = common.InitLogger("performance-tests unit tests")
	scenarioTestCfg := newScenarioTestConfig()
	startTime := time.Now()
	err := scenarioTestCfg.parseJson([]byte(multipleTasksScenarioJson))
	assert.Nil(t, err)
	err = scenarioTestCfg.CreateSchedules(startTime)
	assert.Nil(t, err)

	// Given this task config, the tasks should execute in the following way (assuming start time is 0:00:00)
	// E at 0:00:00, B at 0:02:00, A at 0:02:30, C at 0:02:30, D at ~00:14:46 (A's duration is 00:12:16)
	testBDuration := time.Duration(2) * time.Minute
	testDDelay := time.Duration(2) * time.Minute
	expectedBStartTime := startTime.Add(testBDuration)
	testE := scenarioTestCfg.tests["E"]
	startE, _, _ := validateTestTime(t, testE)
	assert.Equal(t, startTime, startE)

	testB := scenarioTestCfg.tests["B"]
	startB, _, _ := validateTestTime(t, testB)
	assert.Equal(t, expectedBStartTime, startB)

	testA := scenarioTestCfg.tests["A"]
	startA, _, endA := validateTestTime(t, testA)
	expectedAStartTime := startB.Add(time.Duration(30000) * time.Millisecond)
	assert.Equal(t, expectedAStartTime, startA)

	testC := scenarioTestCfg.tests["C"]
	startC, _, endC := validateTestTime(t, testC)
	assert.Equal(t, startC, startA)

	testD := scenarioTestCfg.tests["D"]
	startD, _, _ := validateTestTime(t, testD)
	maxTime := endA
	if endC.After(endA) {
		maxTime = endC
	}
	expectedDStartTime := maxTime.Add(testDDelay)
	assert.Equal(t, expectedDStartTime, startD)
}

func validateTestTime(t *testing.T, testCfg *PerformanceTestConfig) (time.Time, time.Duration, time.Time) {
	duration := testCfg.schedulableTest.GetDuration()

	endTime, err := testCfg.schedulableTest.GetEndTime()
	assert.Nil(t, err)
	startTime, err := testCfg.schedulableTest.GetStartTime()
	assert.Nil(t, err)

	assert.Equal(t, duration, endTime.Sub(startTime))
	return startTime, duration, endTime
}

func TestParseScenarioConfigInvalidScheduleThrowsErr(t *testing.T) {
	logger = common.InitLogger("performance-tests unit tests")

	invalidDurationJson := fmt.Sprintf(`
{
	"scenario_name":"TestCPKAFKA",
    "schedule":{
        "A":{
            "start_delay":"30something",
            "start_after_begin":["B"]
        },
        "B":{
            "start_delay":"2m"
        },
        "C":{
            "start_after_begin":["A", "B"]
        },
        "D":{
            "start_delay":"2m",
            "start_after_end":["C", "A"]
        }
    },
    "test_definitions":[
        %s
	]
}
`, abcdeTaskDefinitionsJson)
	err := newScenarioTestConfig().parseJson([]byte(invalidDurationJson))
	assert.NotNil(t, err)

	// we cannot have both start_after_end and start_after_begin defined
	bothStartAfterDefinedJson := fmt.Sprintf(`
{
	"scenario_name":"TestCPKAFKA",
    "schedule":{
        "A":{
            "start_delay":"30s",
            "start_after_end":["A"],
            "start_after_begin":["B"]
        },
        "B":{
            "start_delay":"2m"
        },
        "C":{
            "start_after_begin":["A", "B"]
        },
        "D":{
            "start_delay":"2m",
            "start_after_end":["C", "A"]
        }
    },
    "test_definitions":[
        %s
	]
}
`, abcdeTaskDefinitionsJson)
	err = newScenarioTestConfig().parseJson([]byte(bothStartAfterDefinedJson))
	assert.NotNil(t, err)

	invalidTaskNameSpecified := fmt.Sprintf(`
{
	"scenario_name":"TestCPKAFKA",
    "schedule":{
        "LLL":{
            "start_delay":"30s",
            "start_after_begin":["B"]
        },
        "B":{
            "start_delay":"2m"
        },
        "C":{
            "start_after_begin":["A", "B"]
        },
        "D":{
            "start_delay":"2m",
            "start_after_end":["C", "A"]
        }
    },
    "test_definitions":[
        %s
	]
}
`, abcdeTaskDefinitionsJson)
	err = newScenarioTestConfig().parseJson([]byte(invalidTaskNameSpecified))
	assert.NotNil(t, err)

	invalidTaskNameSpecified2 := fmt.Sprintf(`
{
	"scenario_name":"TestCPKAFKA",
    "schedule":{
        "A":{
            "start_delay":"30s",
            "start_after_begin":["LL"]
        },
        "B":{
            "start_delay":"2m"
        },
        "C":{
            "start_after_begin":["A", "B"]
        },
        "D":{
            "start_delay":"2m",
            "start_after_end":["C", "A"]
        }
    },
    "test_definitions":[
        %s
	]
}
`, abcdeTaskDefinitionsJson)
	err = newScenarioTestConfig().parseJson([]byte(invalidTaskNameSpecified2))
	assert.NotNil(t, err)

	taskReferencesItselfJson := fmt.Sprintf(`
{
	"scenario_name":"TestCPKAFKA",
    "schedule":{
        "A":{
            "start_delay":"30s",
            "start_after_begin":["A"]
        },
        "B":{
            "start_delay":"2m"
        },
        "C":{
            "start_after_begin":["A", "B"]
        },
        "D":{
            "start_delay":"2m",
            "start_after_end":["C", "A"]
        }
    },
    "test_definitions":[
        %s
	]
}
`, abcdeTaskDefinitionsJson)
	err = newScenarioTestConfig().parseJson([]byte(taskReferencesItselfJson))
	assert.NotNil(t, err)
}
