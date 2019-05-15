package performance

import (
	"encoding/json"
	"fmt"
	logutil "github.com/confluentinc/cc-utils/log"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"github.com/pkg/errors"

	"io/ioutil"
	"time"
)

// ScenarioTestConfig is the top-most definition for all the performance tests scheduled to run
type ScenarioTestConfig struct {
	Name               string                   `json:"scenario_name"`
	TestDefinitions    []*PerformanceTestConfig `json:"test_definitions"`
	ScheduleDefinition ScheduleDef              `json:"schedule"`

	tests   map[string]*PerformanceTestConfig
	context *ScenarioContext
}

func newScenarioTestConfig() *ScenarioTestConfig {
	return &ScenarioTestConfig{
		tests:   make(map[string]*PerformanceTestConfig),
		context: newScenarioContext(),
	}
}

// CreateTests() creates all the Trogdor tasks for each test that is part of this scenario.
// It requires that the config is parsed and that the schedules are created
func (sct *ScenarioTestConfig) CreateTests(trogdorAgentsCount int, bootstrapServers string) ([]trogdor.TaskSpec, error) {
	// since each task has a specific start/endTime, the ordering of tasks doesn't matter
	var tasks []trogdor.TaskSpec

	for _, testDef := range sct.TestDefinitions {
		currTasks, err := testDef.CreateTest(trogdorAgentsCount, bootstrapServers)
		if err != nil {
			return tasks, err
		}
		tasks = append(tasks, currTasks...)
	}

	return tasks, nil
}

func (sct *ScenarioTestConfig) ParseConfig(configPath string) error {
	raw, err := ioutil.ReadFile(configPath)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed reading the performance test configuration from %s", configPath))
	}

	return sct.parseJson(raw)
}

// CreateSchedules() parses the user-defined scheduling and sets each test's startTime/endTime accordingly
func (sct *ScenarioTestConfig) CreateSchedules(startTime time.Time) error {
	for _, testConfig := range sct.TestDefinitions {
		err := testConfig.prepareForScheduling(sct.context)
		if err != nil {
			return err
		}
	}

	logutil.Info(logger, "Calculating task schedules starting at %s", startTime)
	scheduler := common.NewScheduler(startTime)
	err := sct.addTestsToScheduler(scheduler)
	if err != nil {
		return err
	}

	err = scheduler.Schedule()
	if err != nil {
		return err
	}
	logutil.Debug(logger, "Calculated task schedules successfully!")

	// populate start and end times
	for _, test := range sct.tests {
		job, err := test.getSchedulableJob()
		if err != nil {
			return err
		}
		startTime, duration, endTime, err := scheduler.JobTimes(job.Name)
		if err != nil {
			return err
		}
		if duration != job.Duration {
			return fmt.Errorf("test %s duration (%d) does not match the given duration (%d) by the scheduler",
				test.Name, job.Duration, duration)
		}
		logutil.Debug(logger, "Test %s got scheduled startTime: %s, endTime: %s", test.Name, startTime, endTime)
		test.schedulableTest.SetStartTime(startTime)
		test.schedulableTest.SetEndTime(endTime)
		if !endTime.After(startTime) {
			return fmt.Errorf("test %s got scheduled invalid times - startTime: %s, endTime: %s",
				test.Name, startTime, endTime)
		}
	}
	return nil
}

func (sct *ScenarioTestConfig) parseTests() error {
	configsNeedingContext := []*PerformanceTestConfig{}
	for _, testConfig := range sct.TestDefinitions {
		err := testConfig.ParseTest(sct.context)
		if err != nil {
			switch err.(type) {
			case *NotEnoughContextError:
				configsNeedingContext = append(configsNeedingContext, testConfig)
			default:
				return err
			}
		} else {
			sct.context.addTest(testConfig.schedulableTest)
		}
	}
	for _, testConfig := range configsNeedingContext {
		err := testConfig.ParseTest(sct.context)
		if err != nil {
			switch err.(type) {
			case *NotEnoughContextError:
				return errors.Wrapf(err, "test %s did not have enough context after a second iteration", testConfig.Name)
			default:
				return err
			}
		}
		sct.context.addTest(testConfig.schedulableTest)
	}
	return nil
}

// parseJson() parses a json string into the scenario test structure
func (sct *ScenarioTestConfig) parseJson(rawJson []byte) error {
	err := json.Unmarshal(rawJson, &sct)
	if err != nil {
		return err
	}
	for _, def := range sct.TestDefinitions {
		if sct.tests[def.Name] != nil {
			return fmt.Errorf("test %s is defined twice", def.Name)
		}
		sct.tests[def.Name] = def
	}

	logutil.Info(logger, "Loaded configuration for scenario %s consisting of %d test definitions.",
		sct.Name, len(sct.TestDefinitions))
	return sct.ScheduleDefinition.validate(sct.tests)
}

// addTestsToScheduler() prepares all of this scenario's tests for scheduling
func (sct *ScenarioTestConfig) addTestsToScheduler(scheduler *common.Scheduler) error {
	schedJobByTest := make(map[string]*common.SchedulableJob)
	for _, test := range sct.TestDefinitions {
		job, err := test.getSchedulableJob()
		if err != nil {
			return err
		}
		schedJobByTest[test.Name] = job
	}

	if sct.ScheduleDefinition == nil || len(sct.ScheduleDefinition) == 0 {
		// schedule all jobs from start
		for _, test := range sct.TestDefinitions {
			scheduler.AddJob(schedJobByTest[test.Name])
		}
		return nil
	}

	for testName, schedule := range sct.ScheduleDefinition {
		job := schedJobByTest[testName]
		if job == nil {
			return fmt.Errorf("no schedulable job for test %s found", testName)
		}
		scheduler.AddJob(job)
		delayDuration := time.Duration(0)
		if schedule.StartDelay != "" {
			dur, err := time.ParseDuration(schedule.StartDelay)
			if err != nil {
				return err
			}
			delayDuration = dur
		}
		err := sct.scheduleJob(job, schedule, delayDuration)
		if err != nil {
			return err
		}
	}
	return nil
}

// scheduleJob() calls the Schedule() API of the common library
// to define this jobs' dependencies on others
// we expect that only one of StartAfterBegin and StartAfterEnd is populated
func (sct *ScenarioTestConfig) scheduleJob(job *common.SchedulableJob, schedule *Schedule, delayDuration time.Duration) error {
	job.DelayDuration = delayDuration

	if len(schedule.StartAfterBegin) != 0 {
		schedulableJobs := make([]*common.SchedulableJob, 0)
		for _, testName := range schedule.StartAfterBegin {
			job, err := sct.tests[testName].getSchedulableJob()
			if err != nil {
				return err
			}
			schedulableJobs = append(schedulableJobs, job)
		}
		err := job.ScheduleAfterJobStart(schedulableJobs)
		if err != nil {
			return err
		}
	} else if len(schedule.StartAfterEnd) != 0 {
		schedulableJobs := make([]*common.SchedulableJob, 0)
		for _, testName := range schedule.StartAfterEnd {
			job, err := sct.tests[testName].getSchedulableJob()
			if err != nil {
				return err
			}
			schedulableJobs = append(schedulableJobs, job)
		}
		err := job.ScheduleAfterJobEnd(schedulableJobs)
		if err != nil {
			return err
		}
	}

	if len(schedule.RunUntilEndOf) != 0 {
		schedulableJobs := make([]*common.SchedulableJob, 0)
		for _, testName := range schedule.RunUntilEndOf {
			job, err := sct.tests[testName].getSchedulableJob()
			if err != nil {
				return err
			}
			schedulableJobs = append(schedulableJobs, job)
		}
		err := job.RunUntilEndOf(schedulableJobs)
		if err != nil {
			return err
		}
	}

	return nil
}
