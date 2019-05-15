package performance

import (
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/pkg/errors"
	"time"
)

type ScheduleDef map[string]*Schedule

type Schedule struct {
	StartDelay      string   `json:"start_delay"`
	StartAfterBegin []string `json:"start_after_begin"`
	StartAfterEnd   []string `json:"start_after_end"`
	RunUntilEndOf   []string `json:"run_until_end_of"`
}

func (sd ScheduleDef) validate(taskNames map[string]*PerformanceTestConfig) error {
	for testName, schedule := range sd {
		if taskNames[testName] == nil {
			return errors.Errorf("test %s does not exist", testName)
		}
		if len(schedule.StartAfterEnd) != 0 && len(schedule.StartAfterBegin) != 0 {
			return errors.New("cannot have both start_after_begin and start_after_end populated")
		}
		for _, sTest := range schedule.StartAfterEnd {
			if taskNames[sTest] == nil {
				return errors.Errorf("test %s does not exist", sTest)
			}
			if testName == sTest {
				return errors.Errorf("test %s cannot start after itself", testName)
			}
			if common.StringSliceContains(schedule.RunUntilEndOf, sTest) {
				return errors.Errorf("test %s cannot start after %s ends since it is scheduled to run until it ends", testName, sTest)
			}
		}
		for _, sTest := range schedule.StartAfterBegin {
			if taskNames[sTest] == nil {
				return errors.Errorf("test %s does not exist", sTest)
			}
			if testName == sTest {
				return errors.Errorf("test %s cannot start after itself", testName)
			}
			if common.StringSliceContains(schedule.RunUntilEndOf, sTest) {
				return errors.Errorf("test %s cannot start after %s begins since it is scheduled to run until it ends", testName, sTest)
			}
		}
		for _, runUntilTest := range schedule.RunUntilEndOf {
			if taskNames[runUntilTest] == nil {
				return errors.Errorf("test %s does not exist", runUntilTest)
			}
		}

		if schedule.StartDelay != "" {
			_, err := time.ParseDuration(schedule.StartDelay)
			if err != nil {
				return errors.Wrapf(err, "could not parse duration %s", schedule.StartDelay)
			}
		}
	}

	return nil
}
