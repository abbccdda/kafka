package performance

import (
	"github.com/pkg/errors"
	"time"
)

type ScheduleDef map[string]*Schedule

type Schedule struct {
	StartDelay      string   `json:"start_delay"`
	StartAfterBegin []string `json:"start_after_begin"`
	StartAfterEnd   []string `json:"start_after_end"`
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
		}
		for _, sTest := range schedule.StartAfterBegin {
			if taskNames[sTest] == nil {
				return errors.Errorf("test %s does not exist", sTest)
			}
			if testName == sTest {
				return errors.Errorf("test %s cannot start after itself", testName)
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
