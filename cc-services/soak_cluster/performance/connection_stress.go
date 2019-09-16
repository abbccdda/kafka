package performance

import (
	"fmt"
	"time"

	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/common"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"github.com/pkg/errors"
)

type ConnectionStress struct {
	Name string

	Duration                common.Duration `json:"duration"`
	TargetConnectionsPerSec int             `json:"target_connections_per_sec"`
	NumThreads              int             `json:"num_threads"`
	Fanout                  int             `json:"fanout"`
	TasksPerStep            int             `json:"tasks_per_step"`
	Action                  string          `json:"action"`
	SlowStartPerStepMs      uint64          `json:"slow_start_per_step_ms"`

	startTime time.Time
	endTime   time.Time
}

func (cs *ConnectionStress) testName() string {
	return cs.Name + "-connection-stress"
}

func (cs *ConnectionStress) CreateTest(trogdorAgentsCount int, bootstrapServers string) (tasks []trogdor.TaskSpec, err error) {
	clientNodes := common.TrogdorAgentPodNames(trogdorAgentsCount)
	common.ShuffleSlice(clientNodes)
	taskCount := len(clientNodes)
	if cs.TasksPerStep > 0 {
		taskCount = cs.TasksPerStep
	}
	startTime, err := cs.GetStartTime()
	if err != nil {
		return tasks, err
	}
	endTime, err := cs.GetEndTime()
	if err != nil {
		return tasks, err
	}
	spec := trogdor.ScenarioSpec{
		UsedNames: map[string]bool{},
	}

	for i := 1; i <= cs.Fanout; i++ {
		scenarioConfig := trogdor.ScenarioConfig{
			ScenarioID: trogdor.TaskId{
				TaskType: cs.testName(),
				StartMs:  common.TimeToUnixMilli(startTime),
				Desc: fmt.Sprintf(
					"%s-%dThreads-%dConnectionsPerSec-%sDuration",
					cs.Action, cs.NumThreads, cs.TargetConnectionsPerSec, cs.GetDuration().String(),
				),
			},
			Class:              trogdor.CONNECTION_STRESS_SPEC_CLASS,
			TaskCount:          taskCount,
			DurationMs:         uint64(endTime.Sub(startTime) / time.Millisecond),
			StartMs:            common.TimeToUnixMilli(startTime),
			BootstrapServers:   bootstrapServers,
			AdminConf:          adminConfig,
			ClientNodes:        clientNodes,
			SlowStartPerStepMs: cs.SlowStartPerStepMs,
			ConnectionStressTestConfig: trogdor.ConnectionStressTestConfig{
				TargetConnectionsPerSec: cs.TargetConnectionsPerSec,
				NumThreads:              cs.NumThreads,
				Action:                  cs.Action,
			},
		}
		spec.CreateScenario(scenarioConfig)
		tasks = append(tasks, spec.TaskSpecs...)
	}
	return tasks, nil
}

func (cs *ConnectionStress) GetName() string {
	return cs.Name
}

func newConnectionStress() *ConnectionStress {
	return &ConnectionStress{}
}

func (cs *ConnectionStress) validate() error {
	if (cs.Duration) < 0 {
		return errors.Errorf("%s: duration %s cannot be negative.", cs.Name, cs.GetDuration())
	}
	return nil
}

func (cs *ConnectionStress) GetDuration() time.Duration {
	return time.Duration(cs.Duration)
}

func (cs *ConnectionStress) GetStartTime() (time.Time, error) {
	if cs.startTime.IsZero() {
		return cs.startTime, errors.New("startTime not set")
	}
	return cs.startTime, nil
}

func (cs *ConnectionStress) GetEndTime() (time.Time, error) {
	if cs.endTime.IsZero() {
		return cs.endTime, errors.New("endTime not set")
	}
	return cs.endTime, nil
}

func (cs *ConnectionStress) SetStartTime(startTime time.Time) {
	cs.startTime = startTime
}

func (cs *ConnectionStress) SetEndTime(endTime time.Time) {
	cs.endTime = endTime
}
