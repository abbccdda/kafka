package common

import (
	"fmt"
	logutil "github.com/confluentinc/cc-utils/log"
	"github.com/confluentinc/ce-kafka/cc-services/soak_cluster/trogdor"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/spf13/viper"
	"os"
	"sync"
	"time"
)

var (
	abortChannel = make(chan abortError)
	abortGrp     = sync.WaitGroup{}
)

type abortError struct {
	message string
	taskIds []string
	logger  log.Logger
}

func (a *abortError) Error() string {
	return a.message
}

// Creates the passed in Trogdor tasks by calling the Trogdor Coordinator REST API
func ScheduleTrogdorTasks(logger log.Logger, taskSpecs []trogdor.TaskSpec, trogdorCoordinatorHost string) {
	var runningTaskNames []string

	logutil.Info(logger, "Creating %d Trogdor tasks", len(taskSpecs))
	for _, task := range taskSpecs {
		resp, body, err := task.CreateTask(trogdorCoordinatorHost)
		if err != nil {
			logutil.Error(logger, "Error while trying to create task %s. err: %s", task.ID, err)
			abortAndCleanUp(trogdorCoordinatorHost,
				abortError{err.Error(), runningTaskNames, logger})
		}
		if code := resp.StatusCode; code > 299 {
			errMsg := fmt.Sprintf("Error while trying to create task %s. status code: %d, body: %s", task.ID, code, body)
			logutil.Error(logger, errMsg)
			abortAndCleanUp(trogdorCoordinatorHost,
				abortError{errMsg, runningTaskNames, logger})
		}
		logutil.Debug(logger, "Created task %s", task.ID)
		runningTaskNames = append(runningTaskNames, task.ID)
	}
	logutil.Info(logger, "Created %d Trogdor tasks", len(runningTaskNames))
}

// waits until all the existing tasks get cleaned up and panics
func abortAndCleanUp(trogdorCoordinatorHost string, err abortError) {
	abortGrp.Add(1)
	go abortTasks(trogdorCoordinatorHost)
	abortChannel <- err

	logutil.Info(err.logger, "Aborting...")
	timedOut := waitTimeout(&abortGrp, time.Minute*5)
	if timedOut {
		logutil.Warn(err.logger, "Timed out waiting for task cleanup")
	}

	logutil.Error(err.logger, "Panicking... Error message: %s", err.Error())
	panic(err)
}

// Task cleanup in case of failure
func abortTasks(trogdorCoordinatorHost string) {
	abortErr := <-abortChannel

	deletedTaskCount := 0
	logutil.Info(abortErr.logger, "Beginning to delete %d tasks", len(abortErr.taskIds))
	for _, taskId := range abortErr.taskIds {
		resp, body, err := trogdor.DeleteTask(trogdorCoordinatorHost, taskId)
		if err != nil {
			logutil.Debug(abortErr.logger, "error while trying to DELETE task %s. err: %s", taskId, err)
			continue
		}
		if code := resp.StatusCode; code > 299 {
			logutil.Debug(abortErr.logger, "error while trying to DELETE task %s. status code: %d, body: %s", taskId, code, body)
			continue
		}

		logutil.Debug(abortErr.logger, "Successfully deleted task %s", taskId)
		deletedTaskCount += 1
	}
	logutil.Info(abortErr.logger, "Successfully deleted %d tasks", len(abortErr.taskIds))
	abortGrp.Done()
}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func InitLogger(appName string) log.Logger {
	var logger log.Logger
	viper.BindEnv("DEBUG_LOGS")
	logger = log.NewJSONLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	logger = log.With(logger, "caller", log.Caller(4))
	logger = log.With(logger, "app", appName)
	if viper.GetBool("DEBUG_LOGS") {
		logger = level.NewFilter(logger, level.AllowDebug())
		logutil.Debug(logger, "enabled debug logs")
	} else {
		logger = level.NewFilter(logger, level.AllowInfo())
	}
	return logger
}

// returns a slice of the Trogdor agent pod names
func TrogdorAgentPodNames(trogdorAgentsCount int) []string {
	var agentNodes []string
	for agentID := 0; agentID < trogdorAgentsCount; agentID++ {
		agentNodes = append(agentNodes, fmt.Sprintf("cc-trogdor-service-agent-%d", agentID))
	}
	return agentNodes
}

func TimeToUnixMilli(t time.Time) uint64 {
	return uint64(t.UnixNano() / 1e6)
}
