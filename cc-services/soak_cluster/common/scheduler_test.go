package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var oneMinDur = time.Duration(60000) * time.Millisecond
var twoMinDur = time.Duration(120000) * time.Millisecond

func TestScheduleDetectsCycles(t *testing.T) {
	// C->B->A->C - a cyclic graph
	jobA := SchedulableJob{
		Name:     "A",
		Duration: time.Duration(30000) * time.Millisecond,
	}
	jobB := SchedulableJob{
		Name:     "B",
		Duration: time.Duration(30000) * time.Millisecond,
	}
	jobC := SchedulableJob{
		Name:     "C",
		Duration: time.Duration(30000) * time.Millisecond,
	}
	err := jobC.ScheduleAfterJobStart([]*SchedulableJob{&jobB})
	assert.Nil(t, err)
	err = jobB.ScheduleAfterJobStart([]*SchedulableJob{&jobA})
	assert.Nil(t, err)
	err = jobA.ScheduleAfterJobStart([]*SchedulableJob{&jobC})
	assert.Nil(t, err)

	scheduler := NewScheduler(time.Now())
	scheduler.AddJob(&jobA)
	scheduler.AddJob(&jobB)
	scheduler.AddJob(&jobC)

	err = scheduler.Schedule()

	assert.NotNil(t, err)
}

// 3 Jobs - A, B and C. A and B have a defined duration.
// A is scheduled to start after 1 minute
// B is scheduled to start after the end of A.
// C is scheduled to start 1 minute after the start of A and until the latest of A and B.
// Given start time 0:00:00
// A should start at 0:00:01 (end at 0:00:03)
// B should start at 0:00:03 (end at 0:00:05)
// C should start at 0:00:02 (end at 0:00:05)
func TestScheduleRunUntil(t *testing.T) {
	// arrange
	jobA := SchedulableJob{
		Name:          "A",
		Duration:      twoMinDur,
		DelayDuration: oneMinDur,
	}
	jobB := SchedulableJob{
		Name:     "B",
		Duration: twoMinDur,
	}
	jobC := SchedulableJob{
		Name: "C",
		// no Duration: specified
		DelayDuration: oneMinDur,
	}
	err := jobB.ScheduleAfterJobEnd([]*SchedulableJob{&jobA})
	assert.Nil(t, err)

	err = jobC.ScheduleAfterJobStart([]*SchedulableJob{&jobA})
	assert.Nil(t, err)
	err = jobC.RunUntilEndOf([]*SchedulableJob{&jobA, &jobB})
	assert.Nil(t, err)

	baseStart := time.Now()
	scheduler := NewScheduler(baseStart)
	scheduler.AddJob(&jobA)
	scheduler.AddJob(&jobB)
	scheduler.AddJob(&jobC)

	// act
	err = scheduler.Schedule()

	// assert
	assert.Nil(t, err)
	assert.Equal(t, baseStart.Add(oneMinDur), jobA.startTime)
	assert.Equal(t, baseStart.Add(oneMinDur).Add(twoMinDur), jobA.endTime) // 0:00:03
	assert.Equal(t, jobA.endTime.Sub(jobA.startTime), jobA.Duration)

	assert.Equal(t, jobA.endTime, jobB.startTime)
	assert.Equal(t, jobA.endTime.Add(twoMinDur), jobB.endTime)
	assert.Equal(t, jobB.endTime.Sub(jobB.startTime), jobB.Duration)

	assert.Equal(t, jobA.startTime.Add(oneMinDur), jobC.startTime)
	assert.Equal(t, jobB.endTime, jobC.endTime)
	assert.Equal(t, jobC.endTime.Sub(jobC.startTime), jobC.Duration)
}

// 3 Jobs - A, B and C. A and B have a defined duration.
// A is scheduled to start after 1 minute
// B is scheduled to start after the end of A.
// C is scheduled to start at the very beginning and run until the latest of A and B.
// Given start time 0:00:00
// A should start at 0:00:01 (end at 0:00:03)
// B should start at 0:00:03 (end at 0:00:05)
// C should start at 0:00:00 (end at 0:00:05)
func TestScheduleRunUntilEarlyStart(t *testing.T) {
	// arrange
	jobA := SchedulableJob{
		Name:          "A",
		Duration:      twoMinDur,
		DelayDuration: oneMinDur,
	}
	jobB := SchedulableJob{
		Name:     "B",
		Duration: twoMinDur,
	}
	jobC := SchedulableJob{
		Name: "C",
		// no Duration: specified
	}
	err := jobB.ScheduleAfterJobEnd([]*SchedulableJob{&jobA})
	assert.Nil(t, err)

	err = jobC.RunUntilEndOf([]*SchedulableJob{&jobA, &jobB})
	assert.Nil(t, err)

	baseStart := time.Now()
	scheduler := NewScheduler(baseStart)
	scheduler.AddJob(&jobA)
	scheduler.AddJob(&jobB)
	scheduler.AddJob(&jobC)

	// act
	err = scheduler.Schedule()

	// assert
	assert.Nil(t, err)
	assert.Equal(t, baseStart.Add(oneMinDur), jobA.startTime)
	assert.Equal(t, baseStart.Add(oneMinDur).Add(twoMinDur), jobA.endTime) // 0:00:03
	assert.Equal(t, jobA.endTime.Sub(jobA.startTime), jobA.Duration)

	assert.Equal(t, jobA.endTime, jobB.startTime)
	assert.Equal(t, jobA.endTime.Add(twoMinDur), jobB.endTime)
	assert.Equal(t, jobB.endTime.Sub(jobB.startTime), jobB.Duration)

	assert.Equal(t, baseStart, jobC.startTime)
	assert.Equal(t, jobB.endTime, jobC.endTime)
	assert.Equal(t, jobC.endTime.Sub(baseStart), jobC.Duration)
}

// 3 Jobs - A, B and C. A and B have a defined duration.
// A is scheduled to start after 1 minute
// B is scheduled to start after the end of A.
// C is scheduled to start at the after A and run until B.
// Given start time 0:00:00
// A should start at 0:00:01 (end at 0:00:03)
// B should start at 0:00:03 (end at 0:00:05)
// C should start at 0:00:03 (end at 0:00:05)
func TestScheduleRunUntilLateStart(t *testing.T) {
	// arrange
	jobA := SchedulableJob{
		Name:          "A",
		Duration:      twoMinDur,
		DelayDuration: oneMinDur,
	}
	jobB := SchedulableJob{
		Name:     "B",
		Duration: twoMinDur,
	}
	jobC := SchedulableJob{
		Name: "C",
		// no Duration: specified
	}
	err := jobB.ScheduleAfterJobEnd([]*SchedulableJob{&jobA})
	assert.Nil(t, err)

	err = jobC.ScheduleAfterJobEnd([]*SchedulableJob{&jobA})
	assert.Nil(t, err)
	err = jobC.RunUntilEndOf([]*SchedulableJob{&jobB})
	assert.Nil(t, err)

	baseStart := time.Now()
	scheduler := NewScheduler(baseStart)
	scheduler.AddJob(&jobA)
	scheduler.AddJob(&jobB)
	scheduler.AddJob(&jobC)

	// act
	err = scheduler.Schedule()

	// assert
	assert.Nil(t, err)
	assert.Equal(t, baseStart.Add(oneMinDur), jobA.startTime)
	assert.Equal(t, baseStart.Add(oneMinDur).Add(twoMinDur), jobA.endTime) // 0:00:03
	assert.Equal(t, jobA.endTime.Sub(jobA.startTime), jobA.Duration)

	assert.Equal(t, jobA.endTime, jobB.startTime)
	assert.Equal(t, jobA.endTime.Add(twoMinDur), jobB.endTime)
	assert.Equal(t, jobB.endTime.Sub(jobB.startTime), jobB.Duration)

	assert.Equal(t, jobA.endTime, jobC.startTime)
	assert.Equal(t, jobB.endTime, jobC.endTime)
	assert.Equal(t, jobC.endTime.Sub(jobC.startTime), jobC.Duration)
}

// Two jobs - A and B. A is scheduled to run after B. B is scheduled to run until A. This is invalid
func TestScheduleJobCannotRunUntilAJobScheduledToStartAfterIt(t *testing.T) {
	// arrange
	jobA := SchedulableJob{
		Name:     "A",
		Duration: twoMinDur,
	}
	jobB := SchedulableJob{
		Name: "B",
	}
	err := jobA.ScheduleAfterJobEnd([]*SchedulableJob{&jobB})
	assert.Nil(t, err)
	err = jobB.RunUntilEndOf([]*SchedulableJob{&jobA})
	assert.Nil(t, err)

	baseStart := time.Now()
	scheduler := NewScheduler(baseStart)
	scheduler.AddJob(&jobA)
	scheduler.AddJob(&jobB)

	// act
	err = scheduler.Schedule()

	// assert
	assert.NotNil(t, err)
}

func TestSchedule(t *testing.T) {
	// 4 Jobs - A,B,C and D.
	// Given start time 0:00:02
	// B should start at 0:02:02 (ends at 0:03:02)
	// A should start at 0:02:32 (ends at 0:03:32)
	// C should start at 0:02:32 (ends at 0:04:02)
	// D should start at 0:06:02 (ends at 0:07:02)
	thirtySeconds := time.Duration(30000) * time.Millisecond
	jobA := SchedulableJob{
		Name:          "A",
		Duration:      time.Duration(60000) * time.Millisecond,
		DelayDuration: thirtySeconds,
	}
	jobB := SchedulableJob{
		Name:          "B",
		Duration:      time.Duration(60000) * time.Millisecond,
		DelayDuration: twoMinDur,
	}
	jobC := SchedulableJob{
		Name:     "C",
		Duration: time.Duration(90000) * time.Millisecond,
	}
	jobD := SchedulableJob{
		Name:          "D",
		Duration:      oneMinDur,
		DelayDuration: twoMinDur,
	}
	err := jobA.ScheduleAfterJobStart([]*SchedulableJob{&jobB})
	assert.Nil(t, err)
	err = jobC.ScheduleAfterJobStart([]*SchedulableJob{&jobA, &jobB})
	assert.Nil(t, err)
	err = jobD.ScheduleAfterJobEnd([]*SchedulableJob{&jobC, &jobA})
	assert.Nil(t, err)

	baseStart := time.Now()
	scheduler := NewScheduler(baseStart)
	scheduler.AddJob(&jobA)
	scheduler.AddJob(&jobB)
	scheduler.AddJob(&jobC)
	scheduler.AddJob(&jobD)

	err = scheduler.Schedule()

	assert.Nil(t, err)
	assert.Equal(t, jobB.startTime, baseStart.Add(twoMinDur))
	assert.Equal(t, jobB.endTime, jobB.startTime.Add(oneMinDur))
	start, dur, end, err := scheduler.JobTimes(jobB.Name)
	assert.Nil(t, err)
	assert.Equal(t, jobB.startTime, start)
	assert.Equal(t, jobB.Duration, dur)
	assert.Equal(t, jobB.endTime, end)

	assert.Equal(t, jobA.startTime, jobB.startTime.Add(thirtySeconds))
	assert.Equal(t, jobA.endTime, jobA.startTime.Add(jobA.Duration))
	start, dur, end, err = scheduler.JobTimes(jobA.Name)
	assert.Nil(t, err)
	assert.Equal(t, jobA.startTime, start)
	assert.Equal(t, jobA.Duration, dur)
	assert.Equal(t, jobA.endTime, end)

	assert.Equal(t, jobC.startTime, jobA.startTime)
	assert.Equal(t, jobC.endTime, jobC.startTime.Add(jobC.Duration))
	start, dur, end, err = scheduler.JobTimes(jobC.Name)
	assert.Nil(t, err)
	assert.Equal(t, jobC.startTime, start)
	assert.Equal(t, jobC.Duration, dur)
	assert.Equal(t, jobC.endTime, end)

	assert.Equal(t, jobD.endTime, jobD.startTime.Add(jobD.Duration))
	assert.Equal(t, jobD.startTime, jobC.endTime.Add(twoMinDur))
	start, dur, end, err = scheduler.JobTimes(jobD.Name)
	assert.Nil(t, err)
	assert.Equal(t, jobD.startTime, start)
	assert.Equal(t, jobD.Duration, dur)
	assert.Equal(t, jobD.endTime, end)
}

func TestJobTimesThrowsErrors(t *testing.T) {
	jobA := SchedulableJob{Name: "A"}
	scheduler := NewScheduler(time.Now())
	_, _, _, err := scheduler.JobTimes("A") // not added
	assert.NotNil(t, err)
	scheduler.AddJob(&jobA)
	_, _, _, err = scheduler.JobTimes("A") // not scheduled
	assert.NotNil(t, err)
	jobA.startScheduled = true
	jobA.endScheduled = true
	_, _, _, err = scheduler.JobTimes("A")
	assert.Nil(t, err)
}

func TestJobCannotScheduleBothAfterStartAndEnd(t *testing.T) {
	jobA := SchedulableJob{Name: "A"}
	jobB := SchedulableJob{Name: "B"}
	jobC := SchedulableJob{Name: "C"}

	err := jobA.ScheduleAfterJobStart([]*SchedulableJob{&jobB})
	assert.Nil(t, err)
	assert.NotNil(t, jobA.scheduleAfterStart)
	err = jobA.ScheduleAfterJobEnd([]*SchedulableJob{&jobC})
	assert.NotNil(t, err)
	assert.Nil(t, jobA.scheduleAfterEnd)
}

func TestJobCannotScheduleBothAfterItself(t *testing.T) {
	jobA := SchedulableJob{Name: "A"}

	err := jobA.ScheduleAfterJobStart([]*SchedulableJob{&jobA})
	assert.NotNil(t, err)
	assert.Nil(t, jobA.scheduleAfterStart)
	err = jobA.ScheduleAfterJobEnd([]*SchedulableJob{&jobA})
	assert.NotNil(t, err)
	assert.Nil(t, jobA.scheduleAfterEnd)
}

func TestJobCannotRunUntilItself(t *testing.T) {
	jobA := SchedulableJob{Name: "A"}

	err := jobA.RunUntilEndOf([]*SchedulableJob{&jobA})
	assert.NotNil(t, err)
	assert.Nil(t, jobA.runUntilEndOf)
}

// If A runs until B, no job can run until A
func TestJobCannotRunUntilAnotherJobThatRunsUntil(t *testing.T) {
	jobA := SchedulableJob{Name: "A"}
	jobB := SchedulableJob{Name: "B"}
	jobC := SchedulableJob{Name: "C"}

	err := jobA.RunUntilEndOf([]*SchedulableJob{&jobB})
	assert.Nil(t, err)
	err = jobC.RunUntilEndOf([]*SchedulableJob{&jobA})
	assert.NotNil(t, err)
}

func TestJobCannotRunUntilAnotherJobWithoutADuration(t *testing.T) {
	// three jobs - A, B and C. A runs until 0:00:05. B runs until A, C runs until B.
	// to ease implementation and limit complexity, we do not support C running until B
	jobA := SchedulableJob{Name: "A"}
	jobB := SchedulableJob{Name: "B"}
	jobC := SchedulableJob{Name: "C"}

	err := jobB.RunUntilEndOf([]*SchedulableJob{&jobA})
	assert.Nil(t, err)
	err = jobC.RunUntilEndOf([]*SchedulableJob{&jobB})
	assert.NotNil(t, err)

	jobA = SchedulableJob{Name: "A"}
	jobB = SchedulableJob{Name: "B"}
	jobC = SchedulableJob{Name: "C"}

	err = jobC.RunUntilEndOf([]*SchedulableJob{&jobB})
	assert.Nil(t, err)
	err = jobB.RunUntilEndOf([]*SchedulableJob{&jobA})
	assert.NotNil(t, err)
}
