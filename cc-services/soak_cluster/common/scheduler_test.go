package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

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

func TestSchedule(t *testing.T) {
	// 4 Jobs - A,B,C and D.
	// Given start time 0:00:02
	// B should start at 0:02:02 (ends at 0:03:02)
	// A should start at 0:02:32 (ends at 0:03:32)
	// C should start at 0:02:32 (ends at 0:04:02)
	// D should start at 0:06:02 (ends at 0:07:02)
	twoMinutes := time.Duration(120000) * time.Millisecond
	sixtySeconds := time.Duration(60000) * time.Millisecond
	thirtySeconds := time.Duration(30000) * time.Millisecond
	jobA := SchedulableJob{
		Name:          "A",
		Duration:      time.Duration(60000) * time.Millisecond,
		DelayDuration: thirtySeconds,
	}
	jobB := SchedulableJob{
		Name:          "B",
		Duration:      time.Duration(60000) * time.Millisecond,
		DelayDuration: twoMinutes,
	}
	jobC := SchedulableJob{
		Name:     "C",
		Duration: time.Duration(90000) * time.Millisecond,
	}
	jobD := SchedulableJob{
		Name:          "D",
		Duration:      sixtySeconds,
		DelayDuration: twoMinutes,
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
	assert.Equal(t, jobB.startTime, baseStart.Add(twoMinutes))
	assert.Equal(t, jobB.endTime, jobB.startTime.Add(sixtySeconds))
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
	assert.Equal(t, jobD.startTime, jobC.endTime.Add(twoMinutes))
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
	jobA.scheduled = true
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
