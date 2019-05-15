package common

import (
	"errors"
	"fmt"
	"time"
)

type Scheduler struct {
	jobs          []*SchedulableJob
	jobsByName    map[string]*SchedulableJob
	startTime     time.Time
	traversedJobs map[string]bool
}

func NewScheduler(baseStartTime time.Time) *Scheduler {
	return &Scheduler{
		jobs:          []*SchedulableJob{},
		startTime:     baseStartTime,
		traversedJobs: make(map[string]bool),
		jobsByName:    make(map[string]*SchedulableJob),
	}
}

func (s *Scheduler) AddJob(job *SchedulableJob) {
	s.jobs = append(s.jobs, job)
	s.jobsByName[job.Name] = job
}

// JobTimes() returns the time a job should start, its duration and the time at which it should end
// it is an error to call this function if the given job has not been added to the Scheduler and Scheduler#Schedule() has not been called
func (s *Scheduler) JobTimes(jobName string) (startTime time.Time, duration time.Duration, endTime time.Time, err error) {
	job := s.jobsByName[jobName]
	if job == nil {
		return startTime, duration, endTime, fmt.Errorf("job %s does not exist in the scheduler", jobName)
	}
	if !job.scheduled() {
		return startTime, duration, endTime, fmt.Errorf("job %s has not been scheduled yet", jobName)
	}

	return job.startTime, job.Duration, job.endTime, nil
}

// Schedule() parses all of the jobs, validates/traverses the dependency graph (topology sort)
// 	and populates each job's expected startTime and endTime
func (s *Scheduler) Schedule() error {
	for _, job := range s.jobs {
		err := s.traverseJobs(job, make(map[string]bool))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) traverseJobs(job *SchedulableJob, traversedJobsThisIteration map[string]bool) error {
	if s.traversedJobs[job.Name] {
		return nil
	}

	s.traversedJobs[job.Name] = true
	traversedJobsThisIteration[job.Name] = true
	for _, depJob := range job.dependentJobs() {
		if traversedJobsThisIteration[depJob.Name] && !depJob.scheduled() {
			// we have a cycle, we're trying to go back in the graph
			return errors.New("dependency cycle in jobs detected")
		}
		err := s.traverseJobs(depJob, traversedJobsThisIteration)
		if err != nil {
			return err
		}
	}

	_, err := job.schedule(s.startTime)
	return err
}

type SchedulableJob struct {
	Name          string
	Duration      time.Duration
	DelayDuration time.Duration

	startTime          time.Time
	startScheduled     bool
	endScheduled       bool
	endTime            time.Time
	scheduleAfterStart []*SchedulableJob
	scheduleAfterEnd   []*SchedulableJob
	runUntilEndOf      []*SchedulableJob

	// scheduledUntilEnd holds jobs that are scheduled to run until this job's end
	scheduledUntilEnd []*SchedulableJob
}

func (j *SchedulableJob) scheduled() bool {
	return j.startScheduled && j.endScheduled
}

func (j *SchedulableJob) schedule(baseStartTime time.Time) (scheduled bool, err error) {
	if len(j.scheduleAfterEnd) == 0 && len(j.scheduleAfterStart) == 0 {
		j.startTime = baseStartTime.Add(j.DelayDuration)
	} else if len(j.scheduleAfterEnd) != 0 {
		latestEndTime := baseStartTime
		for _, job := range j.scheduleAfterEnd {
			if !job.scheduled() {
				return false, fmt.Errorf("job %s is not scheduled. %s cannot start after it",
					job.Name, j.Name)
			}
			if job.endTime.After(latestEndTime) {
				latestEndTime = job.endTime
			}
		}
		j.startTime = latestEndTime.Add(j.DelayDuration)
	} else {
		latestStartTime := baseStartTime
		for _, job := range j.scheduleAfterStart {
			if job.startTime.After(latestStartTime) {
				latestStartTime = job.startTime
			}
		}
		j.startTime = latestStartTime.Add(j.DelayDuration)
	}
	j.startScheduled = true

	if len(j.runUntilEndOf) == 0 {
		j.endTime = j.startTime.Add(j.Duration)
		j.endScheduled = true

		for _, depJob := range j.scheduledUntilEnd {
			if depJob.endScheduled && depJob.endTime.After(j.endTime) {
				// that job is scheduled to run until a job that runs longer than this one
				continue
			}
			depJob.endTime = j.endTime
			if depJob.startScheduled {
				// update the duration if the task had its start time scheduled already
				depJob.Duration = j.endTime.Sub(depJob.startTime)
			}
			depJob.endScheduled = true

			// it is okay to not check depJob scheduledUntilEnd since we do not support such dependencies
		}
	} else {
		// should be scheduled by another tasks' schedule() method
		// check and update duration if end time is scheduled
		if j.endScheduled {
			j.Duration = j.endTime.Sub(j.startTime)
		}
	}
	return j.scheduled(), nil
}

func (j *SchedulableJob) ScheduleAfterJobEnd(jobs []*SchedulableJob) error {
	if len(j.scheduleAfterStart) != 0 {
		return fmt.Errorf("cannot schedule job %s after other jobs because it is already scheduled before some others", j.Name)
	}
	for _, schedJob := range jobs {
		if schedJob.Name == j.Name {
			return fmt.Errorf("cannot schedule job %s after itself", j.Name)
		}
	}
	j.scheduleAfterEnd = jobs
	return nil
}

func (j *SchedulableJob) ScheduleAfterJobStart(jobs []*SchedulableJob) error {
	if len(j.scheduleAfterEnd) != 0 {
		return fmt.Errorf("cannot schedule job %s before other jobs because it is already scheduled after some others", j.Name)
	}
	for _, schedJob := range jobs {
		if schedJob.Name == j.Name {
			return fmt.Errorf("cannot schedule job %s after itself", j.Name)
		}
	}
	j.scheduleAfterStart = jobs
	return nil
}

// RunUntilEndOf() schedules a job J to run until the latest endTime of all the given jobs
func (j *SchedulableJob) RunUntilEndOf(jobs []*SchedulableJob) error {
	// validate
	if len(j.runUntilEndOf) != 0 {
		return fmt.Errorf("cannot schedule job %s to run until other jobs because it is already scheduled to run until some others", j.Name)
	}
	if len(j.scheduledUntilEnd) != 0 {
		return fmt.Errorf("cannot schedule job %s to run until other jobs because it already has jobs scheduled to run until itself", j.Name)
	}
	if j.Duration != 0 {
		return fmt.Errorf("cannot run until a certain job when job %s's duration is explicitly defined", j.Name)
	}
	for _, schedJob := range jobs {
		if schedJob.Name == j.Name {
			return fmt.Errorf("cannot schedule job %s to run until itself", j.Name)
		}
		if len(schedJob.runUntilEndOf) != 0 {
			return fmt.Errorf("cannot schedule job %s to run until a task without a defined duration", j.Name)
		}
	}

	// schedule
	j.runUntilEndOf = jobs
	for _, schedJob := range jobs {
		schedJob.scheduledUntilEnd = append(schedJob.scheduledUntilEnd, j)
	}

	return nil
}

func (j *SchedulableJob) dependentJobs() []*SchedulableJob {
	if len(j.scheduleAfterEnd) == 0 && len(j.scheduleAfterStart) == 0 {
		return []*SchedulableJob{}
	} else if len(j.scheduleAfterEnd) == 0 {
		return j.scheduleAfterStart
	}
	return j.scheduleAfterEnd
}
