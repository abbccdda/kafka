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
	if !job.scheduled {
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
		if traversedJobsThisIteration[depJob.Name] && !depJob.scheduled {
			// we have a cycle, we're trying to go back in the graph
			return errors.New("dependency cycle in jobs detected")
		}
		err := s.traverseJobs(depJob, traversedJobsThisIteration)
		if err != nil {
			return err
		}
	}

	job.schedule(s.startTime)
	return nil
}

type SchedulableJob struct {
	Name          string
	Duration      time.Duration
	DelayDuration time.Duration

	startTime          time.Time
	scheduled          bool
	endTime            time.Time
	scheduleAfterStart []*SchedulableJob
	scheduleAfterEnd   []*SchedulableJob
}

func (j *SchedulableJob) schedule(baseStartTime time.Time) {
	if len(j.scheduleAfterEnd) == 0 && len(j.scheduleAfterStart) == 0 {
		j.startTime = baseStartTime.Add(j.DelayDuration)
	} else if len(j.scheduleAfterEnd) != 0 {
		latestEndTime := baseStartTime
		for _, job := range j.scheduleAfterEnd {
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
	j.endTime = j.startTime.Add(j.Duration)
	j.scheduled = true
}

func (j *SchedulableJob) ScheduleAfterJobEnd(jobs []*SchedulableJob) error {
	if len(j.scheduleAfterStart) != 0 {
		return errors.New("cannot schedule this job after other jobs because it is already scheduled before some others")
	}
	for _, schedJob := range jobs {
		if schedJob.Name == j.Name {
			return errors.New("cannot schedule job after itself")
		}
	}
	j.scheduleAfterEnd = jobs
	return nil
}

func (j *SchedulableJob) ScheduleAfterJobStart(jobs []*SchedulableJob) error {
	if len(j.scheduleAfterEnd) != 0 {
		return errors.New("cannot schedule this job before other jobs because it is already scheduled after some others")
	}
	for _, schedJob := range jobs {
		if schedJob.Name == j.Name {
			return errors.New("cannot schedule job after itself")
		}
	}
	j.scheduleAfterStart = jobs
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
