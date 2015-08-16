package goq

import (
	"encoding/json"
	"errors"
	"time"
)

type Status struct {
	Code     uint8
	Progress uint8
}

type Job struct {
	ID         string
	JSON       string
	ResultJSON string
	Status     *Status
	processor  Processor
	queueName  string
}

// Method to requeue job, will remove this job from failed job if exists.
func (j *Job) Retry() error {
	err := client.SRem(JOB_FAILED_PREFIX+j.queueName, j.JSON).Err()
	if err != nil {
		return errors.New("Failed to remove from failed jobs set of job " + j.ID + " : " + err.Error())
	}
	j.processor(j)

	return nil
}

// Method to fail job to failed job set.
// Also set the status Code to 3
func (j *Job) Fail() error {
	err := client.SAdd(JOB_FAILED_PREFIX+j.queueName, j.JSON).Err()
	if err != nil {
		return errors.New("Failed to add to failed jobs set of job " + j.ID + " : " + err.Error())
	}
	err = j.SetStatus(3, 0)
	if err != nil {
		return err
	}

	return nil
}

// Method to set this job Status locally and to redis.
// Code must be 0 for waiting, 1 for working, 2 for completed, and 3 for failed.
// Progress must be 0-100
func (j *Job) SetStatus(code, progress uint8) error {
	if code < 0 || code > 3 {
		return errors.New("Failed to set status code of job " + j.ID + " : code must be 0-3")
	}
	if progress < 0 || progress > 100 {
		return errors.New("Failed to set status progress of job " + j.ID + " : progress must be 0-100")
	}

	j.Status.Code = code
	j.Status.Progress = progress

	statusJSON, err := json.Marshal(j.Status)
	if err != nil {
		return errors.New("Failed to marshal status of job " + j.ID + " : " + err.Error())
	}

	err = client.Set(JOB_STATUS_PREFIX+j.ID, string(statusJSON), 0).Err()
	if err != nil {
		return errors.New("Failed to set status of job " + j.ID + " : " + err.Error())
	}

	return nil
}

// Method to update this job Status from redis.
func (j *Job) GetStatus() error {
	dataJSON, err := client.Get(JOB_STATUS_PREFIX + j.ID).Result()
	if err != nil {
		return errors.New("Failed to get status of job " + j.ID + " : " + err.Error())
	}

	err = json.Unmarshal([]byte(dataJSON), j.Status)
	if err != nil {
		return errors.New("Failed to unmarshal status of job " + j.ID + " : " + err.Error())
	}

	return nil
}

// Method to save this job result to redis with ttl in seconds.
func (j *Job) SetCache(ttl time.Duration) error {
	err := client.Set(JOB_CACHE_PREFIX+j.ID, j.ResultJSON, ttl).Err()
	if err != nil {
		return errors.New("Failed to set cache of job " + j.ID + " : " + err.Error())
	}

	return nil
}

// Method to check if this job result is cached.
func (j *Job) IsCached() (bool, error) {
	exists, err := client.Exists(JOB_CACHE_PREFIX + j.ID).Result()
	if err != nil {
		return false, errors.New("Failed to check existence of job " + j.ID + " : " + err.Error())
	}
	if !exists {
		return false, nil
	}

	return true, nil
}

// Method to load cached job result from redis.
func (j *Job) GetCache() error {
	// check if cached or not first
	cached, err := j.IsCached()
	if err != nil {
		return err
	}
	if !cached {
		return errors.New("Failed to get cache of job " + j.ID + " : job is not cached")
	}
	j.ResultJSON, err = client.Get(JOB_CACHE_PREFIX + j.ID).Result()
	if err != nil {
		return errors.New("Failed to get cache of job " + j.ID + " : " + err.Error())
	}

	return nil
}

// Function to get cache of job result json by job id.
// Returns existence, the JSON, and error.
func GetCache(id string) (bool, string, error) {
	j := &Job{
		ID: id,
	}

	exists, err := j.IsCached()
	if err != nil {
		return false, "", err
	}
	if !exists {
		return false, "", nil
	}

	err = j.GetCache()
	if err != nil {
		return false, "", err
	}

	return true, j.ResultJSON, nil
}
