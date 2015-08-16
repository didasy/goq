package goq

import (
	"encoding/base32"
	"encoding/json"
	"errors"
	"gopkg.in/redis.v3"
	"time"
)

const (
	JOB_STATUS_PREFIX = "goq:queue:job:status:"
	JOB_CACHE_PREFIX  = "goq:queue:job:cache:"
	JOB_FAILED_PREFIX = "goq:queue:job:failed:"
)

var (
	client *redis.Client
)

// Function signature for job processor
type Processor func(*Job)

// Function signature for error handler
type ErrorHandler func(error)

type ConnectionOptions struct {
	Addr         string
	Password     string
	DB           int64
	MaxRetries   int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
	PoolTimeout  time.Duration
	IdleTimeout  time.Duration
}

type Options struct {
	Connection   *ConnectionOptions
	Concurrency  uint8
	QueueName    string
	Processor    Processor
	ErrorHandler ErrorHandler
}

// Function to create new Queue struct
func New(opt *Options) *Queue {
	if client == nil {
		redisOpt := &redis.Options{
			Addr:         opt.Connection.Addr,
			Password:     opt.Connection.Password,
			DB:           opt.Connection.DB,
			MaxRetries:   opt.Connection.MaxRetries,
			DialTimeout:  opt.Connection.DialTimeout,
			ReadTimeout:  opt.Connection.ReadTimeout,
			WriteTimeout: opt.Connection.WriteTimeout,
			PoolSize:     opt.Connection.PoolSize,
			PoolTimeout:  opt.Connection.PoolTimeout,
			IdleTimeout:  opt.Connection.IdleTimeout,
		}
		client = redis.NewClient(redisOpt)
	}

	return &Queue{
		jobChannel:   make(chan string, 1000),
		concurrency:  opt.Concurrency,
		queueName:    opt.QueueName,
		processor:    opt.Processor,
		errorHandler: opt.ErrorHandler,
	}
}

type Queue struct {
	jobChannel   chan string
	concurrency  uint8
	queueName    string
	processor    Processor
	errorHandler ErrorHandler
}

type QueueStatus struct {
	QueueLength int64
}

// Method to get status of this queue
func (q *Queue) QueueStatus() (*QueueStatus, error) {
	if client != nil {
		queueLen, err := client.LLen(q.queueName).Result()
		if err != nil {
			return nil, err
		}

		return &QueueStatus{
			QueueLength: queueLen,
		}, nil
	}

	return nil, errors.New("Failed to queue status: no initialized client")
}

// Method to enqueue job to queue, returns job id
func (q *Queue) Enqueue(jobJSON string) (string, error) {
	var err error
	// push to queue
	err = client.RPush(q.queueName, jobJSON).Err()
	if err != nil {
		return "", err
	}

	// create status JSON
	statusJSON, err := json.Marshal(&Status{
		Code:     0,
		Progress: 0,
	})
	if err != nil {
		return "", err
	}
	// create id
	id := base32.StdEncoding.EncodeToString([]byte(jobJSON))
	// set status of this job
	err = client.Set(JOB_STATUS_PREFIX+id, string(statusJSON), 0).Err()
	if err != nil {
		return "", err
	}

	return id, nil
}

// Method to run the queue worker
func (q *Queue) Run() {
	for i := uint8(0); i < q.concurrency; i++ {
		go q.work()
	}
	for {
		// dequeue the job
		// jobJSONSlice will always be 2 length
		jobJSONSlice, err := client.BLPop(0, q.queueName).Result()
		if err != nil {
			q.errorHandler(err)
			continue
		}

		q.jobChannel <- jobJSONSlice[1]
	}
}

func (q *Queue) work() {
	for {
		jobJSON := <-q.jobChannel
		// create the id
		id := base32.StdEncoding.EncodeToString([]byte(jobJSON))
		// check status
		statusJSON, err := client.Get(JOB_STATUS_PREFIX + id).Result()
		if err != nil {
			q.errorHandler(errors.New("Failed to get status of job " + id + " : " + err.Error()))
			continue
		}
		// unmarshal the status
		status := &Status{}
		err = json.Unmarshal([]byte(statusJSON), status)
		if err != nil {
			q.errorHandler(errors.New("Failed to unmarshal status of job " + id + " : " + err.Error()))
			continue
		}
		// create a job
		job := &Job{
			ID:     id,
			JSON:   jobJSON,
			Status: status,
			processor: q.processor,
			queueName: q.queueName,
		}
		// process it
		q.processor(job)
	}
}
