package main

import (
	".."
	"log"
	"time"
)

func main() {
	var err error

	q := goq.New(&goq.Options{
		Connection: &goq.ConnectionOptions{
			Addr:     "localhost:6379",
			DB:       0,
			Password: "",
		},
		Concurrency:  1,
		QueueName:    "myqueue",
		Processor:    doJob,
		ErrorHandler: errorHandler,
	})

	go q.Run()

	id, err := q.Enqueue(`{"data":"test"}`)
	if err != nil {
		panic(err)
	}
	log.Println("Job id is: ", id)

	status, err := q.QueueStatus()
	if err != nil {
		panic(err)
	}
	log.Println("Queue length: ", status)

	time.Sleep(time.Second * 1)

	// load from cache
	exists, cacheJSON, err := goq.GetCache(`{"data":"test"}`)
	if err != nil {
		panic(err)
	}
	log.Println(exists, cacheJSON)
}

func doJob(job *goq.Job) {
	log.Println(job.ID, job.JSON)
	// save to cache
	job.ResultJSON = `{"result":"json"}`
	err := job.SetCache(time.Second * 5)
	if err != nil {
		panic(err)
	}
}

func errorHandler(queue *goq.Queue, jobJSON string, err error) {
	panic(err)
}
