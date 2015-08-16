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
	log.Println(status)

	time.Sleep(time.Second * 1)
}

func doJob(job *goq.Job) {
	log.Println(job.ID, job.JSON)
}

func errorHandler(err error) {
	panic(err)
}
