// Copyright 2017 Netflix, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package conductor

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/lisuo3389/conductor-client-go/task"
)

var (
	hostname, hostnameError = os.Hostname()
)

func init() {
	if hostnameError != nil {
		log.Fatal("Could not get hostname")
	}
}

type TaskExecuteFunction func(t *task.Task) (*task.TaskResult, error)


type ConductorWorker struct {
	ConductorHttpClient *ConductorHttpClient
	ThreadCount         int
	PollingInterval     int
	ctxMap map[string]context.CancelFunc
}

func NewConductorWorker(baseUrl string, threadCount int, pollingInterval int) *ConductorWorker {
	conductorWorker := new(ConductorWorker)
	conductorWorker.ThreadCount = threadCount
	conductorWorker.PollingInterval = pollingInterval
	conductorHttpClient := NewConductorHttpClient(baseUrl)
	conductorWorker.ConductorHttpClient = conductorHttpClient
	conductorWorker.ctxMap = map[string]context.CancelFunc{}
	return conductorWorker
}

func (c *ConductorWorker) Execute(t *task.Task, executeFunction TaskExecuteFunction) {
		taskResult, err := executeFunction(t)
		if err != nil {
			log.Println("Error Executing task:", err.Error())
			taskResult.Status = task.FAILED
			taskResult.ReasonForIncompletion = err.Error()
		}
		taskResultJsonString, err := taskResult.ToJSONString()
		if err != nil {
			log.Println(err.Error())
			log.Println("Error Forming TaskResult JSON body")
			return
		}
		c.ConductorHttpClient.UpdateTask(taskResultJsonString)
}

func (c *ConductorWorker) UpdateTask( ctx context.Context, t *task.Task )  {
	taskResult := task.NewTaskResult(t)
	taskResult.Status = task.TaskResultStatus(task.IN_PROGRESS)
	taskResult.CallbackAfterSeconds = int64(t.ResponseTimeoutSeconds - 1)
	taskResultJsonString, err := taskResult.ToJSONString()
	if err != nil {
		log.Println(err.Error())
		log.Println("Error Forming TaskResult JSON body")
		return
	}
	c.ConductorHttpClient.UpdateTask(taskResultJsonString)
	for {
		select {
		case <- ctx.Done():
			return
		case <- time.After( time.Duration(t.ResponseTimeoutSeconds - 1) * time.Second):
			c.ConductorHttpClient.UpdateTask(taskResultJsonString)
		}
	}
}

func (c *ConductorWorker) PollAndExecute(ctx context.Context, taskType string, executeFunction TaskExecuteFunction ){
	for {
		select {
		case <- time.After(time.Duration(c.PollingInterval) * time.Microsecond):
			// Poll for Task taskType
			polled, err := c.ConductorHttpClient.PollForTask(taskType, hostname)
			if err != nil {
				log.Println("Error Polling task:", err.Error())
				continue
			}
			if polled == "" {
				log.Println("No task found for:", taskType)
				continue
			}

			// Parse Http response into Task
			parsedTask, err := task.ParseTask(polled)
			if err != nil {
				log.Println("Error Parsing task:", err.Error())
				continue
			}

			// Found a task, so we send an Ack
			_, ackErr := c.ConductorHttpClient.AckTask(parsedTask.TaskId, parsedTask.WorkerId)
			if ackErr != nil {
				log.Println("Error Acking task:", ackErr.Error())
				continue
			}

			taskCtx, cancel  := context.WithCancel(context.Background())
			c.ctxMap[parsedTask.TaskId] = cancel
			defer cancel()

			go c.UpdateTask(taskCtx, parsedTask)


			// Execute given function
			c.Execute(parsedTask, executeFunction)

			// quit ack
			cancel()
			delete(c.ctxMap, parsedTask.TaskId)
		 case <- ctx.Done():
		 	for taskId, cancel := range  c.ctxMap{
		 		log.Printf("quit ack %s done", taskId)
		 		cancel()
			}
		}
	}
}

func (c *ConductorWorker) Start(ctx context.Context , taskType string, executeFunction TaskExecuteFunction, wait bool) {
	log.Println("Polling for task:", taskType, "with a:", c.PollingInterval, "(ms) polling interval with", c.ThreadCount, "goroutines for task execution, with workerid as", hostname)
	for i := 1; i <= c.ThreadCount; i++ {
		go c.PollAndExecute(ctx, taskType, executeFunction)
	}

	// wait infinitely while the go routines are running
	if wait {
		select {}
	}
}
