package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/streadway/amqp"
)

// RabbitMQ server connection parameters
const (
	rabbitMQHost            = "ta-vm-01.ticketattendant.com"
	rabbitMQUsername        = "IOS"
	rabbitMQPassword        = "17a2f860-9e0d-4dd5-8869-25361a582b1d"
	rabbitMQQueueName       = "IOSQueue_job"
	rabbitMQOutputQueueName = "QueueManager.MessageQueue.POSLowPriorityQueue+Message, QueueManager_job"
)

var (
	mu      sync.Mutex
	jobData []JobData
)

type RabbitMQManager struct {
	channel *amqp.Channel
}
type JobData struct {
	JobId       string `json:"JobId"`
	JobSettings struct {
		// Define your JobSettings structure based on the actual JSON structure
	} `json:"JobSettings"`
}

func InitRabbitMQConnection() (*RabbitMQManager, error) {
	fmt.Println("Connecting RabbitMQ Server")

	// Construct RabbitMQ connection URL
	amqpURL := fmt.Sprintf("amqp://%s:%s@%s/", rabbitMQUsername, rabbitMQPassword, rabbitMQHost)

	// Establish a connection to the RabbitMQ server
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
		return nil, err
	}
	// defer conn.Close()
	fmt.Println("Connected to RabbitMQ")

	// Create a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
		return nil, err
	}
	// defer ch.Close()
	fmt.Println("Opened the RabbitMQ channel")

	// Declare a queue
	_, err = ch.QueueDeclare(
		rabbitMQQueueName, // queue name
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
		return nil, err
	}
	fmt.Println("Declared the iOS RabbitMQ queue")

	// Declare a queue
	_, err = ch.QueueDeclare(
		rabbitMQOutputQueueName, // queue name
		true,                    // durable
		false,                   // delete when unused
		false,                   // exclusive
		false,                   // no-wait
		nil,                     // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
		return nil, err
	}
	fmt.Println("Declared the Queue Name RabbitMQ queue")

	return &RabbitMQManager{channel: ch}, nil
	// fmt.Println("Shutting down gracefully...")
}

func processMessage(body []byte) {
	fmt.Println("Processing Message.")
	var newJobData JobData

	// Unmarshal JSON data into the JobData struct
	err := json.Unmarshal(body, &newJobData)
	if err != nil {
		log.Printf("Failed to unmarshal JSON: %v", err)
		return
	}

	// Print the extracted JobId and JobSettings
	fmt.Printf("Received JobId: %s\n", newJobData.JobId)

	// Update the global variable with the latest JobData
	addJobData(newJobData)
}

func ConsumeMessages(rmq *RabbitMQManager) {
	// Consume messages from the queue
	msgs, err := rmq.channel.Consume(
		rabbitMQQueueName, // queue name
		"",                // consumer
		true,              // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)

	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
		fmt.Println("failed to")
	}
	fmt.Println("Registered the RabbitMQ consumer")

	// Handle incoming messages
	for d := range msgs {
		go processMessage(d.Body)
	}

	// Wait for a signal to gracefully exit
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
}

func ProcessOutputMessage(rmq *RabbitMQManager, jobId int) error {
	body := fmt.Sprintf(`{"JobId":"%d","JobSettings":{}}`, jobId)

	fmt.Printf("Received JobId: %s\n", body)

	if rmq.channel == nil {
		fmt.Println("RabbitMQ channel is not initialized")
		return nil
	}
	mu.Lock()
	defer mu.Unlock()
	print(rmq.channel)
	err := rmq.channel.Publish(
		"",                      // Exchange
		rabbitMQOutputQueueName, // Routing key
		false,                   // Mandatory
		false,                   // Immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(body),
		},
	)
	if err != nil {
		return fmt.Errorf("Failed to publish a message: %v", err)
	}
	return nil
}

func GetCurrentJobData() []JobData {
	// Retrieve the current job data
	mu.Lock()
	currentJobData := jobData
	mu.Unlock()

	return currentJobData
}

func addJobData(newJobData JobData) {
	mu.Lock()
	// Check if there is already an element with the same JobId
	for _, existingJobData := range jobData {
		if existingJobData.JobId == newJobData.JobId {
			// If there is a matching JobId, do nothing
			fmt.Printf("JobId %s already exists in the array\n", newJobData.JobId)
			mu.Unlock()
			return
		}
	}

	// If no matching JobId is found, add the new instance to the array
	jobData = append(jobData, newJobData)
	fmt.Printf("JobId %s added to the array\n", newJobData.JobId)
	mu.Unlock()
}

func GetAndRemoveFirstJobData() (JobData, error) {
	print("Queued JobData: ", jobData)

	mu.Lock()
	defer mu.Unlock()

	print("Job Data length: ", len(jobData))
	if len(jobData) == 0 {
		jobData = []JobData{
			{
				JobId: "7879880",
				JobSettings: struct {
					// Initialize JobSettings based on the actual JSON structure
				}{
					// Initialize JobSettings fields here
				},
			},
		}
		// If the array is empty, return an error indicating no jobs are available
		// return JobData{}, fmt.Errorf("no jobs available")
	}

	// Get the first element from the array
	firstJobData := jobData[0]
	// firstJobData := JobData{
	// 	JobId: "7946305",
	// 	JobSettings: struct {
	// 		// Initialize JobSettings based on the actual JSON structure
	// 	}{
	// 		// Initialize JobSettings fields here
	// 	},
	// }
	fmt.Println("First Job Data: ", firstJobData)

	// Remove the first element from the array
	jobData = jobData[1:]

	return firstJobData, nil
}
