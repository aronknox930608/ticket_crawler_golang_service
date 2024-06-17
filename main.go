package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"ticket-crawler/utils"
	"ticket-crawler/utils/model"

	"github.com/gin-gonic/gin"
)

func main() {
	// Initialize MSSQL connection
	err := utils.InitDB()
	if err != nil {
		log.Fatal("Error initializing database connection:", err)
	}

	rabbitMQManager, err := utils.InitRabbitMQConnection()

	go utils.ConsumeMessages(rabbitMQManager)
	if err != nil {
		log.Fatalf("Failed to initialize RabbitMQ: %v", err)
	}
	router := gin.Default()

	router.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusBadRequest, gin.H{"message": "This is the microservice base URL for ticket barcode cralwer."})
	})

	// Define API endpoint to fetch crawlerInfo
	router.GET("/getCrawlerDetails", func(c *gin.Context) {
		firstJob, err := utils.GetAndRemoveFirstJobData()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": false, "error": err.Error(), "data": nil})
			return
		}

		crawlerInfo, err := utils.GetCrawlInfoByJobId(firstJob.JobId)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": false, "error": err.Error(), "data": nil})
			return
		}

		jsonString, err := json.Marshal(crawlerInfo)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": false, "error": err.Error(), "data": nil})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": true, "data": string(jsonString)})
	})

	// Define API endpoint to insert token
	router.POST("/insert-token", func(c *gin.Context) {
		var requestBody struct {
			Token            string `form:"token" binding:"required"`
			BallparkTicketId string `form:"ballparkTicketId" binding:"required"`
		}

		if err := c.ShouldBind(&requestBody); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"status": false, "error": err.Error(), "data": nil})
			return
		}
		fmt.Println("Request body", requestBody)
		fmt.Println("Token: ", requestBody.Token)
		fmt.Println("BallparkTicketId: ", requestBody.BallparkTicketId)
		success, id, token, err := utils.InsertBarcodeToken(
			requestBody.Token,
			requestBody.BallparkTicketId,
		)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": success, "error": err.Error(), "data": nil})
			return
		}
		barCodeToken := model.BarcodeToken{
			Id:    id,
			Token: token,
		}
		jsonString, err := json.Marshal(barCodeToken)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": success, "error": err.Error(), "data": nil})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": success, "data": string(jsonString)})
	})
	// Define API endpoint to insert token
	router.POST("/set-job-status", func(c *gin.Context) {
		var requestBody struct {
			BallparkDownloadId string `form:"ballparkDownloadId" binding:"required"`
			Message            string `form:"message" binding:"required"`
			ClientId           string `form:"clientId" binding:"required"`
		}

		if err := c.ShouldBind(&requestBody); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"status": false, "error": err.Error(), "data": nil})
			return
		}
		fmt.Println("Request body:", requestBody)
		fmt.Println("Token: ", requestBody.Message)
		fmt.Println("BallparkTicketId: ", requestBody.BallparkDownloadId)
		success, err := utils.SetJobStatus(
			requestBody.BallparkDownloadId,
			requestBody.Message,
			requestBody.ClientId,
			rabbitMQManager,
		)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": success, "error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"status": success})
	})

	router.Run(":8080")
}

func GetJobs(c *gin.Context) {
	currentJobData := utils.GetCurrentJobData()

	// Perform further processing based on your requirements
	// For example, you can query MSSQL based on the extracted JobId
	// result, err := queryMSSQL(currentJobData.JobId)

	c.JSON(http.StatusOK, gin.H{"jobData": currentJobData})
}
