package utils

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"ticket-crawler/utils/model"

	_ "github.com/denisenkom/go-mssqldb"
)

// DBConn holds the database connection
var DBConnVeritas *sql.DB
var DBConnVulcan *sql.DB

const (
	server    = "ta-vm-02.ticketattendant.com"
	port      = 1433
	user      = "sa"
	password  = "Johnson0069"
	dbVeritas = "Veritas"
	dbVulcan  = "Vulcan"
)

// InitDB initializes the MSSQL database connection
func InitDB() error {
	connStringVeritas := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, dbVeritas)

	var err error
	DBConnVeritas, err = sql.Open("sqlserver", connStringVeritas)
	if err != nil {
		return err
	}

	err = DBConnVeritas.Ping()
	if err != nil {
		return err
	}

	fmt.Println("Connected to MSSQL server with Veritas!")

	connStringVulcan := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, dbVulcan)

	DBConnVulcan, err = sql.Open("sqlserver", connStringVulcan)
	if err != nil {
		return err
	}

	err = DBConnVulcan.Ping()
	if err != nil {
		return err
	}

	fmt.Println("Connected to MSSQL server with Vulcan!")
	return nil
}

// ExecuteQuery executes a SQL query and returns the result
func ExecuteQuery(DBConn *sql.DB, query string) (*sql.Rows, error) {
	rows, err := DBConn.Query(query)
	if err != nil {
		log.Println("Error executing query:", err)
		return nil, err
	}
	return rows, nil
}

// convertToString converts an interface{} value to a string
func convertToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func getResultSet(DBConn *sql.DB, query string, args ...any) ([]map[string]interface{}, error) {
	rows, err := DBConn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := []map[string]interface{}{}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			entry[col] = values[i]
		}

		result = append(result, entry)
	}

	// fmt.Println("Query result: ", result)
	return result, nil
}

func GetCrawlInfoByJobId(JobId string) (*model.CrawlerInfo, error) {
	sql := `
	SELECT
		j.JobId,
		j.JobTypeId,
		j.JobStatusId,
		j.DateCreated,
		j.DateUpdated,
		j.DateCompleted,
		j.ClientId,
		j.SubmittedByApplicationId,
		jt.QueueTypeId,
		js.Name,
		js.Value 
	FROM
		Job j
		LEFT OUTER JOIN JobSetting js ON j.JobId = js.JobId
		LEFT OUTER JOIN JobType jt ON jt.JobTypeId = j.JobTypeId 
	WHERE
		j.JobId = @p1
	`

	fmt.Println("Getting Cralwer Info JobId: ", JobId)
	result, error := getResultSet(DBConnVulcan, sql, JobId)
	if error != nil {
		fmt.Println("Error:", error)
		return nil, error
	}

	clientAccountId := ""
	ballparkDownloadJobId := ""
	eventDateFrom := ""
	eventDateTo := ""

	for _, item := range result {
		itemName, _ := item["Name"].(string)
		if itemName == "ClientAccountId" {
			clientAccountId = convertToString(item["Value"])
		}
		if itemName == "BallparkDownloadJobId" {
			ballparkDownloadJobId = convertToString(item["Value"])
		}
		if itemName == "EventDateFrom" {
			eventDateFrom = convertToString(item["Value"])
		}
		if itemName == "EventDateTo" {
			eventDateTo = convertToString(item["Value"])
		}
	}

	fmt.Println("ClientAccountId: ", clientAccountId)
	fmt.Println("BallparkDownloadJobId: ", ballparkDownloadJobId)
	fmt.Println("EventDateFrom: ", eventDateFrom)
	fmt.Println("EventDateTo: ", eventDateTo)

	UserName := ""
	Password := ""
	Url := ""
	clientInfoSQL := `
	SELECT
		ClientAccountId, Username, Password, Url
	FROM
		ClientAccount c
		JOIN ClientAccountType ct ON c.ClientAccountTypeId = ct.ClientAccountTypeId 
	WHERE
		c.ClientAccountId = @p1
	`

	clientInfoResult, clientInfoError := getResultSet(DBConnVeritas, clientInfoSQL, clientAccountId)
	if clientInfoError != nil {
		return nil, clientInfoError
	}
	if len(clientInfoResult) < 1 {
		return nil, errors.New("Fialed to get client info")
	} else {
		clientInfo := clientInfoResult[0]
		UserName, _ = clientInfo["Username"].(string)
		Password, _ = clientInfo["Password"].(string)
		Url, _ = clientInfo["Url"].(string)
	}

	fmt.Println("Username: ", UserName)
	fmt.Println("Password: ", Password)
	fmt.Println("Url: ", Url)

	ballParkSql := "SELECT * FROM BallparkTicket WHERE JobId = @p1"
	ballParkResult, ballParkError := getResultSet(DBConnVeritas, ballParkSql, ballparkDownloadJobId)
	if ballParkError != nil {
		return nil, ballParkError
	}

	// fmt.Println("BallparkTicket result: ", ballParkResult)

	crawlerInfo := &model.CrawlerInfo{
		JobId:                 JobId,
		Url:                   Url,
		UserName:              UserName,
		Password:              Password,
		EventDateFrom:         eventDateFrom,
		EventDateTo:           eventDateTo,
		BallparkDownloadJobId: ballparkDownloadJobId,
		ClientId:              clientAccountId,
		SeatInfos:             []model.SeatInfo{},
	}

	for _, item := range ballParkResult {
		fmt.Println("BallparkTicket item: ", ballParkResult)
		BallparkTicketId, _ := item["BallparkTicketId"].(int64)
		Section, _ := item["Section"].(string)
		Row, _ := item["Row"].(string)
		Seat, _ := item["Seat"].(string)
		EventName, _ := item["EventName"].(string)
		seatInfo := model.SeatInfo{
			BallparkTicketId: convertToString(BallparkTicketId),
			Section:          Section,
			Row:              Row,
			Seat:             Seat,
			EventName:        EventName,
		}
		crawlerInfo.SeatInfos = append(crawlerInfo.SeatInfos, seatInfo)
	}

	return crawlerInfo, nil
}
func SetJobStatus(ballparkDownloadId string, message string, clientID string, rmq *RabbitMQManager) (bool, error) {
	var stmt *sql.Stmt
	var err error

	if message == "success" {
		stmt, err = DBConnVulcan.Prepare("UPDATE Job SET JobStatusId = 10, DateUpdated = GETUTCDATE() WHERE JobId = @ballparkDownloadId")
	} else {
		stmt, err = DBConnVulcan.Prepare("UPDATE Job SET JobStatusId = 99, DateUpdated = GETUTCDATE() WHERE JobId = @ballparkDownloadId")
	}
	if err != nil {
		return false, fmt.Errorf("Error preparing SQL statement: %v", err)
	}
	defer stmt.Close()
	//sql.Named("tokenValue", tokenValue), sql.Named("ballparkTicketId", ballparkTicketId
	_, err = stmt.Exec(sql.Named("ballparkDownloadId", ballparkDownloadId))
	if err != nil {
		return false, fmt.Errorf("Error executing SQL statement: %v", err)
	}

	isSuccess := message == "success" // Assuming isSuccess is determined by the message
	if !isSuccess {
		stmt, err = DBConnVulcan.Prepare("UPDATE JobDetail SET UserErrorReference = @message WHERE JobId = @ballparkDownloadId")
		if err != nil {
			return false, fmt.Errorf("Error preparing SQL statement: %v", err)
		}
		defer stmt.Close()

		_, err = stmt.Exec(sql.Named("message", message), sql.Named("ballparkDownloadId", ballparkDownloadId))
		if err != nil {
			return false, fmt.Errorf("Error executing SQL statement: %v", err)
		}
	}

	// third logic
	if isSuccess || strings.Contains(message, "Not able to find") {
		// get client id

		stmt, err := DBConnVulcan.Prepare(`
			INSERT INTO Job (JobTypeId, JobStatusId, DateCreated, ClientId, SubmittedByApplicationId)
			VALUES (801, 1, GETUTCDATE(), @p1, 46);
			SELECT CAST(SCOPE_IDENTITY() as int);
		`)
		if err != nil {
			return false, fmt.Errorf("Error preparing SQL statement: %v", err)
		}
		defer stmt.Close()

		var newId int
		err = stmt.QueryRow(clientID).Scan(&newId) // Pass clientId as argument
		if err != nil {
			// Handle error
			return false, fmt.Errorf("Error executing SQL statement: %v", err)
		}

		insertJobDetailstmt, err := DBConnVulcan.Prepare(`
			INSERT INTO JobDetail (JobId, UserJobDetails)
			VALUES (@p1, 'Attach Job Sent from the IOS App')
		`)
		_, err = insertJobDetailstmt.Exec(newId)
		if err != nil {
			// Handle error
			return false, fmt.Errorf("Error executing SQL statement: %v", err)
		}

		insertJobSettingstmt, err := DBConnVulcan.Prepare(`
			INSERT INTO JobSetting (JobId, Name, Value)
			VALUES (@jobID, 'BallparkDownloadJobId', @ballparkDownloadId)
		`)
		_, err = insertJobSettingstmt.Exec(sql.Named("ballparkDownloadId", ballparkDownloadId), sql.Named("jobID", newId))
		if err != nil {
			// Handle error
			return false, fmt.Errorf("Error executing SQL statement: %v", err)
		}

		err = ProcessOutputMessage(rmq, 180)
		if err != nil {
			return false, fmt.Errorf("Error pushing to RabbitMQ queue: %v", err)
		}
	}

	return true, nil
}

func InsertBarcodeToken(tokenValue string, ballparkTicketId string) (bool, int64, string, error) {

	stmt, err := DBConnVeritas.Prepare("UPDATE BallparkTicket SET BarcodeToken = @tokenValue WHERE BallparkTicketId = @ballparkTicketId")
	if err != nil {
		return false, -1, "", fmt.Errorf("Error preparing SQL statement: %v", err)
	}
	defer stmt.Close()

	// Execute the prepared statement. Replace 'tokenValue' and 'ballparkTicketId' with actual values
	res, err := stmt.Exec(sql.Named("tokenValue", tokenValue), sql.Named("ballparkTicketId", ballparkTicketId))
	if err != nil {
		return false, -1, "", fmt.Errorf("Error executing SQL statement: %v", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, -1, "", fmt.Errorf("Error getting rows affected: %v", err)
	}

	// Check if the update affected any rows
	if rowsAffected == 0 {
		// No rows were updated, handle accordingly
		return false, -1, "", fmt.Errorf("No rows were updated")
	} else {
		return true, rowsAffected, tokenValue, nil
	}

}
