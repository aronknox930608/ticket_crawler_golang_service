package model

// Token struct and functions related to Cralwer job
type SeatInfo struct {
	BallparkTicketId string
	Section          string
	Row              string
	Seat             string
	EventName        string
}

type CrawlerInfo struct {
	JobId                 string
	Url                   string
	UserName              string
	Password              string
	EventDateFrom         string
	EventDateTo           string
	BallparkDownloadJobId string
	ClientId              string
	SeatInfos             []SeatInfo
}
