package lib

import "time"

// Country contains basic information about country
type Country struct {
	ID         int64
	Code       string
	GeoID      string
	Name       string
	Population int64
	Continent  string
}

// CovidStatisticsRecord contains daily COVID statistics per country
type CovidStatisticsRecord struct {
	Date       time.Time
	Cases      int
	Deaths     int
	Cumulative float64
	Country    *Country
}

// CovidStatisticsRecordResult contains statistics lookup result or possible error
type CovidStatisticsRecordResult struct {
	Result CovidStatisticsRecord
	Error  error
}
