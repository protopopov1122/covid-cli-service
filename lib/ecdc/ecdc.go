package ecdc

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/protopopov1122/covidservice/lib"
)

// Record contains ECDC COVID statistics for a single country per day
type Record struct {
	Date            string `json:"dateRep"`
	Day             string `json:"day"`
	Month           string `json:"month"`
	Year            string `json:"year"`
	Cases           int    `json:"cases"`
	Deaths          int    `json:"deaths"`
	CountryName     string `json:"countriesAndTerritories"`
	GeoID           string `json:"geoId"`
	CountryCode     string `json:"countryterritoryCode"`
	Population      int64  `json:"popData2019"`
	Continent       string `json:"continentExp"`
	CumulativeCases string `json:"Cumulative_number_for_14_days_of_COVID-19_cases_per_100000"`
}

// Records structure contains ECDC COVID statistics snapshot
type Records struct {
	Records []Record `json:"records"`
}

type ecdcImporter struct {
	db              *lib.Database
	records         *Records
	lastRecordCache map[string]time.Time
}

// Timestamp extracts timestamp from record
func (record *Record) Timestamp() (time.Time, error) {
	day, err := strconv.ParseUint(record.Day, 10, 32)
	if err != nil {
		return time.Unix(0, 0), err
	}
	month, err := strconv.ParseUint(record.Month, 10, 32)
	if err != nil {
		return time.Unix(0, 0), err
	}
	year, err := strconv.ParseUint(record.Year, 10, 32)
	if err != nil {
		return time.Unix(0, 0), err
	}
	return time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.Local), nil
}

// LoadRecords parses incoming JSON into Records
func LoadRecords(r io.Reader) (*Records, error) {
	var result Records
	if err := json.NewDecoder(r).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// FetchRecords loads JSON records from specified url
func FetchRecords(url string) (*Records, error) {
	client := http.Client{}
	rsp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()
	return LoadRecords(rsp.Body)
}

// DataSource defines ECDC COVID data source
type DataSource struct {
	url     string
	records *Records
}

// NewDataSource constructs new ECDC COVID data source
func NewDataSource(url string) (*DataSource, error) {
	records, err := FetchRecords(url)
	if err != nil {
		return nil, err
	}
	return &DataSource{
		url:     url,
		records: records,
	}, nil
}

// Import imports data into database
func (ecdc *DataSource) Import(db *lib.Database) error {
	importer := ecdcImporter{
		db:              db,
		records:         ecdc.records,
		lastRecordCache: make(map[string]time.Time),
	}
	return importer.Import()
}

func (importer *ecdcImporter) Import() error {
	var recordsForImport []lib.CovidStatisticsRecord
	for _, record := range importer.records.Records {
		lastRecord, err := importer.lastRecordTime(record.CountryCode)
		if err != nil {
			return err
		}
		timestamp, err := record.Timestamp()
		if err != nil {
			return err
		}
		var cumulative float64 = 0
		if len(record.CumulativeCases) > 0 {
			cumulative, err = strconv.ParseFloat(record.CumulativeCases, 64)
			if err != nil {
				return err
			}
		}
		if !lastRecord.Before(timestamp) {
			continue
		}
		country, err := importer.db.PutCountry(record.CountryCode, record.GeoID, record.CountryName, record.Population, record.Continent)
		if err != nil {
			return err
		}
		recordsForImport = append(recordsForImport, lib.CovidStatisticsRecord{
			Country:    country,
			Date:       timestamp,
			Cases:      record.Cases,
			Deaths:     record.Deaths,
			Cumulative: cumulative,
		})
	}
	return importer.db.ImportRecords(recordsForImport)
}

func (importer *ecdcImporter) lastRecordTime(countryCode string) (time.Time, error) {
	_, ok := importer.lastRecordCache[countryCode]
	if !ok {
		timestamp, err := importer.db.LastRecordDate(countryCode)
		if err != nil {
			return time.Unix(0, 0), err
		}
		importer.lastRecordCache[countryCode] = timestamp
	}
	return importer.lastRecordCache[countryCode], nil
}
