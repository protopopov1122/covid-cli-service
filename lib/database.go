package lib

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const insertNewRecordSQL string = `
	INSERT
		INTO Cases (CountryId, Date, Cases, Deaths, Cumulative)
		VALUES (?, ?, ?, ?, ?)`

type dbStatementCache struct {
	InsertNewRecord *sql.Stmt
	SelectRecord    *sql.Stmt
}

// Database contains COVID case statistics
type Database struct {
	database     *sql.DB
	countryCache map[int64]*Country
	stmtCache    dbStatementCache
}

// CovidDataSource abstracts importable data source
type CovidDataSource interface {
	Import(db *Database) error
}

// CountryQueryType defines country query  (by id/geo id/name)
type CountryQueryType int

const (
	// CountryQueryByID defines country query by ID
	CountryQueryByID CountryQueryType = iota
	// CountryQueryByGeoID defines country query by Geo ID
	CountryQueryByGeoID
	// CountryQueryByName defines country query by name
	CountryQueryByName
)

// CountryQuery defines tagged union for database queries
type CountryQuery struct {
	QueryType CountryQueryType
	Query     string
}

func (cache *dbStatementCache) Prepare(db *sql.DB) error {
	stmt, err := db.Prepare(insertNewRecordSQL)
	if err != nil {
		return err
	}
	cache.InsertNewRecord = stmt
	stmt, err = db.Prepare(`
		SELECT
			Cases.Cases, Cases.Deaths, Cases.Cumulative
		FROM Cases
		INNER JOIN Countries
			ON Countries.Id = Cases.CountryId
		WHERE Countries.Code = ? AND Cases.Date = ?`)
	if err != nil {
		return err
	}
	cache.SelectRecord = stmt
	return nil
}

func (cache *dbStatementCache) Close() error {
	if err := cache.InsertNewRecord.Close(); err != nil {
		return err
	}
	if err := cache.SelectRecord.Close(); err != nil {
		return err
	}
	return nil
}

// NewDatabase initializes provided DB pointer and constructs Database
func NewDatabase(db *sql.DB) (*Database, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(`CREATE TABLE IF NOT EXISTS Countries (
		Id INTEGER PRIMARY KEY AUTOINCREMENT,
		Code CHAR(3),
		GeoId CHAR(2),
		Name VARCHAR(255) COLLATE NOCASE,
		Population INTEGER,
		Continent VARCHAR(32)
	)`)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	_, err = tx.Exec(`CREATE TABLE IF NOT EXISTS Cases (
		Date INTEGER,
		CountryId INTEGER,
		Cases INTEGER,
		Deaths INTEGER,
		Cumulative REAL,
		PRIMARY KEY (Date, CountryId),
		FOREIGN KEY (CountryId) REFERENCES Countries(Id)
	)`)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	cdb := &Database{
		database:     db,
		countryCache: make(map[int64]*Country),
	}
	if err = cdb.stmtCache.Prepare(db); err != nil {
		return nil, err
	}
	return cdb, nil
}

// Close closes COVID database
func (db *Database) Close() error {
	if err := db.stmtCache.Close(); err != nil {
		return err
	}
	return db.database.Close()
}

// Country looks for the most recent country information
func (db *Database) Country(code string) (*Country, error) {
	res, err := db.database.Query(`
		SELECT Id, Code, GeoId, Name, Population, Continent
			FROM Countries WHERE Code = ?
			ORDER BY Id DESC LIMIT 1
	`, code)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if res.Next() {
		var country Country
		res.Scan(&country.ID, &country.Code, &country.GeoID, &country.Name,
			&country.Population, &country.Continent)
		return &country, nil
	}
	return nil, nil
}

// CountryByID looks for  country information for specified ID
func (db *Database) CountryByID(id int64) (*Country, error) {
	if db.countryCache[id] != nil {
		return db.countryCache[id], nil
	}
	res, err := db.database.Query(`
		SELECT Id, Code, GeoId, Name, Population, Continent
			FROM Countries WHERE Id = ?
	`, id)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if res.Next() {
		var country Country
		err = res.Scan(&country.ID, &country.Code, &country.GeoID, &country.Name,
			&country.Population, &country.Continent)
		if err != nil {
			return nil, err
		}
		db.countryCache[id] = &country
		return &country, nil
	}
	return nil, nil
}

func (db *Database) newCountryRecord(tx *sql.Tx, country *Country) error {
	stmt, err := tx.Prepare(`
		INSERT
			INTO Countries (Code, GeoID, Name, Population, Continent)
			VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	res, err := stmt.Exec(country.Code, country.GeoID, country.Name, country.Population, country.Continent)
	if err != nil {
		return err
	}
	country.ID, err = res.LastInsertId()
	if err != nil {
		return err
	}
	db.countryCache[country.ID] = country
	return nil
}

// PutCountry creates new country in database, or create a new revision of existing one
func (db *Database) PutCountry(code string, geoID string, name string, population int64, continent string) (*Country, error) {
	country := &Country{
		Code:       code,
		GeoID:      geoID,
		Name:       name,
		Population: population,
		Continent:  continent,
	}
	tx, err := db.database.Begin()
	if err != nil {
		return nil, err
	}
	res, err := tx.Query(`
		SELECT Id, Code, GeoId, Name, Population, Continent
			FROM Countries WHERE Code = ?
			ORDER BY Id DESC
			LIMIT 1
	`, code)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if hasCurrentRevision := res.Next(); hasCurrentRevision {
		currentRevision := Country{}
		err = res.Scan(&currentRevision.ID, &currentRevision.Code, &currentRevision.GeoID, &currentRevision.Name,
			&currentRevision.Population, &currentRevision.Continent)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		if currentRevision.GeoID != geoID || currentRevision.Name != name ||
			currentRevision.Population != population || currentRevision.Continent != continent {
			if err = db.newCountryRecord(tx, country); err != nil {
				tx.Rollback()
				return nil, err
			}
			if err = tx.Commit(); err != nil {
				return nil, err
			}
			return country, nil
		}
		if err = tx.Rollback(); err != nil {
			return nil, err
		}
		return &currentRevision, nil
	}
	if err = db.newCountryRecord(tx, country); err != nil {
		tx.Rollback()
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return country, nil
}

func normalizeDate(timestamp time.Time) time.Time {
	year, month, day := timestamp.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.Local)
}

// NewRecord registers new date for country COVID stats
func (db *Database) NewRecord(record *CovidStatisticsRecord) error {
	date := normalizeDate(record.Date)
	_, err := db.stmtCache.InsertNewRecord.Exec(record.Country.ID, date.Unix(), record.Cases, record.Deaths, record.Cumulative)
	if err != nil {
		res, qerr := db.stmtCache.SelectRecord.Query(record.Country.Code, date.Unix())
		if qerr != nil {
			return err
		}
		defer res.Close()
		if hasRecord := res.Next(); hasRecord {
			return fmt.Errorf("Record for %s on %s is already registered", record.Country.Code, date.Format("2006-01-02"))
		}
		return err
	}
	return nil
}

// ImportRecords inserts an array of records as a signle transaction
func (db *Database) ImportRecords(records []CovidStatisticsRecord) error {
	tx, err := db.database.Begin()
	if err != nil {
		return err
	}
	insertStmt, err := tx.Prepare(insertNewRecordSQL)
	if err != nil {
		return err
	}
	defer insertStmt.Close()

	for _, record := range records {
		_, err = insertStmt.Exec(record.Country.ID, normalizeDate(record.Date).Unix(), record.Cases, record.Deaths, record.Cumulative)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

// LastRecordDate returns date of last records for country
func (db *Database) LastRecordDate(countryCode string) (time.Time, error) {
	res, err := db.database.Query(`
		SELECT Cases.Date
			FROM Cases
			INNER JOIN Countries
				ON Cases.CountryId = Countries.Id
			WHERE Countries.Code = ?
			ORDER BY Date DESC
			LIMIT 1`, countryCode)
	epochStart := time.Unix(0, 0)
	if err != nil {
		return epochStart, err
	}
	defer res.Close()
	if !res.Next() {
		return epochStart, nil
	}
	var timestamp int64
	if err = res.Scan(&timestamp); err != nil {
		return epochStart, err
	}
	return time.Unix(timestamp, 0), nil
}

// RetrieveRecordsSince collects country statistics from the database
func (db *Database) RetrieveRecordsSince(query CountryQuery, since time.Time) (chan CovidStatisticsRecordResult, error) {
	sinceDate := normalizeDate(since)
	var countryQuery string
	switch query.QueryType {
	default:
		countryQuery = "Countries.Code"
	case CountryQueryByGeoID:
		countryQuery = "Countries.GeoID"
	case CountryQueryByName:
		countryQuery = "Countries.Name"
	}
	res, err := db.database.Query(fmt.Sprintf(`
		SELECT Cases.CountryId, Cases.Date, Cases.Cases, Cases.Deaths, Cases.Cumulative
			FROM Cases
			INNER JOIN Countries
				ON Cases.CountryId = Countries.Id
			WHERE %s = ? AND Cases.Date >= ?
			ORDER BY Date ASC`, countryQuery), query.Query, sinceDate.Unix())
	if err != nil {
		return nil, err
	}
	chnl := make(chan CovidStatisticsRecordResult)
	go func() {
		for res.Next() {
			var countryID int64
			var cases, deaths int
			var timestamp int64
			var cumulative float64
			err := res.Scan(&countryID, &timestamp, &cases, &deaths, &cumulative)
			if err != nil {
				chnl <- CovidStatisticsRecordResult{
					Error: err,
				}
				break
			}
			country, err := db.CountryByID(countryID)
			if err != nil {
				chnl <- CovidStatisticsRecordResult{
					Error: err,
				}
				break
			}
			chnl <- CovidStatisticsRecordResult{
				Result: CovidStatisticsRecord{
					Country:    country,
					Date:       time.Unix(timestamp, 0),
					Cases:      cases,
					Deaths:     deaths,
					Cumulative: cumulative,
				},
			}
		}
		close(chnl)
	}()
	return chnl, nil
}

// RetrieveRecords collects country statistics from the database
func (db *Database) RetrieveRecords(query CountryQuery) (chan CovidStatisticsRecordResult, error) {
	return db.RetrieveRecordsSince(query, time.Unix(0, 0))
}

// NewQuery constructs new country query based on query string
func NewQuery(query string) CountryQuery {
	if len(query) == 2 && strings.ToUpper(query) == query {
		return CountryQuery{
			Query:     query,
			QueryType: CountryQueryByGeoID,
		}
	} else if len(query) == 3 && strings.ToUpper(query) == query {
		return CountryQuery{
			Query:     query,
			QueryType: CountryQueryByID,
		}
	} else {
		return CountryQuery{
			Query:     query,
			QueryType: CountryQueryByName,
		}
	}
}
