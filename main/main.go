package main

import (
	"database/sql"
	"fmt"
	"os"

	covid "github.com/protopopov1122/covidservice/lib"

	_ "github.com/mattn/go-sqlite3"
)

func makeDatabase(env *covid.Env) (*covid.Database, error) {
	database, err := sql.Open("sqlite3", env.DatabasePath)
	if err != nil {
		return nil, err
	}
	cdb, err := covid.NewDatabase(database)
	if err != nil {
		database.Close()
		return nil, err
	}
	return cdb, nil
}

func startApp(entry covid.EntryFn) error {
	env, err := covid.NewDefaultEnv("https://opendata.ecdc.europa.eu/covid19/casedistribution/json/")
	if err != nil {
		return err
	}
	env.Load()

	cdb, err := makeDatabase(env)
	if err != nil {
		return err
	}
	defer cdb.Close()

	return entry(cdb, env, os.Stdout)
}

func main() {
	cli := covid.NewCli()
	cli.Bind(covid.NewCommand("import", "Import the most recent data", importFn))
	cli.Bind(covid.NewCommand("query", "Query country data", queryFn))
	cli.Bind(covid.NewCommand("help", "Print help", helpFn))
	if err := startApp(cli.NewEntry(os.Args[1:])); err != nil {
		fmt.Println(err)
	}
}
