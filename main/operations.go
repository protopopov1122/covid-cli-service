package main

import (
	"errors"
	"fmt"
	"io"

	covid "github.com/protopopov1122/covidservice/lib"
	"github.com/protopopov1122/covidservice/lib/ecdc"
)

func importFn(cdb *covid.Database, env *covid.Env, _ *covid.Cli, argv []string, out io.Writer) error {
	fmt.Fprintf(out, "Importing from %s into %s", env.EcdcDataSourceURL, env.DatabasePath)
	dataSource, err := ecdc.NewDataSource(env.EcdcDataSourceURL)
	if err != nil {
		return err
	}

	return dataSource.Import(cdb)
}

func queryFn(cdb *covid.Database, env *covid.Env, _ *covid.Cli, argv []string, out io.Writer) error {
	if len(argv) == 0 {
		return errors.New("Provide country code")
	}
	records, err := cdb.RetrieveRecords(covid.NewQuery(argv[0]))
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "%-15s %-15s %-15s %-10s %s\n", "Country", "Date", "Cumulative", "New cases", "New deaths")
	for record := range records {
		if record.Error != nil {
			return record.Error
		}
		fmt.Fprintf(out, "%-15s %-15s %-15f %-10d %d\n", record.Result.Country.Name, record.Result.Date.Format("2006-01-02"), record.Result.Cumulative, record.Result.Cases, record.Result.Deaths)
	}
	fmt.Fprintf(out, "Data source:\t%s\n", env.EcdcDataSourceURL)
	return nil
}

func helpFn(cdb *covid.Database, env *covid.Env, cli *covid.Cli, argv []string, out io.Writer) error {
	fmt.Fprintf(out, "Database:\t%s\b", env.DatabasePath)
	fmt.Fprintf(out, "Data source:\t%s\n", env.EcdcDataSourceURL)
	fmt.Fprintf(out, "Command list:\n")
	cli.PrintCommands(out)
	return nil
}
