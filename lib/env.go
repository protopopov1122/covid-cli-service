package lib

import (
	"errors"
	"fmt"
	"os"
)

// Env contains basic application data passed in environment variables
type Env struct {
	DatabasePath      string
	EcdcDataSourceURL string
}

// NewDefaultEnv initialized Env with default values
func NewDefaultEnv(ecdcURL string) (*Env, error) {
	env := &Env{
		EcdcDataSourceURL: ecdcURL,
	}
	if xdgDataHome := os.Getenv("XDG_DATA_HOME"); len(xdgDataHome) > 0 {
		env.DatabasePath = fmt.Sprintf("%s/covid.db", xdgDataHome)
	} else if home := os.Getenv("HOME"); len(home) > 0 {
		env.DatabasePath = fmt.Sprintf("%s/.local/share/covid.db", home)
	} else {
		return nil, errors.New("Unable to detect user home directory: empty $HOME")
	}
	return env, nil
}

// Load overwrites default values based on environment variables
func (env *Env) Load() {
	if dbPath := os.Getenv("COVID_DB_PATH"); len(dbPath) > 0 {
		env.DatabasePath = dbPath
	}
	if ecdcURL := os.Getenv("COVID_ECDC_URL"); len(ecdcURL) > 0 {
		env.EcdcDataSourceURL = ecdcURL
	}
}
