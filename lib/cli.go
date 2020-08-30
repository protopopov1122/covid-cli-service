package lib

import (
	"errors"
	"fmt"
	"io"
)

// EntryFn defines application entry point
type EntryFn func(cdb *Database, env *Env, out io.Writer) error

// CliCommand defines a signle command line command
type CliCommand interface {
	Mnemonic() string
	Description() string
	Execute(cdb *Database, env *Env, cli *Cli, argv []string, out io.Writer) error
}

// Cli defines command line interface
type Cli struct {
	commands map[string]CliCommand
}

type commandImpl struct {
	mnemonic    string
	description string
	callback    func(cdb *Database, env *Env, cli *Cli, argv []string, out io.Writer) error
}

// NewCli constructs command line interface instance
func NewCli() *Cli {
	return &Cli{
		commands: make(map[string]CliCommand),
	}
}

// Bind attaches command to command line interface
func (cli *Cli) Bind(cmd CliCommand) error {
	if _, exists := cli.commands[cmd.Mnemonic()]; exists {
		return fmt.Errorf("Command with mnemonic '%s' is already bound", cmd.Mnemonic())
	}
	cli.commands[cmd.Mnemonic()] = cmd
	return nil
}

// Execute invokes command line according to provided arguments
func (cli *Cli) Execute(cdb *Database, env *Env, argv []string, out io.Writer) error {
	if len(argv) == 0 {
		return errors.New("No command supplied")
	}
	mnemonic := argv[0]
	cmd, exists := cli.commands[mnemonic]
	if !exists {
		return fmt.Errorf("Command with mnemonic '%s' does not exist", mnemonic)
	}
	return cmd.Execute(cdb, env, cli, argv[1:], out)
}

// NewEntry constructs entry point for provided command line arguments
func (cli *Cli) NewEntry(argv []string) EntryFn {
	return func(cdb *Database, env *Env, out io.Writer) error {
		return cli.Execute(cdb, env, argv, out)
	}
}

// PrintCommands outputs CLI commands along with descriptions
func (cli *Cli) PrintCommands(out io.Writer) {
	for _, cmd := range cli.commands {
		fmt.Fprintf(out, "%s\t%s\n", cmd.Mnemonic(), cmd.Description())
	}
}

// NewCommand constructs standard command
func NewCommand(mnemonic string, description string, callback func(cdb *Database, env *Env, cli *Cli, argv []string, out io.Writer) error) CliCommand {
	return &commandImpl{
		mnemonic:    mnemonic,
		description: description,
		callback:    callback,
	}
}

// Mnemonic returns command mnemonic string
func (cmd *commandImpl) Mnemonic() string {
	return cmd.mnemonic
}

// Description returns command description string
func (cmd *commandImpl) Description() string {
	return cmd.description
}

// Execute runs command callback
func (cmd *commandImpl) Execute(cdb *Database, env *Env, cli *Cli, argv []string, out io.Writer) error {
	return cmd.callback(cdb, env, cli, argv, out)
}
