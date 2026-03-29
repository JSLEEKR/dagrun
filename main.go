// dagrun is a lightweight DAG workflow runner.
//
// Inspired by dagu (https://github.com/dagu-org/dagu).
// Reimplemented from scratch with zero external dependencies.
package main

import (
	"os"

	"github.com/JSLEEKR/dagrun/internal/cli"
)

func main() {
	app := cli.New(os.Stdout, os.Stderr)
	code := app.Run(os.Args[1:])
	os.Exit(code)
}
