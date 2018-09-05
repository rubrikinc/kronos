package cli

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCtx struct {
	certsDir string
}

// RootCmd is the main command which contains all the kronos subcommands
var RootCmd = &cobra.Command{
	Use:   "kronos",
	Short: "kronos time service",
	Long: `
Kronos time service is used for getting synchronized time in a cluster of nodes
`,
}

// kronosCertsDir returns the certificates directory passed as a flag
// If an empty certificates directory is passed as a flag, it returns
// the value of the environment variable COCKROACH_CERTS_DIR
func kronosCertsDir() string {
	if len(rootCtx.certsDir) > 0 {
		return rootCtx.certsDir
	}

	return os.Getenv("KRONOS_CERTS_DIR")
}

func init() {
	RootCmd.PersistentFlags().StringVar(
		&rootCtx.certsDir,
		"certs-dir",
		"",
		"Certificates directory for GRPC Server / Client",
	)

	RootCmd.AddCommand(clusterCmd, startCmd, statusCmd, validateCmd, timeCmd)
}
