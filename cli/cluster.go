package cli

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/metadata"
)

var clusterCtx struct {
	dataDir string
}

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Operate on cluster metadata persisted on node.",
	Long: `
Operate on cluster metadata persisted on node.
`,
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Usage()
		os.Exit(1)
	},
}

var clusterBackupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Take backup of cluster metadata file.",
	Long: `
Take backup of cluster metadata file. This command fails if kronos server is 
running. This should be done before re_ip to recover if there were any failures.
`,
	Run: func(cmd *cobra.Command, args []string) {
		runClusterBackup()
	},
}

var clusterRestoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore cluster metadata file.",
	Long: `
Restore cluster metadata file backed up by previous backup. This command fails
if kronos server is running. Ensure that backup was done at the time you want to
restore metadata to, otherwise this can corrupt node metadata by restoring a
stale copy of the metadata.
`,
	Run: func(cmd *cobra.Command, args []string) {
		runClusterRestore()
	},
}

var clusterLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "Show persisted metadata of nodes in the cluster",
	Long: `
Show persisted metadata of nodes in the cluster. This is the metadata persisted 
on this node and may not be the true state at the current time.
`,
	Run: func(cmd *cobra.Command, args []string) {
		runClusterLs()
	},
}

func init() {
	dataDirCmds := []*cobra.Command{
		clusterLsCmd,
		clusterBackupCmd,
		clusterRestoreCmd,
	}
	for _, cmd := range dataDirCmds {
		cmd.Flags().StringVar(
			&clusterCtx.dataDir,
			dataDirFlag,
			"",
			"Data directory of kronos server",
		)
		_ = cmd.MarkFlagRequired(dataDirFlag)
	}

	clusterCmd.AddCommand(
		clusterLsCmd,
		clusterRemoveCmd,
		clusterBackupCmd,
		clusterRestoreCmd,
	)
}

func runCmdWithFatal(ctx context.Context, cmd *exec.Cmd) {
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			log.Error(ctx, stderr.String())
		}
		log.Fatal(ctx, err)
	}
}

const backupFileSuffix = ".bak"

func runClusterBackup() {
	ctx := context.Background()
	// acquire lock to ensure that kronos is not running
	cluster, err := metadata.LoadCluster(clusterCtx.dataDir, false /*readOnly*/)
	if err != nil {
		log.Fatal(ctx, err)
	}
	defer kronosutil.CloseWithErrorLog(ctx, cluster)
	clusterInfoFile := metadata.ClusterInfoFilename(clusterCtx.dataDir)
	backupFile := clusterInfoFile + backupFileSuffix
	copyArgs := []string{"-f", clusterInfoFile, backupFile}
	runCmdWithFatal(ctx, exec.Command("cp", copyArgs...))
}

func runClusterRestore() {
	ctx := context.Background()
	// acquire lock to ensure that kronos is not running
	cluster, err := metadata.LoadCluster(clusterCtx.dataDir, false /*readOnly*/)
	if err != nil {
		log.Fatal(ctx, err)
	}
	defer kronosutil.CloseWithErrorLog(ctx, cluster)
	clusterInfoFile := metadata.ClusterInfoFilename(clusterCtx.dataDir)
	backupFile := clusterInfoFile + backupFileSuffix
	copyArgs := []string{"-f", backupFile, clusterInfoFile}
	runCmdWithFatal(ctx, exec.Command("cp", copyArgs...))
}

// validateMapping validates mapping of old to new addresses for re_ip. It
// validates that no address is empty and ports are either present in all
// addresses or absent from all addresses.
// It returns true if ports are present in all addresses and false in all other
// cases. error is non-nil if some validation fails.
func validateMapping(oldToNewAddr map[string]string) (bool, error) {
	var withPorts bool
	for old := range oldToNewAddr {
		if _, _, err := net.SplitHostPort(old); err == nil {
			withPorts = true
		}
		break
	}
	for o, n := range oldToNewAddr {
		if len(o) == 0 || len(n) == 0 {
			return false, errors.New("some key or value in mapping file is empty")
		}
		if withPorts {
			for _, v := range []string{o, n} {
				if _, _, err := net.SplitHostPort(v); err != nil {
					return false, errors.Errorf(
						"%s doesn't contain port but some other address in mapping file does, error: %v",
						v,
						err,
					)
				}
			}
		} else {
			for _, v := range []string{o, n} {
				if _, _, err := net.SplitHostPort(v); err == nil {
					return false, errors.Errorf(
						"%s contains port also but some other address in mapping file doesn't, error: %v",
						v,
						err,
					)
				}
			}
		}
	}
	return withPorts, nil
}

func runClusterLs() {
	ctx := context.Background()
	clstr, err := metadata.LoadCluster(clusterCtx.dataDir, true /*readOnly*/)
	if err != nil {
		log.Fatal(ctx, err)
	}
	s, err := clstr.PrettyPrint()
	if err != nil {
		log.Fatal(ctx, err)
	}
	fmt.Print(s)
}
