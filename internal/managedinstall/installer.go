package managedinstall

import (
	"flag"
	"fmt"
	"io"
	"runtime"
)

// HandleInstallerCommand runs before runtime dispatch. Installers use the
// verified executable's native writer without a sqlite CLI dependency, shell
// escaping of SQL, or a JSON handoff file. This is not a top-level profile name.
func HandleInstallerCommand(args []string, stdout, stderr io.Writer) (int, bool) {
	if len(args) < 2 || args[1] != "--cxp-record-install" {
		return 0, false
	}
	fs := flag.NewFlagSet("--cxp-record-install", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var path, shim string
	r := Record{SchemaVersion: RecordSchemaVersion, TargetSource: "installer", TargetState: string(StateManaged)}
	fs.StringVar(&path, "record-path", "", "Install record database")
	fs.StringVar(&r.TargetPath, "target", "", "Installed executable")
	fs.StringVar(&shim, "shim", "", "Installed shim")
	fs.StringVar(&r.Repo, "repo", "", "Release repository")
	fs.StringVar(&r.Version, "version", "", "Installed version")
	fs.StringVar(&r.GOOS, "goos", runtime.GOOS, "Installed operating system")
	fs.StringVar(&r.GOARCH, "goarch", runtime.GOARCH, "Installed architecture")
	if err := fs.Parse(args[2:]); err != nil {
		return 1, true
	}
	if fs.NArg() != 0 || r.TargetPath == "" {
		fmt.Fprintln(stderr, "install record requires --target and no positional arguments")
		return 1, true
	}
	if shim != "" {
		r.Shims = []string{shim}
	}
	if err := SaveRecord(path, r); err != nil {
		fmt.Fprintf(stderr, "write install record: %v\n", err)
		return 1, true
	}
	native, err := recordPath(path)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1, true
	}
	fmt.Fprintf(stdout, "Install record: %s\n", native)
	return 0, true
}
