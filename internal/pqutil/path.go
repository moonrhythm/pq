package pqutil

import (
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"syscall"
)

// Home gets the PostgreSQL configuration dir in the user's home directory:
// %APPDATA%/postgresql on Windows, and $HOME/.postgresql/postgresql.crt
// everywhere else.
//
// Returns an empy string if no home directory was found.
//
// Matches pqGetHomeDirectory() from PostgreSQL.
// https://github.com/postgres/postgres/blob/2b117bb/src/interfaces/libpq/fe-connect.c#L8214
func Home(subdir bool) string {
	if runtime.GOOS == "windows" {
		// pq uses SHGetFolderPath(), which is deprecated but x/sys/windows has
		// KnownFolderPath(). We don't really want to pull that in though, so
		// use APPDATA env. This is also what PostgreSQL uses in some other
		// codepaths (get_home_path() for example).
		ad := os.Getenv("APPDATA")
		if ad == "" {
			return ""
		}
		return filepath.Join(ad, "postgresql")
	}

	home, _ := os.UserHomeDir()
	if home == "" {
		u, err := user.Current()
		if err != nil {
			return ""
		}
		home = u.HomeDir
	}
	// libpq reads some files from ~/ and some from ~/.postgresql – on Windows
	// it always uses %APPDATA%/postgresql.
	if subdir {
		home = filepath.Join(home, ".postgresql")
	}
	return home
}

// ErrNotExists reports if err is a "path doesn't exist" type error.
//
// fs.ErrNotExist is not enough, as "/dev/null/somefile" will return ENOTDIR
// instead of ENOENT.
func ErrNotExists(err error) bool {
	perr := new(os.PathError)
	if errors.As(err, &perr) && (perr.Err == syscall.ENOENT || perr.Err == syscall.ENOTDIR) {
		return true
	}
	return false
}

