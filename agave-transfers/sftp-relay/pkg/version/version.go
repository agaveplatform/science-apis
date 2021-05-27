package version

import (
	"os"
	"strings"
)

const version = "2.x.x"

var (
	commit = os.Getenv("AGAVE_GIT_COMMIT_HASH")
	date   = os.Getenv("AGAVE_BUILD_TIME")
	info   Info
)

// Info defines version details
type Info struct {
	Version    string   `json:"version"`
	BuildDate  string   `json:"build_date"`
	CommitHash string   `json:"commit_hash"`
	Features   []string `json:"features"`
}

// GetAsString returns the string representation of the version
func GetAsString() string {
	var sb strings.Builder
	sb.WriteString(info.Version)
	if len(info.CommitHash) > 0 {
		sb.WriteString("-")
		sb.WriteString(info.CommitHash)
	}
	if len(info.BuildDate) > 0 {
		sb.WriteString("-")
		sb.WriteString(info.BuildDate)
	}
	if len(info.Features) > 0 {
		sb.WriteString(" ")
		sb.WriteString(strings.Join(info.Features, " "))
	}
	return sb.String()
}

func init() {
	ver := os.Getenv("AGAVE_VERSION")
	if ver == "" {
		ver = version
	}
	info = Info{
		Version:    ver,
		CommitHash: commit,
		BuildDate:  date,
	}
}

// AddFeature adds a feature description
func AddFeature(feature string) {
	info.Features = append(info.Features, feature)
}

// Get returns the Info struct
func Get() Info {
	return info
}