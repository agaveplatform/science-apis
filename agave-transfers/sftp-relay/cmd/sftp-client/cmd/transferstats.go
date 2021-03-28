package cmd

import "fmt"

type TransferStats struct {
	Files int32
	Dirs  int32
	Depth int
	Bytes int64
	Runtime int64
}

func (t *TransferStats) String() (string)  {
	return  fmt.Sprintf(" Files: %d \n Dirs: %d \n Depth: %d \n Bytes: %d \n Runtime: %d \n", t.Files, t.Dirs, t.Depth, t.Bytes, t.Runtime)
}

func (t *TransferStats) Add(transferStats TransferStats) {
	t.Files = t.Files + transferStats.Files
	t.Dirs = t.Dirs + transferStats.Dirs
	t.Bytes = t.Bytes + transferStats.Bytes
	t.Depth = t.Depth + transferStats.Depth
}