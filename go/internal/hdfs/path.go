package hdfs

import (
	"strconv"

	hdfs "github.com/colinmarc/hdfs/v2"
	"github.com/unhealme/cdp-metric-collector/go/internal"
)

const ISOTimeFormat = "2006-01-02 15:04:05.999999"

type HDFSPath struct {
	*hdfs.FileInfo
	fpath   string
	content *hdfs.ContentSummary
	depth   int
}

func (p *HDFSPath) ToRow() []string {
	s := p.content.Size()
	u := p.content.SizeAfterReplication()
	return []string{
		strconv.Itoa(p.depth),
		p.Mode().String(),
		p.fpath,
		p.Owner(),
		p.OwnerGroup(),
		p.AccessTime().Format(ISOTimeFormat),
		p.ModTime().Format(ISOTimeFormat),
		strconv.FormatInt(s, 10),
		internal.FormatSize(float64(s)),
		strconv.FormatInt(u, 10),
		internal.FormatSize(float64(u)),
		strconv.Itoa(p.content.DirectoryCount() + p.content.FileCount()),
	}
}
