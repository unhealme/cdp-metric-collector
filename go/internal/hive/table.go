package hive

import (
	"strings"
	"sync"

	"github.com/unhealme/cdp-metric-collector/go/internal/hdfs"
)

type TableDesc struct {
	CreateTime       *string
	LastAccess       *string
	Location         *string
	PartitionColumns []string
	Type             *string
}

type TableNames struct {
	Db, Table string
}

type TableParts struct {
	FirstPartition *string
	LastPartition  *string
	PartitionCount *int
	Partitions     []string
	PartitionsL1   []string
}

type TableStats struct {
	FileCount *int
	Size      *int64
}

type Table struct {
	DDL *string

	TableDesc
	TableNames
	TableParts
	TableStats
}

func (c *HiveClient) Table(hdfs *hdfs.HDFSClient, name string, fields []string) (*Table, error) {
	logger.Info("getting table", logger.Args("name", name, "fields", fields))
	var (
		tbl Table

		names = sync.OnceValue(func() *TableNames {
			db, tbl, _ := strings.Cut(name, ".")
			return &TableNames{db, tbl}
		})
		desc = sync.OnceValues(func() (*TableDesc, error) {
			return c.Describe(name)
		})
		parts = sync.OnceValues(func() (*TableParts, error) {
			part, err := c.Partitions(name)
			if err != nil {
				return nil, err
			}
			var tp TableParts
			c := len(part)
			if c > 0 {
				tp.FirstPartition = &part[0]
				tp.LastPartition = &part[c-1]
				tp.PartitionCount = &c
			}
			tp.Partitions = part
			for _, p := range part {
				tp.PartitionsL1 = append(tp.PartitionsL1, strings.Split(p, "/")[0])
			}
			return &tp, nil
		})
		ddl = sync.OnceValues(func() (*string, error) {
			return c.DDL(name)
		})
		stats = sync.OnceValues(func() (*TableStats, error) {
			desc, err := desc()
			if err != nil {
				return nil, err
			}
			var st TableStats
			c, err := hdfs.GetContentSummary(*desc.Location)
			if err != nil {
				return nil, err
			}
			fc := c.DirectoryCount() + c.FileCount()
			fs := c.SizeAfterReplication()
			st.FileCount = &fc
			st.Size = &fs
			return &st, nil
		})
	)
	for _, f := range fields {
		switch f {
		case "db":
			tbl.Db = names().Db
		case "table":
			tbl.Table = names().Table
		case "loc":
			tdesc, err := desc()
			if err == nil {
				tbl.Location = tdesc.Location
			} else {
				logger.Warn("unable to describe", logger.Args("table", name))
			}
		}
	}
	return &tbl, nil
}
