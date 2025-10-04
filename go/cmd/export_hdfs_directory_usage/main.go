package main

import (
	"encoding/csv"

	"github.com/unhealme/cdp-metric-collector/go/internal"
	"github.com/unhealme/cdp-metric-collector/go/internal/hdfs"
)

const Version = "r2025.09.27-0"

var (
	logger = internal.DefaultLogger()

	outputField = []string{
		"Depth",
		"Mode",
		"Path",
		"Owner",
		"Group",
		"Last Access",
		"Last Modified",
		"Size",
		"Rounded Size",
		"Usage",
		"Rounded Usage",
		"File and Directory Count",
	}
)

func main() {
	args, err := ParseArgs()
	if err != nil {
		logger.Fatal("unable to parse arguments", logger.Args("error", err))
	}
	if args.Verbose {
		logger.Level = internal.LogLevelDebug
	}
	logger.Debug("successfully parsed arguments", logger.Args(args.ToArgs()...))
	c, err := hdfs.GetClient()
	if err != nil {
		logger.Fatal("unable to create HDFS client", logger.Args("error", err))
	}
	fw := csv.NewWriter(args._Outf)
	defer args._Outf.Close()
	defer fw.Flush()
	if !args.Append {
		fw.Write(outputField)
	}
	for _, p := range args.Paths {
		walker, err := c.Walk2(p, args.DateMin.Time, args.DateMax.Time, args.MaxDepth, args.DirOnly)
		if err != nil {
			logger.Warn("unable to walk path", logger.Args("path", p, "error", err))
			continue
		}
		for p := range walker {
			fw.Write(p.ToRow())
		}
	}
}
