package hdfs

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	hdfs "github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/credentials"
	"github.com/unhealme/cdp-metric-collector/go/internal"
)

var logger = internal.DefaultLogger()

type HDFSClient struct{ *hdfs.Client }

func (c *HDFSClient) Walk2(basePath string, dateMin, dateMax time.Time, maxDepth int, dirOnly bool) (<-chan HDFSPath, error) {
	var (
		fp chan HDFSPath
		qp []string

		fetchPaths = func(p string, d int) {
			paths, err := c.ReadDir(p)
			if err != nil {
				logger.Warn("unable to read path", logger.Args("name", p, "error", err))
			}
			for _, path := range paths {
				fname := filepath.Join(p, path.Name())
				if path.IsDir() {
					qp = append(qp, fname)
				}
				mt := path.ModTime()
				if mt.Before(dateMax) && mt.After(dateMin) && !(dirOnly && !path.IsDir()) {
					content, err := c.GetContentSummary(fname)
					if err != nil {
						continue
					}
					fp <- HDFSPath{path.(*hdfs.FileInfo), fname, content, d}
				}
			}
		}
	)
	if !filepath.IsAbs(basePath) {
		return nil, fmt.Errorf("%q is not an absolute path", basePath)
	}
	first_info, err := c.Stat(basePath)
	if err != nil {
		return nil, err
	}
	first_content, err := c.GetContentSummary(basePath)
	if err != nil {
		return nil, err
	}
	fp = make(chan HDFSPath)
	go func() {
		fp <- HDFSPath{first_info.(*hdfs.FileInfo), basePath, first_content, 0}
		qp = append(qp, basePath)
		var i []string
		for d := 1; internal.LimitReached(d, maxDepth); d++ {
			if len(qp) < 1 {
				break
			}
			i = qp[:]
			qp = nil
			for _, p := range i {
				logger.Info("walking paths", logger.Args("path", p, "depth", d, "max", maxDepth))
				fetchPaths(p, d)
			}
		}
		close(fp)
	}()
	return fp, nil
}

func GetClient() (*HDFSClient, error) {
	conf, err := hadoopconf.Load("/etc/hadoop/conf")
	if err != nil {
		return nil, err
	}
	co := hdfs.ClientOptionsFromConf(conf)
	if co.KerberosClient != nil {
		kcf, err := config.Load(internal.GetEnv("KRB5_CONFIG", "/etc/krb5.conf"))
		if err != nil {
			return nil, err
		}
		cc, err := credentials.LoadCCache(internal.GetEnv("KRB5CCNAME", fmt.Sprintf("/tmp/krb5cc_%d", os.Getuid())))
		if err != nil {
			return nil, err
		}
		kc, err := client.NewFromCCache(cc, kcf)
		if err != nil {
			return nil, err
		}
		co.KerberosClient = kc
	}
	base, err := hdfs.NewClient(co)
	if err != nil {
		return nil, err
	}
	return &HDFSClient{base}, nil
}
