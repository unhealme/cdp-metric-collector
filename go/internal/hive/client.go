package hive

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	_ "github.com/beltran/gohive/v2"
	"github.com/unhealme/cdp-metric-collector/go/internal"
)

var logger = internal.DefaultLogger()

type HiveClient struct {
	connection *sql.DB
}

func (c *HiveClient) Close() error {
	return c.connection.Close()
}

func (c *HiveClient) Databases() ([]string, error) {
	logger.Debug("getting all databases")
	r, err := c.connection.Query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var db []string
	for r.Next() {
		var name string
		if err := r.Scan(&name); err != nil {
			return nil, err
		}
		db = append(db, name)
	}
	return db, nil
}

func (c *HiveClient) DDL(table string) (*string, error) {
	logger.Debug("getting ddl table", logger.Args("name", table))
	r, err := c.connection.Query(fmt.Sprintf("SHOW CREATE TABLE %s", table))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var b strings.Builder
	for r.Next() {
		var line string
		if err := r.Scan(&line); err != nil {
			return nil, err
		}
		b.WriteString(line + "\n")
	}
	ddl := b.String()
	return &ddl, nil
}

func (c *HiveClient) Describe(table string) (*TableDesc, error) {
	logger.Debug("describe table", logger.Args("name", table))
	r, err := c.connection.Query(fmt.Sprintf("DESCRIBE FORMATTED %s", table))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var desc TableDesc
	pcolSection := false
	for r.Next() {
		var colName string
		var dataType *string
		var comment *string
		if err := r.Scan(&colName, &dataType, &comment); err != nil {
			return nil, err
		}
		dataTypeTrimmed := strings.TrimSpace(*dataType)
		if !pcolSection {
			switch strings.TrimSpace(colName) {
			case "Location:":
				if locParsed, err := url.Parse(*dataType); err == nil {
					desc.Location = &locParsed.Path
				}
			case "Table Type:":
				desc.Type = &dataTypeTrimmed
			case "LastAccessTime:":
				desc.LastAccess = &dataTypeTrimmed
			case "CreateTime:":
				desc.CreateTime = &dataTypeTrimmed
			case "# Partition Information":
				pcolSection = true
			}
		} else {
			if pcol := strings.TrimSpace(colName); len(pcol) > 0 && !strings.HasPrefix(pcol, "#") {
				desc.PartitionColumns = append(desc.PartitionColumns, pcol)
			} else {
				pcolSection = false
			}
		}
	}
	return &desc, nil
}

func (c *HiveClient) Partitions(table string) ([]string, error) {
	logger.Debug("getting table partitions", logger.Args("name", table))
	r, err := c.connection.Query(fmt.Sprintf("SHOW PARTITIONS %s", table))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var partitions []string
	for r.Next() {
		var p string
		if err := r.Scan(&p); err != nil {
			return nil, err
		}
		partitions = append(partitions, p)
	}
	return partitions, nil
}

func (c *HiveClient) Tables(db, pattern string) ([]string, error) {
	logger.Debug("getting tables", logger.Args("database", db, "pattern", pattern))
	r, err := c.connection.Query(fmt.Sprintf("SHOW TABLES IN %s LIKE ?", db), pattern)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var tbl []string
	for r.Next() {
		var name string
		if err := r.Scan(&name); err != nil {
			return nil, err
		}
		tbl = append(tbl, name)
	}
	return tbl, nil
}

func GetClient(hostname string, port int, database string) (*HiveClient, error) {
	db, err := sql.Open("hive", fmt.Sprintf("hive://%s:%d/%s?auth=KERBEROS&service=hive", hostname, port, database))
	if err != nil {
		return nil, err
	}
	return &HiveClient{connection: db}, nil
}
