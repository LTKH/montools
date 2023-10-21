package chouse

import (
    "fmt"
    "log"
    //"os"
    "time"
    "context"
    //"errors"
    //"regexp"
    //"strings"
    //"encoding/json"
    //"database/sql"
    "github.com/ClickHouse/clickhouse-go/v2"
    "github.com/ltkh/montools/internal/config"
)

type Client struct {
    client clickhouse.Conn
    config *config.Source
}

func NewClient(conf *config.Source) (*Client, error) {
    conn, err := clickhouse.Open(&clickhouse.Options{
        Addr: conf.Addr,
        Auth: clickhouse.Auth{
			Database: conf.Database,
			Username: conf.Username,
			Password: conf.Password,
		},
        Settings: clickhouse.Settings{
			"max_execution_time": conf.MaxExecutionTime,
		},
        DialTimeout: time.Second * conf.DialTimeout,
        Compression: &clickhouse.Compression{
            Method: clickhouse.CompressionLZ4,
        },
        Protocol:  clickhouse.HTTP,
    })
	if err != nil {
		return &Client{}, err
	}
    return &Client{ client: conn, config: conf }, nil
}

func (db *Client) Close() error {
    db.client.Close()

    return nil
}

func (db *Client) ShowTables() ([]string, error) {
    var tables []string

    ts, ok := db.config.Fields["timestamp"]

    rows, err := db.client.Query(context.Background(), "show tables")
    if err != nil {
        return tables, err
    }

    for rows.Next() {

        var name string
        if err := rows.Scan(&name); err != nil {
            log.Printf("[error] %v", err)
            continue
        }

        test, err := db.client.Query(context.Background(), fmt.Sprintf("select * from %s limit 1", name))
        if err != nil {
            return tables, err
        }
        fields := test.Columns()
        
        for _, field := range fields {
            if ok && ts == field {
                continue
            }
            tables = append(tables, fmt.Sprintf("%s:%s", name, field))
        }
    }

    return tables, nil
}