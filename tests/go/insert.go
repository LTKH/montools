package main

import (
	"fmt"
	"math/rand"
	"context"
	"time"
    client "github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
    conn, err := client.Open(&client.Options{
        Addr: []string{"127.0.0.1:9000"},
        Auth: client.Auth{
			Database: "default",
			Username: "default",
			Password: "password",
		},
        Protocol:  client.HTTP,
    })

	if err != nil {
		return
	}

	for {
		rand.Seed(time.Now().UnixNano())
		conn.Exec(context.Background(), fmt.Sprintf("insert into metrics.test001 values (toDateTime(now()), %d, 'test_string_1');", rand.Intn(100)))
		conn.Exec(context.Background(), fmt.Sprintf("insert into metrics.test001 values (toDateTime(now()), %d, 'test_string_2');", rand.Intn(30)))
        time.Sleep(15 * time.Second)
	}
    
}