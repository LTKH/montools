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
		conn.Exec(context.Background(), fmt.Sprintf("insert into metrics.test001 values ('[INFO] plugin/ready', 'info', toDateTime(now()), %d, 'test_string_1');", rand.Intn(100)))
		conn.Exec(context.Background(), fmt.Sprintf("insert into metrics.test001 values ('[INFO] plugin/ready', 'info', toDateTime(now()), %d, 'test_string_2');", rand.Intn(30)))
        time.Sleep(15 * time.Second)
	}
    
}

//SELECT sum(value) AS value, timestamp, label FROM (SELECT any(`value`) AS value, toStartOfInterval(timestamp, toIntervalSecond(15)) AS timestamp, * FROM `metrics`.`test001` GROUP BY timestamp, * ORDER BY timestamp ASC WITH FILL STEP toIntervalSecond(15) INTERPOLATE (label, value)) GROUP BY timestamp, label
//SELECT any(`value`) AS value, toStartOfInterval(timestamp, toIntervalSecond(15)) AS timestamp, * FROM `metrics`.`test001` GROUP BY timestamp, * ORDER BY timestamp ASC WITH FILL STEP toIntervalSecond(15) INTERPOLATE (label, value) LIMIT 5
//SELECT any(`value`) AS value, timestamp, * FROM `metrics`.`test001` GROUP BY timestamp, * ORDER BY timestamp ASC WITH FILL STEP toIntervalSecond(15) INTERPOLATE (label, value) LIMIT 5