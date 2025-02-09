package clickhouse

import (
    "io"
    "fmt"
    "log"
    //"os"
    "time"
    "sync"
    "context"
    //"errors"
    "crypto/sha1"
    "encoding/hex"
    "regexp"
    "strings"
    "reflect"
    "encoding/json"
    //"database/sql"
    client "github.com/ClickHouse/clickhouse-go/v2"
    "github.com/ltkh/montools/internal/config/mtprom"
    "github.com/prometheus/prometheus/model/labels"
    "github.com/prometheus/prometheus/promql/parser"
)

type Client struct {
    client client.Conn
    config *config.Source
    tables Tables
}

type Tables struct {
    sync.RWMutex
    Items map[string]Items
}

type Items struct {
    Labels map[string]bool
    Values map[string]bool
}

func NewClient(conf *config.Source) (*Client, error) {
    conn, err := client.Open(&client.Options{
        Addr: conf.Addr,
        Auth: client.Auth{
			Database: conf.Database,
			Username: conf.Username,
			Password: conf.Password,
		},
        Settings: client.Settings{
        	"max_execution_time": conf.MaxExecutionTime,
        },
        //DialTimeout: time.Second * conf.DialTimeout,
        Compression: &client.Compression{
            Method: client.CompressionLZ4,
        },
        Protocol:  client.HTTP,
    })
	if err != nil {
		return &Client{}, err
	}

    cache := Tables{
        Items: make(map[string]Items),
    }

    return &Client{ client: conn, config: conf, tables: cache }, nil
}

func inArray(value string, array []string) bool {
    if len(array) == 0 {
        return true
    }

    for _, val := range array {
        matched, err := regexp.MatchString("^"+val+"$", value)
        if err != nil {
            log.Printf("[error] regexp match %v", err)
            return false
        }
        if matched {
            return true
        }
    }

    return false
}

func getHash(text string) string {
    h := sha1.New()
    io.WriteString(h, text)
    return hex.EncodeToString(h.Sum(nil))
}

func (db *Client) Close() error {
    db.client.Close()

    return nil
}

func (db *Client) LabelValues(name string, start, end time.Time) ([]string, error) {
    var values []string

    dbs, err := db.client.Query(context.Background(), "SHOW DATABASES")
    if err != nil {
        return values, err
    }
    defer dbs.Close()

    for dbs.Next() {
        var dbase string
        if err := dbs.Scan(&dbase); err != nil {
            return values, err
        }

        dbCtx := client.Context(context.Background(), client.WithParameters(client.Parameters{
            "dbase": dbase,
        }))
        tbs, err := db.client.Query(dbCtx, "SHOW TABLES FROM {dbase:Identifier}")
        if err != nil {
            return values, err
        }
        defer tbs.Close()

        for tbs.Next() {
            labels := make(map[string]bool)

            var table string
            if err := tbs.Scan(&table); err != nil {
                return values, err
            }

            if !inArray(fmt.Sprintf("%v.%v", dbase, table), db.config.TableNames) {
                continue
            }

            chCtx := client.Context(context.Background(), client.WithParameters(client.Parameters{
                "dbase": dbase,
                "table": table,
                "start": start.Format("2006-01-02 15:04:05"), 
                "end": end.Format("2006-01-02 15:04:05"),
                "timestamp": db.config.Tables[0].Timestamp,
            }))

            rows, err := db.client.Query(chCtx, 
                `
                    SELECT * FROM {dbase:Identifier}.{table:Identifier} 
                    WHERE {timestamp:Identifier} BETWEEN toDateTime({start:String},'UTC') AND toDateTime({end:String},'UTC')
                `,
            )
            if err != nil {
                log.Printf("[error] read table %s: %v", table, err)
                continue
            }
            defer rows.Close()

            columnTypes := rows.ColumnTypes()
            vars := make([]interface{}, len(columnTypes))
            for i := range columnTypes {
                vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
            }

            for rows.Next() {
                if err := rows.Scan(vars...); err != nil {
                    return values, err
                }

                for i, val := range vars {
                    ctype := columnTypes[i]
                    if name == "" {
                        if ctype.DatabaseTypeName() == "String" && inArray(ctype.Name(), db.config.Tables[0].LabelNames) {
                            labels[ctype.Name()] = true
                        }
                    } else if name == "__name__" {
                        //log.Printf("[debug] %v - %v", ctype.Name(), ctype.DatabaseTypeName())
                        if strings.Contains(ctype.DatabaseTypeName(), "Int") && inArray(ctype.Name(), db.config.Tables[0].ValueNames) {
                            labels[fmt.Sprintf("%s:%s:%s", dbase, table, ctype.Name())] = true
                        }
                        if strings.Contains(ctype.DatabaseTypeName(), "Float") && inArray(ctype.Name(), db.config.Tables[0].ValueNames) {
                            labels[fmt.Sprintf("%s:%s:%s", dbase, table, ctype.Name())] = true
                        }
                    } else {
                        if name == ctype.Name() && ctype.DatabaseTypeName() == "String" {
                            switch v := val.(type) {
                                case *string:
                                    labels[*v] = true
                            }
                        }
                    }
                }

                if name == "__name__" {
                    break
                }
            }

            for key, _ := range labels {
                values = append(values, key)
            }
        }
    }

    return values, nil
}

func (db *Client) Labels(start, end time.Time) ([]string, error) {
    labels, err := db.LabelValues("", start, end)
    if err != nil {
        return []string{}, err
    }

    return labels, nil
}

func (db *Client) Series(match string, start, end time.Time) ([]map[string]string, error) {
    var series []map[string]string

    expr, err := parser.ParseExpr(match)
    if err != nil {
        return series, err
    }

    selectors := parser.ExtractSelectors(expr)
    for _, sel := range selectors {
        clientParams := client.Parameters{}
        columns := "*"
        groupBy := []string{}
        conditions := []string{}
        
        for _, s := range sel {
            if s.Name == "__name__" {
                params := strings.Split(s.Value, ":")
                if len(params) < 2 {
                    return series, nil
                }
                clientParams["__name__"] = s.Value
                clientParams["dbase"] = params[0]
                clientParams["table"] = params[1]
            } else {
                switch s.Type {
                    case labels.MatchEqual:
                        conditions = append(conditions, fmt.Sprintf(" AND equals(%s, '%s')", s.Name, s.Value))
                    case labels.MatchNotEqual:
                        conditions = append(conditions, fmt.Sprintf(" AND notEquals(%s, '%s')", s.Name, s.Value))
                    case labels.MatchRegexp:
                        conditions = append(conditions, fmt.Sprintf(" AND match(%s, '%s')", s.Name, s.Value))
                    case labels.MatchNotRegexp:
                        conditions = append(conditions, fmt.Sprintf(" AND NOT match(%s, '%s')", s.Name, s.Value))
                }
                groupBy = append(groupBy, s.Name)
                clientParams[s.Name] = s.Value
            }
            
        }

        if _, ok := clientParams["__name__"]; ok == false {
            return series, nil
        }
        //log.Printf("[debug] match: %v", match)

        clientParams["start"] = start.Format("2006-01-02 15:04:05")
        clientParams["end"] = end.Format("2006-01-02 15:04:05")
        clientParams["timestamp"] = db.config.Tables[0].Timestamp
        if len(groupBy) > 0 {
            columns = strings.Join(groupBy, ",")
        }

        chCtx := client.Context(context.Background(), client.WithParameters(clientParams))
        
        rows, err := db.client.Query(chCtx, fmt.Sprintf(
            `
                SELECT %s FROM {dbase:Identifier}.{table:Identifier} 
                WHERE {timestamp:Identifier} BETWEEN toDateTime({start:String},'UTC') AND toDateTime({end:String},'UTC') %s 
                GROUP BY ALL
            `, 
            columns,
            strings.Join(conditions, ""),
        ))
        if err != nil {
            log.Printf("[error] read table %s: %v", clientParams["table"], err)
            return series, err
        }
        defer rows.Close()

        columnTypes := rows.ColumnTypes()
        vars := make([]interface{}, len(columnTypes))
        for i := range columnTypes {
            vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
        }

        for rows.Next() {
            if err := rows.Scan(vars...); err != nil {
                return series, err
            }

            labels := make(map[string]string)
            labels["__name__"] = clientParams["__name__"]
            
            for i, val := range vars {
                ctype := columnTypes[i]
                if ctype.DatabaseTypeName() == "String" && inArray(ctype.Name(), db.config.Tables[0].LabelNames) {
                    switch v := val.(type) {
                        case *string:
                            labels[ctype.Name()] = *v
                    }
                    //if reflect.TypeOf(val).String() == "*string" {
                    //    log.Printf("[test] %v - %v", ctype.Name(), reflect.ValueOf(&val).String())
                    //}
                }
            }

            series = append(series, labels)
        }
    }

    return series, nil
}

func (db *Client) TableLabels(clientParams client.Parameters) ([]string, error) {
    var labels []string

    chCtx := client.Context(context.Background(), client.WithParameters(clientParams))

    rows, err := db.client.Query(chCtx, "SELECT * FROM {dbase:Identifier}.{table:Identifier} LIMIT 1")
    if err != nil {
        return labels, err
    }
    defer rows.Close()

    for _, ctype := range rows.ColumnTypes() {
        if ctype.DatabaseTypeName() == "String" && inArray(ctype.Name(), db.config.Tables[0].LabelNames) {
            labels = append(labels, ctype.Name())
        }
    }

    return labels, nil
}

func (db *Client) Aggregate(name string, grouping []string, expr parser.Node) (string, error) {
    grouping = append(grouping, "timestamp")

    sel, err := db.QueryWalk(expr)
    if err != nil {
        return "", err
    }

    switch name {
        case "sum":
            return fmt.Sprintf("SELECT sum(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ",")), nil
        case "count":
            return fmt.Sprintf("SELECT count(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ",")), nil
        case "min":
            return fmt.Sprintf("SELECT min(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ",")), nil
        case "max":
            return fmt.Sprintf("SELECT max(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ",")), nil
        case "avg":
            return fmt.Sprintf("SELECT avg(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ",")), nil
    }

    return "", nil
}

func (db *Client) Binary(name string, LHS, RHS parser.Node) (string, error) {
    selL, err := db.QueryWalk(LHS)
    if err != nil {
        return "", err
    }

    selR, err := db.QueryWalk(RHS)
    if err != nil {
        return "", err
    }

    switch name {
        case "+":
            return fmt.Sprintf("SELECT (s1.value + s2.value) AS value, timestamp FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), nil
        case "-":
            return fmt.Sprintf("SELECT (s1.value - s2.value) AS value, timestamp FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), nil
        case "/":
            return fmt.Sprintf("SELECT (s1.value / s2.value) AS value, timestamp FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), nil
        case "*":
            return fmt.Sprintf("SELECT (s1.value * s2.value) AS value, timestamp FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), nil
    }

    return "", nil
}

func (db *Client) Selector(labs []*labels.Matcher) (string, error) {
    clientParams := client.Parameters{}
    conditions := []string{"timestamp BETWEEN toDateTime({start:String},'UTC') AND toDateTime({end:String},'UTC')"}

    for _, l := range labs {
        if l.Name == "__name__" {
            vals := strings.Split(l.Value, ":")
            if len(vals) < 3 {
                return "", nil
            }
            clientParams["dbase"] = vals[0]
            clientParams["table"] = vals[1]
            clientParams["value"] = vals[2]
        } else {
            switch l.Type {
                case labels.MatchEqual:
                    conditions = append(conditions, fmt.Sprintf(" AND equals(%s, '%s')", l.Name, l.Value))
                case labels.MatchNotEqual:
                    conditions = append(conditions, fmt.Sprintf(" AND notEquals(%s, '%s')", l.Name, l.Value))
                case labels.MatchRegexp:
                    conditions = append(conditions, fmt.Sprintf(" AND match(%s, '%s')", l.Name, l.Value))
                case labels.MatchNotRegexp:
                    conditions = append(conditions, fmt.Sprintf(" AND NOT match(%s, '%s')", l.Name, l.Value))
            }
        }
    }

    fields := []string{
        fmt.Sprintf("any(`%s`) AS value", clientParams["value"]),
        "toStartOfInterval(timestamp, toIntervalSecond({step:Float64})) AS timestamp",
    }

    interpolate, _ := db.TableLabels(clientParams)
    fields = append(fields, interpolate...)

    interpolate = append(interpolate, "value")

    query := fmt.Sprintf(
        "SELECT %s FROM `%s`.`%s` WHERE %s GROUP BY timestamp, * ORDER BY timestamp ASC WITH FILL STEP toIntervalSecond({step:Float64}) INTERPOLATE (%s)",
        strings.Join(fields, ", "),
        clientParams["dbase"],
        clientParams["table"],
        strings.Join(conditions, ""),
        strings.Join(interpolate, ", "),
    )

    return query, nil
}

func (db *Client) QueryWalk(node parser.Node) (string, error) {
    fmt.Printf("Walk: %s\n", "-------------------------------")
    switch n := node.(type) {
	    case *parser.BinaryExpr:
            //fmt.Printf("Binary Expression: %s\n", n.Op)
            return db.Binary(n.Op.String(), n.LHS, n.RHS)
		case *parser.AggregateExpr:
			//fmt.Printf("Aggregate Expression: %s\n", n.Op)
            //fmt.Printf("Grouping - %v\n", n.Grouping)
			//Walk(query, n.Expr)
            return db.Aggregate(n.Op.String(), n.Grouping, n.Expr)
		case *parser.Call:
			fmt.Printf("Function: %s\n", n.Func.Name)
			//for _, arg := range n.Args {
            //    Walk(query, arg)
            //}
		case *parser.MatrixSelector:
			fmt.Printf("Matrix: %s\n", n.String())
			//Walk(query, n.VectorSelector)
		case *parser.VectorSelector:
			//fmt.Printf("Vector: %s\n", n.String())
            //fmt.Printf("label - %v\n", n.LabelMatchers)
            return db.Selector(n.LabelMatchers)
        default:
            fmt.Printf("Unknown node type: %T\n", n)
    }

    return "", nil
}

func (db *Client) Query(query string, limit int, time time.Time, timeout time.Duration) ([]config.Result, error) {
    var results []config.Result

    //results, err := db.Query(query, 1, start, end, step)
    //if err != nil {
    //    return results, err
    //}

    return results, nil
}

func (db *Client) QueryRange(query string, limit int, start, end time.Time, step time.Duration) ([]config.Result, error) {
    var results []config.Result
    
    resMap := make(map[string]config.Result)

    expr, err := parser.ParseExpr(query)
    if err != nil {
        return results, err
    }

    sel, err := db.QueryWalk(expr)
    if err != nil {
        return results, err
    }

    fmt.Printf("%s\n\n", sel)

    clientParams := client.Parameters{}
    clientParams["start"] = start.Format("2006-01-02 15:04:05")
    clientParams["end"] = end.Format("2006-01-02 15:04:05")
    clientParams["step"] = fmt.Sprintf("%.3f", step.Seconds())

    chCtx := client.Context(context.Background(), client.WithParameters(clientParams))

    rows, err := db.client.Query(chCtx, sel)
    if err != nil {
        log.Printf("[error] read table %s: %v", clientParams["table"], err)
        return results, err
    }
    defer rows.Close()

    columnTypes := rows.ColumnTypes()
    vars := make([]interface{}, len(columnTypes))
    for i := range columnTypes {
        vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
    }

    for rows.Next() {
        if err := rows.Scan(vars...); err != nil {
            return results, err
        }

        metric := make(map[string]string)
        metric["__name__"] = "value"

        value := []interface{}{float64(time.Now().Unix()), ""}

        for i, val := range vars {
            ctype := columnTypes[i]
            //log.Printf("[debug] %v", ctype.Name())
            if ctype.DatabaseTypeName() == "String" {
                switch v := val.(type) {
                    case *string:
                        metric[ctype.Name()] = *v
                }
            }
            if ctype.Name() == "value" {
                //log.Printf("[debug] %v", reflect.TypeOf(val))
                switch v := val.(type) {
                    case *int32:
                        value[1] = fmt.Sprintf("%v", *v)
                    case *uint8:
                        value[1] = fmt.Sprintf("%v", *v)
                    case *uint64:
                        value[1] = fmt.Sprintf("%v", *v)
                    case *float32:
                        value[1] = fmt.Sprintf("%v", *v)
                    case *float64:
                        value[1] = fmt.Sprintf("%v", *v)
                }
            }
            if ctype.Name() == "timestamp" {
                switch v := val.(type) {
                    case *time.Time:
                        timestamp := *v
                        value[0] = float64(timestamp.Unix())
                }
            }
        }

        text, err := json.Marshal(metric)
        if err != nil {
            return results, err
        }
        hash := getHash(string(text))

        if res, ok := resMap[hash]; ok {
            res.Values = append(res.Values, value)
            resMap[hash] = res
        } else {
            resMap[hash] = config.Result{
                Metric: metric,
                Values: []interface{}{value},
            }
        }
    }

    /*
    selectors := parser.ExtractSelectors(expr)
    for _, sel := range selectors {
        clientParams := client.Parameters{}
        querySelect := []string{
            "any({value:Identifier}) AS value",
            "toStartOfInterval({timestamp:Identifier}, toIntervalSecond({step:Float64})) AS timestamp",
        }
        conditions := []string{
            "{timestamp:Identifier} BETWEEN toDateTime({start:String},'UTC') AND toDateTime({end:String},'UTC')",
        } 
        //queryLimit := ""

        for _, s := range sel {
            if s.Name == "__name__" {
                params := strings.Split(s.Value, ":")
                if len(params) < 3 {
                    return results, nil
                }
                clientParams["__name__"] = s.Value
                clientParams["dbase"] = params[0]
                clientParams["table"] = params[1]
                clientParams["value"] = params[2]
            } else {
                switch s.Type {
                    case labels.MatchEqual:
                        conditions = append(conditions, fmt.Sprintf(" AND equals(%s, '%s')", s.Name, s.Value))
                    case labels.MatchNotEqual:
                        conditions = append(conditions, fmt.Sprintf(" AND notEquals(%s, '%s')", s.Name, s.Value))
                    case labels.MatchRegexp:
                        conditions = append(conditions, fmt.Sprintf(" AND match(%s, '%s')", s.Name, s.Value))
                    case labels.MatchNotRegexp:
                        conditions = append(conditions, fmt.Sprintf(" AND NOT match(%s, '%s')", s.Name, s.Value))
                }
                clientParams[s.Name] = s.Value
            }
        }

        if _, ok := clientParams["__name__"]; ok == false {
            return results, nil
        }

        groupBy, _ := db.TableLabels(clientParams)
        //log.Printf("[debug] %v", groupBy)

        query := fmt.Sprintf(
            `
                SELECT %[1]s FROM {dbase:Identifier}.{table:Identifier}
                WHERE %[2]s
                GROUP BY timestamp ORDER BY timestamp ASC 
                WITH FILL STEP toIntervalSecond({step:Float64})
                INTERPOLATE (value)
            `,
            strings.Join(querySelect, ", "),
            strings.Join(conditions, ""),
        )

        if len(groupBy) > 0 {
            querySelect = append(querySelect, groupBy...)
            query = fmt.Sprintf(
                `
                    SELECT %[1]s FROM {dbase:Identifier}.{table:Identifier} m1
                    LEFT JOIN (
                        SELECT %[3]s FROM {dbase:Identifier}.{table:Identifier}
                        WHERE %[2]s GROUP BY timestamp, %[3]s
                    ) m2 
                    ON m1.label = m2.label AND %[2]s
                    GROUP BY timestamp, %[3]s ORDER BY timestamp ASC 
                    WITH FILL STEP toIntervalSecond({step:Float64}) 
                    INTERPOLATE (value, %[3]s)
                `,
                strings.Join(querySelect, ", "),
                strings.Join(conditions, ""),
                strings.Join(groupBy, ", "),
            )
        }

        clientParams["start"] = start.Format("2006-01-02 15:04:05")
        clientParams["end"] = end.Format("2006-01-02 15:04:05")
        clientParams["step"] = fmt.Sprintf("%.3f", step.Seconds())
        clientParams["timestamp"] = db.config.Tables[0].Timestamp

        chCtx := client.Context(context.Background(), client.WithParameters(clientParams))

        if limit > 0 {
            //queryLimit = fmt.Sprintf("LIMIT %d", limit)
        }
        
        rows, err := db.client.Query(chCtx, query)

        if err != nil {
            log.Printf("[error] read table %s: %v", clientParams["table"], err)
            return results, err
        }
        defer rows.Close()

        columnTypes := rows.ColumnTypes()
        vars := make([]interface{}, len(columnTypes))
        for i := range columnTypes {
            vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
        }

        for rows.Next() {
            if err := rows.Scan(vars...); err != nil {
                return results, err
            }

            metric := make(map[string]string)
            metric["__name__"] = clientParams["__name__"]

            value := []interface{}{float64(time.Now().Unix()), ""}

            for i, val := range vars {
                ctype := columnTypes[i]
                //log.Printf("[debug] %v", ctype.Name())
                if ctype.DatabaseTypeName() == "String" && inArray(ctype.Name(), db.config.Tables[0].LabelNames) {
                    switch v := val.(type) {
                        case *string:
                            metric[ctype.Name()] = *v
                    }
                }
                if ctype.Name() == "value" {
                    //log.Printf("[debug] %v", reflect.TypeOf(val))
                    switch v := val.(type) {
                        case *int32:
                            value[1] = fmt.Sprintf("%v", *v)
                        case *uint8:
                            value[1] = fmt.Sprintf("%v", *v)
                        case *uint64:
                            value[1] = fmt.Sprintf("%v", *v)
                        case *float32:
                            value[1] = fmt.Sprintf("%v", *v)
                        case *float64:
                            value[1] = fmt.Sprintf("%v", *v)
                    }
                }
                if ctype.Name() == "timestamp" {
                    switch v := val.(type) {
                        case *time.Time:
                            timestamp := *v
                            value[0] = float64(timestamp.Unix())
                    }
                }
            }

            text, err := json.Marshal(metric)
            if err != nil {
                return results, err
            }
            hash := getHash(string(text))

            if res, ok := resMap[hash]; ok {
                res.Values = append(res.Values, value)
                resMap[hash] = res
            } else {
                resMap[hash] = config.Result{
                    Metric: metric,
                    Values: []interface{}{value},
                }
            }

            //log.Printf("[debug] %v", resMap[hash])
            
        }
    }
    */

    /*
    metric := config.Result{
        Metric: map[string]string{"__name__": "kqi_base:count","test":"test"},
        Values: []interface{}{
            []interface{}{float64(time.Now().Unix()-60), "1"},
            []interface{}{float64(time.Now().Unix()-45), "1"},
            []interface{}{float64(time.Now().Unix()-30), "1"},
            []interface{}{float64(time.Now().Unix()-15), "2"},
        },
    }

    results = append(results, metric)
    */

    for _, res := range resMap {
        results = append(results, res)
    }
  
    return results, nil
}