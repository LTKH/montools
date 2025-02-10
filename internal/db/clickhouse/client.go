package clickhouse

import (
    "io"
    "fmt"
    "log"
    //"os"
    "time"
    "sync"
    "sort"
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
    debug bool
}

type Tables struct {
    sync.RWMutex
    Items map[string]Items
}

type Items struct {
    Labels map[string]bool
    Values map[string]bool
}

type Selector struct {
    Values []string
    Labels []string
    Fields []string
    Params client.Parameters
    Timestamp string
    Conditions []string
}

func NewClient(conf *config.Source, debug bool) (*Client, error) {
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

    return &Client{ client: conn, config: conf, tables: cache, debug: debug }, nil
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

func (db *Client) NewSelector(labs []*labels.Matcher) (Selector, error) {
    selector := Selector{
        Labels: []string{},
        Fields: []string{},
        Params: client.Parameters{},
        Conditions: []string{},
        Timestamp: "",
    }

    for _, l := range labs {
        if l.Name == "__name__" {
            vals := strings.Split(l.Value, ":")
            selector.Params["__name__"] = l.Value
            if len(vals) > 0 { selector.Params["dbase"] = vals[0] }
            if len(vals) > 1 { selector.Params["table"] = vals[1] }
            if len(vals) > 2 { selector.Params["value"] = vals[2] }
        } else {
            switch l.Type {
                case labels.MatchEqual:
                    selector.Conditions = append(selector.Conditions, fmt.Sprintf("equals(%s, '%s')", l.Name, l.Value))
                case labels.MatchNotEqual:
                    selector.Conditions = append(selector.Conditions, fmt.Sprintf("notEquals(%s, '%s')", l.Name, l.Value))
                case labels.MatchRegexp:
                    selector.Conditions = append(selector.Conditions, fmt.Sprintf("match(%s, '%s')", l.Name, l.Value))
                case labels.MatchNotRegexp:
                    selector.Conditions = append(selector.Conditions, fmt.Sprintf("NOT match(%s, '%s')", l.Name, l.Value))
            }
            selector.Fields = append(selector.Fields, l.Name)
        }
    }

    if _, ok := selector.Params["dbase"]; ok == false {
        return selector, fmt.Errorf("failed to determine target data base")
    }
    if _, ok := selector.Params["table"]; ok == false {
        return selector, fmt.Errorf("failed to determine target table")
    }

    for _, table := range db.config.Tables {
        matched, err := regexp.MatchString("^"+table.Name+"$", fmt.Sprintf("%s.%s", selector.Params["dbase"]))
        if err != nil {
            return selector, err
        }
        if matched {
            selector.Values = table.ValueNames
            selector.Labels = table.LabelNames
            selector.Timestamp = table.Timestamp
            return selector, nil
        }
    }

    chCtx := client.Context(context.Background(), client.WithParameters(selector.Params))

    rows, err := db.client.Query(chCtx, "SELECT * FROM {dbase:Identifier}.{table:Identifier} LIMIT 1")
    if err != nil {
        return selector, err
    }
    defer rows.Close()

    for _, ctype := range rows.ColumnTypes() {
        switch ctype.DatabaseTypeName() {
            case "Float32", "Float64", "Int32", "Int64":
                selector.Values = append(selector.Values, ctype.Name())
            case "String": 
                selector.Labels = append(selector.Labels, ctype.Name())
            case "DateTime":
                selector.Timestamp = ctype.Name()
        }
    }

    if len(selector.Values) == 0 {
        return selector, fmt.Errorf("unable to find value field in table: ", selector.Params["table"])
    }
    if selector.Timestamp == "" {
        return selector, fmt.Errorf("unable to find date field in table: ", selector.Params["table"])
    }

    return selector, nil
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
            qlabels := make(map[string]bool)

            var table string
            if err := tbs.Scan(&table); err != nil {
                return values, err
            }

            if !inArray(fmt.Sprintf("%v.%v", dbase, table), db.config.TableNames) {
                continue
            }

            selector, err := db.NewSelector([]*labels.Matcher{&labels.Matcher{Name: "__name__",Value: fmt.Sprintf("%s:%s", dbase, table)}})
            if err != nil {
                return values, err
            }
            selector.Conditions = append(selector.Conditions, fmt.Sprintf("`%s` BETWEEN toDateTime({start:String},'UTC') AND toDateTime({end:String},'UTC')", selector.Timestamp))
            selector.Params["start"] = start.Format("2006-01-02 15:04:05")
            selector.Params["end"] = end.Format("2006-01-02 15:04:05")

            chCtx := client.Context(context.Background(), client.WithParameters(selector.Params))

            rows, err := db.client.Query(chCtx, fmt.Sprintf("SELECT * FROM {dbase:Identifier}.{table:Identifier} WHERE %s", strings.Join(selector.Conditions, " AND ")))
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
                        if ctype.DatabaseTypeName() == "String" {
                            qlabels[ctype.Name()] = true
                        }
                    } else if name == "__name__" {
                        if strings.Contains(ctype.DatabaseTypeName(), "Int") {
                            qlabels[fmt.Sprintf("%s:%s:%s", dbase, table, ctype.Name())] = true
                        }
                        if strings.Contains(ctype.DatabaseTypeName(), "Float") {
                            qlabels[fmt.Sprintf("%s:%s:%s", dbase, table, ctype.Name())] = true
                        }
                    } else {
                        if name == ctype.Name() && ctype.DatabaseTypeName() == "String" {
                            switch v := val.(type) {
                                case *string:
                                    qlabels[*v] = true
                            }
                        }
                    }
                }

                if name == "__name__" {
                    break
                }
            }

            for key, _ := range qlabels {
                values = append(values, key)
            }
        }
    }

    sort.Strings(values)

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

    if db.debug {
        log.Printf("[series] %v", match)
    }

    selectors := parser.ExtractSelectors(expr)
    for _, sel := range selectors {
        columns := "*"

        selector, err := db.NewSelector(sel)
        if err != nil {
            return series, err
        }
        selector.Conditions = append(selector.Conditions, fmt.Sprintf("`%s` BETWEEN toDateTime({start:String},'UTC') AND toDateTime({end:String},'UTC')", selector.Timestamp))
        selector.Params["start"] = start.Format("2006-01-02 15:04:05")
        selector.Params["end"] = end.Format("2006-01-02 15:04:05")

        if _, ok := selector.Params["__name__"]; ok == false {
            return series, nil
        }

        if len(selector.Fields) > 0 {
            columns = strings.Join(selector.Fields, ", ")
        }

        chCtx := client.Context(context.Background(), client.WithParameters(selector.Params))
        
        rows, err := db.client.Query(chCtx, fmt.Sprintf(
            "SELECT %s FROM {dbase:Identifier}.{table:Identifier} WHERE %s GROUP BY ALL", 
            columns,
            strings.Join(selector.Conditions, " AND "),
        ))
        if err != nil {
            log.Printf("[error] read table %s: %v", selector.Params["table"], err)
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
            labels["__name__"] = selector.Params["__name__"]
            
            for i, val := range vars {
                ctype := columnTypes[i]
                if ctype.DatabaseTypeName() == "String" {
                    labels[ctype.Name()] = reflect.ValueOf(val).Elem().String()
                }
            }

            series = append(series, labels)
        }
    }

    return series, nil
}

func (db *Client) AggregateExpr(name string, grouping []string, expr parser.Node) (string, error) {
    grouping = append(grouping, "timestamp")

    sel, err := db.QueryWalk(expr, 0)
    if err != nil {
        return "", err
    }

    switch name {
        case "sum":
            return fmt.Sprintf("SELECT sum(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ", ")), nil
        case "count":
            return fmt.Sprintf("SELECT count(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ", ")), nil
        case "min":
            return fmt.Sprintf("SELECT min(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ", ")), nil
        case "max":
            return fmt.Sprintf("SELECT max(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ", ")), nil
        case "avg":
            return fmt.Sprintf("SELECT avg(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ", ")), nil
    }

    return "", fmt.Errorf("operator not supported by driver: ", name)
}

func (db *Client) BinaryExpr(name string, LHS, RHS parser.Node) (string, error) {
    selL, err := db.QueryWalk(LHS, 0)
    if err != nil {
        return "", err
    }

    selR, err := db.QueryWalk(RHS, 0)
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

func (db *Client) VectorSelector(labs []*labels.Matcher, lmt int) (string, error) {
    selector, err := db.NewSelector(labs)
    if err != nil {
        return "", err
    }
    selector.Conditions = append(selector.Conditions, fmt.Sprintf("`%s` BETWEEN toDateTime({start:String},'UTC') AND toDateTime({end:String},'UTC')", selector.Timestamp))

    fields := []string{
        fmt.Sprintf("any(`%s`) AS value", selector.Params["value"]),
        fmt.Sprintf("toStartOfInterval(`%s`, toIntervalSecond({step:Float64})) AS timestamp", selector.Timestamp),
        fmt.Sprintf("'%s:%s:%s' AS __name__", selector.Params["dbase"], selector.Params["table"], selector.Params["value"]),
    }
    fields = append(fields, selector.Labels...)

    limit := ""
    if lmt > 0 { limit = fmt.Sprintf("LIMIT %d", lmt) }

    interpolate := append(selector.Labels, "value")

    query := fmt.Sprintf(
        "SELECT %s FROM `%s`.`%s` WHERE %s GROUP BY ALL ORDER BY timestamp ASC WITH FILL STEP toIntervalSecond({step:Float64}) INTERPOLATE (%s) %s",
        strings.Join(fields, ", "),
        selector.Params["dbase"],
        selector.Params["table"],
        strings.Join(selector.Conditions, " AND "),
        strings.Join(interpolate, ", "),
        limit,
    )

    return query, nil
}

func (db *Client) QueryWalk(node parser.Node, limit int) (string, error) {
    //fmt.Printf("Walk: %s\n", "-------------------------------")
    switch n := node.(type) {
	    case *parser.BinaryExpr:
            //fmt.Printf("Binary Expression: %s\n", n.Op)
            return db.BinaryExpr(n.Op.String(), n.LHS, n.RHS)
		case *parser.AggregateExpr:
			//fmt.Printf("Aggregate Expression: %s\n", n.Op)
            //fmt.Printf("Grouping - %v\n", n.Grouping)
			//Walk(query, n.Expr)
            return db.AggregateExpr(n.Op.String(), n.Grouping, n.Expr)
		case *parser.Call:
            return "", fmt.Errorf("unsupported function: %v", n.Func.Name)
			//fmt.Printf("Function: %s\n", n.Func.Name)
			//for _, arg := range n.Args {
            //    Walk(query, arg)
            //}
		case *parser.MatrixSelector:
			fmt.Printf("Matrix: %s\n", n.String())
			//Walk(query, n.VectorSelector)
		case *parser.VectorSelector:
			//fmt.Printf("Vector: %s\n", n.String())
            //fmt.Printf("label - %v\n", n.LabelMatchers)
            return db.VectorSelector(n.LabelMatchers, limit)
        //case *parser.NumberLiteral:
        //    fmt.Printf("Literal: %v\n", n)
        //    fmt.Printf("Pos: %v\n", n.PosRange)
        default:
            return "", fmt.Errorf("unknown node type: %T\n", n)
    }

    return "", nil
}

func (db *Client) Query(query string, limit int, start time.Time, timeout time.Duration) ([]config.Result, error) {
    if db.debug {
        log.Printf("[query] %v", query)
    }

    results, err := db.QueryRange(query, 1, start, time.Now(), 1 * time.Minute)
    if err != nil {
        return []config.Result{}, err
    }

    return results, nil
}

func (db *Client) QueryRange(query string, limit int, start, end time.Time, step time.Duration) ([]config.Result, error) {
    var results []config.Result
    
    resMap := make(map[string]config.Result)

    expr, err := parser.ParseExpr(query)
    if err != nil {
        return results, err
    }

    sel, err := db.QueryWalk(expr, limit)
    if err != nil {
        return results, err
    }

    if db.debug {
        log.Printf("[range] %v", query)
        log.Printf("[range] %v", sel)
    }

    clientParams := client.Parameters{}
    clientParams["start"] = start.Format("2006-01-02 15:04:05")
    clientParams["end"] = end.Format("2006-01-02 15:04:05")
    clientParams["step"] = fmt.Sprintf("%.3f", step.Seconds())

    chCtx := client.Context(context.Background(), client.WithParameters(clientParams))

    rows, err := db.client.Query(chCtx, sel)
    if err != nil {
        log.Printf("[error] %v", err)
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
        value := []interface{}{0.0, ""}

        for i, val := range vars {
            ctype := columnTypes[i]
            if ctype.DatabaseTypeName() == "String" {
                metric[ctype.Name()] = reflect.ValueOf(val).Elem().String()
            }
            if ctype.Name() == "value" {
                value[1] = fmt.Sprintf("%v", reflect.ValueOf(val).Elem())
            }
            if ctype.Name() == "timestamp" {
                if reflect.TypeOf(val).String() == "*time.Time" {
                    timestamp := reflect.ValueOf(val).Interface().(*time.Time)
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

    keys := make([]string, 0, len(resMap))
    for k := range resMap{
        keys = append(keys, k)
    }
    sort.Strings(keys)

    for _, k := range keys {
        results = append(results, resMap[k])
    }
  
    return results, nil
}