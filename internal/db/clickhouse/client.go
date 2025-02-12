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
    "database/sql"
    ch "github.com/ClickHouse/clickhouse-go/v2"
    "github.com/ltkh/montools/internal/config/mtprom"
    "github.com/prometheus/prometheus/model/labels"
    "github.com/prometheus/prometheus/promql/parser"
)

type Client struct {
    client *sql.DB
    config *config.Source
    tables Tables
    debug bool
}

type Tables struct {
    sync.RWMutex
    Items map[string]Item
}

type Item struct {
    Labels map[string]string
    Values map[string]string
    Times  map[string]string
}

type Selector struct {
    DBase  string
    Table  string
    Name   string
    Value  string
    Fields []string
    Values map[string]string
    Labels map[string]string
    Times  map[string]string
    Conditions []string
}

func NewClient(conf *config.Source, debug bool) (*Client, error) {
    conn := ch.OpenDB(&ch.Options{
        Addr: conf.Addr,
        Auth: ch.Auth{
            Database: conf.Database,
            Username: conf.Username,
            Password: conf.Password,
        },
        Settings: ch.Settings{
            "max_execution_time": conf.MaxExecutionTime,
        },
        //DialTimeout: time.Second * conf.DialTimeout,
        Compression: &ch.Compression{
            Method: ch.CompressionLZ4,
        },
        Protocol:  ch.HTTP,
    })

    cache := Tables{
        Items: make(map[string]Item),
    }

    db := &Client{ client: conn, config: conf, tables: cache, debug: debug }

    go func(){
        for {
            db.SetColumns(conf.DBaseNames, conf.TableNames)
            time.Sleep(600 * time.Second)
        }
    }()

    return db, nil
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

func mapKeys(items map[string]string) []string {
    keys := []string{}

    for k, _ := range items {
        keys = append(keys, k)
    }

    return keys
}

func getHash(text string) string {
    h := sha1.New()
    io.WriteString(h, text)
    return hex.EncodeToString(h.Sum(nil))
}

func (tb *Tables) Set(key string, labels, values, times map[string]string) error {
    tb.Lock()
    defer tb.Unlock()

    val, found := tb.Items[key]
    if !found {
        val = Item{
            Labels: map[string]string{},
            Values: map[string]string{},
            Times:  map[string]string{},
        }
    }

    for k, v := range labels {
        val.Labels[k] = v
    }
    for k, v := range values {
        val.Values[k] = v
    }
    for k, v := range times {
        val.Times[k] = v
    }
    
    tb.Items[key] = val

    return nil
}

func (tb *Tables) Get(key string) (Item, bool) {
    tb.RLock()
    defer tb.RUnlock()

    val, found := tb.Items[key]
    if !found {
        return Item{}, false
    }

    return val, true
}

func (tb *Tables) Values(name string) []string {
    tb.RLock()
    defer tb.RUnlock()

    var items []string

    for key, val := range tb.Items {
        if len(val.Times) > 0 {
            for v, _ := range val.Values {
                if name != "" && name != v { continue }
                items = append(items, fmt.Sprintf("%s:%s", key, v))
            }
        }
    }

    return items
}

func (tb *Tables) Labels() []string {
    tb.RLock()
    defer tb.RUnlock()

    var items []string

    for key, val := range tb.Items {
        if len(val.Times) == 0 {
            continue
        }
        //if len(val.Values) == 0 {
        //    continue
        //    val.Values["count"] = "Custom"
        //}
        for v, _ := range val.Labels {
            items = append(items, fmt.Sprintf("%s:%s", key, v))
        }
    }

    return items
}

func (db *Client) Close() error {
    db.client.Close()

    return nil
}

func (db *Client) GetTable(name string) *config.Table {
    for _, tab := range db.config.Tables {
        if m, _ := regexp.MatchString("^"+tab.Name+"$", name); m {
            return tab
        }
    }

    return &config.Table{}
}

func (db *Client) SetColumns(dbaseNames, tableNames []string) error {
    if len(dbaseNames) == 0 {
        dbaseNames = append(dbaseNames, ".*")
    }

    if len(tableNames) == 0 {
        tableNames = append(tableNames, ".*")
    }

    rows, err := db.client.QueryContext(
        context.Background(), 
        "SELECT database, table, name, type FROM system.columns WHERE match(database, @dbase) AND match(table, @table)",
        sql.Named("dbase", fmt.Sprintf("^(%s)$", strings.Join(dbaseNames, "|"))),
        sql.Named("table", fmt.Sprintf("^(%s)$", strings.Join(tableNames, "|"))),
    )
    if err != nil {
        log.Printf("[error] read table system.columns: %v", err)
        return err
    }
    defer rows.Close()

    for rows.Next() {
        var (
            dbase string
            table string
            fname string
            ftype string
        )

        if err := rows.Scan(&dbase, &table, &fname, &ftype); err != nil {
            return err
        }

        id := fmt.Sprintf("%s:%s", dbase, table)
        tb := db.GetTable(id)

        if len(tb.LabelTypes) == 0 {
            tb.LabelTypes = []string{"String"}
        }

        if len(tb.ValueTypes) == 0 {
            tb.ValueTypes = []string{"Int.*","Float.*"}
        }

        if len(tb.TimesTypes) == 0 {
            tb.TimesTypes = []string{"DateTime"}
        }

        labels := map[string]string{}
        values := map[string]string{}
        times  := map[string]string{}

        if inArray(ftype, tb.LabelTypes) && inArray(fname, tb.LabelNames) {
            labels[fname] = ftype
        }
        if inArray(ftype, tb.ValueTypes) && inArray(fname, tb.ValueNames) {
            values[fname] = ftype
        }
        if inArray(ftype, tb.TimesTypes) && inArray(fname, tb.TimesNames) {
            times[fname] = ftype
        }

        db.tables.Set(id, labels, values, times)
    }

    return nil
}

func (db *Client) NewSelector(dbase, table string, labs []*labels.Matcher) (Selector, error) {
    selector := Selector{
        DBase: dbase,
        Table: table,
        Name:  "1",
        Fields: []string{},
        Values: map[string]string{},
        Labels: map[string]string{},
        Times:  map[string]string{},
        Conditions: []string{},
        
    }

    for _, l := range labs {
        if l.Name == "__name__" {
            selector.Name = l.Value
            vals := strings.Split(l.Value, ":")
            if len(vals) > 0 { selector.DBase = vals[0] }
            if len(vals) > 1 { selector.Table = vals[1] }
            if len(vals) > 2 { selector.Value = vals[2] }
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

    if selector.DBase == "" {
        return selector, fmt.Errorf("failed to determine target data base")
    }
    if selector.Table == "" {
        return selector, fmt.Errorf("failed to determine target table")
    }

    id := fmt.Sprintf("%s:%s", selector.DBase, selector.Table)
    item, ok := db.tables.Get(id)

    if !ok {
        return selector, fmt.Errorf("unable to get table attributes: %s.%s", selector.DBase, selector.Table)
    }

    for k, v := range item.Labels {
        selector.Labels[k] = v
    }
    for k, v := range item.Values {
        selector.Values[k] = v
    }
    for k, v := range item.Times {
        selector.Times[k] = v
        break
    }

    if len(selector.Values) == 0 {
        return selector, fmt.Errorf("unable to find value field in table: %s.%s", selector.DBase, selector.Table)
    }
    if len(selector.Times) == 0 {
        return selector, fmt.Errorf("unable to find date field in table: %s.%s", selector.DBase, selector.Table)
    }

    return selector, nil
}

func (db *Client) LabelValues(name string, start, end time.Time) ([]string, error) {
    var results []string

    labs := make(map[string]bool)

    if name == "" {

        for _, val := range db.tables.Labels() {
            v := strings.Split(val, ":")
            if len(v) != 3 { continue }
            labs[v[2]] = true
        }
        for key, _ := range labs {
            results = append(results, key)
        }

    } else if name == "__name__" {
    
        /*
        if err := db.SetColumns(db.config.DBaseNames, db.config.TableNames); err != nil {
            return results, err
        }
        */

        results = db.tables.Values("")

    } else {
        
        for _, val := range db.tables.Values(name) {
            v := strings.Split(val, ":")
            if len(v) != 3 { continue }

            item, ok := db.tables.Get(fmt.Sprintf("%s:%s", v[0], v[1]))
            if !ok { continue }

            chCtx := ch.Context(context.Background(), ch.WithParameters(ch.Parameters{}))

            rows, err := db.client.QueryContext(chCtx, 
                fmt.Sprintf(
                    "SELECT `%s` FROM `%s`.`%s` WHERE `%s` BETWEEN toDateTime(@start,'UTC') AND toDateTime(@end,'UTC') GROUP BY ALL", 
                    name, v[0], v[1], mapKeys(item.Times)[0],
                ),
                sql.Named("start", start.Format("2006-01-02 15:04:05")),
                sql.Named("end", end.Format("2006-01-02 15:04:05")),
            )
            if err != nil {
                log.Printf("[error] read table %s: %v", v[1], err)
                continue
            }
            defer rows.Close()

            for rows.Next() {
                var value string

                if err := rows.Scan(&value); err != nil {
                    continue
                }

                if value != "" {
                    labs[value] = true
                }
            }
        }

        for key, _ := range labs {
            results = append(results, key)
        }
    }

    sort.Strings(results)

    return results, nil
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
        log.Printf("[series] %s", match)
    }

    selectors := parser.ExtractSelectors(expr)
    for _, sel := range selectors {

        selector, err := db.NewSelector("", "", sel)
        if err != nil {
            return series, err
        }
        selector.Conditions = append(
            selector.Conditions, 
            fmt.Sprintf("`%s` BETWEEN toDateTime(@start,'UTC') AND toDateTime(@end,'UTC')", mapKeys(selector.Times)[0]),
        )

        if selector.Name == "" {
            return series, nil
        }

        if len(selector.Labels) > 0 {

            chCtx := ch.Context(context.Background(), ch.WithParameters(ch.Parameters{}))

            query := fmt.Sprintf(
                "SELECT %s FROM `%s`.`%s` WHERE %s GROUP BY ALL LIMIT 100", 
                strings.Join(mapKeys(selector.Labels), ", "), 
                selector.DBase, 
                selector.Table, 
                strings.Join(selector.Conditions, " AND "),
            )
            if db.debug {
                log.Printf("[series] %s", query)
            }
            
            rows, err := db.client.QueryContext(
                chCtx, 
                query,
                sql.Named("start", start.Format("2006-01-02 15:04:05")),
                sql.Named("end", end.Format("2006-01-02 15:04:05")),
            )
            if err != nil {
                log.Printf("[error] read table %s: %v", selector.Table, err)
                return series, err
            }
            defer rows.Close()

            columnTypes, err := rows.ColumnTypes()
            if err != nil {
                return series, err
            }

            vars := make([]interface{}, len(columnTypes))
            for i := range columnTypes {
                vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
            }

            tb := db.GetTable(fmt.Sprintf("%s:%s", selector.DBase, selector.Table))

            if len(tb.LabelTypes) == 0 {
                tb.LabelTypes = []string{"String"}
            }

            for rows.Next() {
                if err := rows.Scan(vars...); err != nil {
                    return series, err
                }

                labels := make(map[string]string)
                labels["__name__"] = selector.Name
                
                for i, val := range vars {
                    ctype := columnTypes[i]
                    if inArray(ctype.DatabaseTypeName(), tb.LabelTypes) && inArray(ctype.Name(), tb.LabelNames) {
                        labels[ctype.Name()] = reflect.ValueOf(val).Elem().String()
                    }
                    
                }

                series = append(series, labels)
            }
        }

        if len(series) == 0 {
            series = append(series, map[string]string{"__name__": selector.Name})
        }
    }

    return series, nil
}

func (db *Client) AggregateExpr(name string, grouping []string, expr parser.Node) (string, []string, error) {
    grouping = append(grouping, "timestamp")

    sel, grouping, err := db.QueryWalk(expr, grouping)
    if err != nil {
        return "", grouping, err
    }

    switch name {
        case "sum":
            return fmt.Sprintf("SELECT sum(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ", ")), grouping, nil
        case "count":
            return fmt.Sprintf("SELECT count(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ", ")), grouping, nil
        case "min":
            return fmt.Sprintf("SELECT min(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ", ")), grouping, nil
        case "max":
            return fmt.Sprintf("SELECT max(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ", ")), grouping, nil
        case "avg":
            return fmt.Sprintf("SELECT avg(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, strings.Join(grouping, ", ")), grouping, nil
    }

    return "", grouping, fmt.Errorf("operator not supported by driver: ", name)
}

func (db *Client) BinaryExpr(name string, grouping []string, LHS, RHS parser.Node) (string, []string, error) {
    selL, _, err := db.QueryWalk(LHS, grouping)
    if err != nil {
        return "", []string{}, err
    }

    selR, _, err := db.QueryWalk(RHS, grouping)
    if err != nil {
        return "", []string{}, err
    }

    switch name {
        case "+":
            return fmt.Sprintf("SELECT (s1.value + s2.value) AS value, timestamp FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), []string{}, nil
        case "-":
            return fmt.Sprintf("SELECT (s1.value - s2.value) AS value, timestamp FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), []string{}, nil
        case "/":
            return fmt.Sprintf("SELECT (s1.value / s2.value) AS value, timestamp FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), []string{}, nil
        case "*":
            return fmt.Sprintf("SELECT (s1.value * s2.value) AS value, timestamp FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), []string{}, nil
    }

    return "", []string{}, nil
}

func (db *Client) VectorSelector(labs []*labels.Matcher, grouping []string) (string, []string, error) {

    selector, err := db.NewSelector("", "", labs)
    if err != nil {
        return "", grouping, err
    }
    selector.Conditions = append(
        selector.Conditions, 
        fmt.Sprintf("`%s` BETWEEN toDateTime(@start,'UTC') AND toDateTime(@end,'UTC')", mapKeys(selector.Times)[0]),
    )

    if db.debug {
        log.Printf("[selector] %s", selector)
    }

    fields := []string{
        //fmt.Sprintf("any(`%s`) AS value", selector.Value),
        fmt.Sprintf("`%s` AS value", selector.Value),
        fmt.Sprintf("`%s` AS timestamp", mapKeys(selector.Times)[0]),
        fmt.Sprintf("'%s:%s:%s' AS __name__", selector.DBase, selector.Table, selector.Value),
        //fmt.Sprintf("toStartOfInterval(`%s`, toIntervalSecond(@step)) AS timestamp", mapKeys(selector.Times)[0]),
    }

    if len(grouping) == 0 {
        for k, _ := range selector.Labels {
            fields = append(fields, k)
            grouping = append(grouping, k)
        }
    } else {
        for _, v := range grouping {
            if _, ok := selector.Labels[v]; !ok {
                return "", grouping, fmt.Errorf("unable to find `%s` field in table: %s.%s", v, selector.DBase, selector.Table)
            }
        }
    }

    query := fmt.Sprintf(
        //"SELECT %s FROM `%s`.`%s` WHERE %s GROUP BY %s ORDER BY timestamp ASC WITH FILL STEP toIntervalSecond(@step) INTERPOLATE (%s) %s",
        "SELECT %s FROM `%s`.`%s` WHERE %s GROUP BY %s ORDER BY timestamp",
        strings.Join(fields, ", "),
        selector.DBase,
        selector.Table,
        strings.Join(selector.Conditions, " AND "),
        strings.Join(append([]string{"value","timestamp","__name__"}, grouping...), ", "),
        //strings.Join(append([]string{"__name__","value"}, grouping...), ", "),
        //limit,
    )

    return query, grouping, nil
}

func (db *Client) QueryWalk(node parser.Node, grouping []string) (string, []string, error) {
    switch n := node.(type) {
        case *parser.BinaryExpr:
            return db.BinaryExpr(n.Op.String(), grouping, n.LHS, n.RHS)
        case *parser.AggregateExpr:
            return db.AggregateExpr(n.Op.String(), n.Grouping, n.Expr)
        case *parser.VectorSelector:
            return db.VectorSelector(n.LabelMatchers, grouping)
        case *parser.Call:
            return "", grouping, fmt.Errorf("unsupported function: %v", n.Func.Name)
            //fmt.Printf("Function: %s\n", n.Func.Name)
            //for _, arg := range n.Args {
            //    Walk(query, arg)
            //}
        //case *parser.NumberLiteral:
        //    fmt.Printf("Literal: %v\n", n)
        //    fmt.Printf("Pos: %v\n", n.PosRange)
        //case *parser.MatrixSelector:
        //    fmt.Printf("Matrix: %s\n", n.String())
        //    //Walk(query, n.VectorSelector)
        default:
            return "", grouping, fmt.Errorf("unknown node type: %T\n", n)
    }

    return "", grouping, nil
}

func (db *Client) Query(query string, limit int, start time.Time, timeout time.Duration) ([]config.Result, error) {
    if db.debug {
        log.Printf("[query] %s", query)
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

    sel, _, err := db.QueryWalk(expr, []string{})
    if err != nil {
        return results, err
    }

    sel = fmt.Sprintf(
        "SELECT %s FROM (%s) GROUP BY %s ORDER BY timestamp %s", 
        "__name__, any(`value`) AS value, toStartOfInterval(`timestamp`, toIntervalSecond(@step)) AS timestamp, label",
        sel,
        "timestamp, __name__, label",
        "WITH FILL STEP toIntervalSecond(@step) INTERPOLATE (__name__, value)",
    )

    if db.debug {
        log.Printf("[range] %s", query)
        log.Printf("[range] %s", sel)
    }

    chCtx := ch.Context(context.Background(), ch.WithParameters(ch.Parameters{}))

    rows, err := db.client.QueryContext(
        chCtx, 
        sel,
        sql.Named("start", start.Format("2006-01-02 15:04:05")),
        sql.Named("end", end.Format("2006-01-02 15:04:05")),
        sql.Named("step", step.Seconds()),
    )
    if err != nil {
        log.Printf("[error] %v", err)
        return results, err
    }
    defer rows.Close()

    columnTypes, err := rows.ColumnTypes()
    if err != nil {
        return results, err
    }

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

            if ctype.Name() != "value" && ctype.Name() != "timestamp" {
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