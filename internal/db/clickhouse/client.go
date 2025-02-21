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
    cache Cache
    debug bool
}

type Cache struct {
    sync.RWMutex
    Items map[string]Item
}

type Item struct {
    Fields map[string]string
    Times  map[string]string
}

type Selector struct {
    //DBase  string
    //Table  string
    //Name   string
    Tables map[string]Table
}

type Table struct {
    DBase  string
    Table  string
    Value  string
    Fields map[string]string
    Times  map[string]string
    Columns []string
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

    cache := Cache{
        Items: make(map[string]Item),
    }

    db := &Client{ client: conn, config: conf, cache: cache, debug: debug }

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

    values := fmt.Sprintf("^(%s)$", strings.Join(array, "|"))
    matched, err := regexp.MatchString(values, value)
    if err != nil {
        log.Printf("[error] regexp match %v", err)
        return false
    }
    if matched {
        return true
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

func (c *Cache) Set(key string, fields, times map[string]string) error {
    c.Lock()
    defer c.Unlock()

    item, found := c.Items[key]
    if !found {
        item = Item{
            Fields: map[string]string{},
            Times:  map[string]string{},
        }
    }

    for k, v := range fields {
        item.Fields[k] = v
    }
    for k, v := range times {
        item.Times[k] = v
    }
    
    c.Items[key] = item

    return nil
}

func (c *Cache) Get(key string) (Item, bool) {
    c.RLock()
    defer c.RUnlock()

    item, found := c.Items[key]
    if !found {
        return Item{}, false
    }

    return item, true
}

func (c *Cache) Keys() []string {
    c.RLock()
    defer c.RUnlock()

    var items []string

    for key, _ := range c.Items {
        items = append(items, key)
    }

    return items
}

func (c *Cache) Fields() []string {
    c.RLock()
    defer c.RUnlock()

    var items []string

    for key, val := range c.Items {
        if len(val.Times) == 0 {
            continue
        }
        for k, _ := range val.Fields {
            items = append(items, fmt.Sprintf("%s:%s", key, k))
        }
    }

    return items
}

func (c *Cache) Values() []string {
    c.RLock()
    defer c.RUnlock()

    var items []string

    for key, item := range c.Items {
        if len(item.Times) == 0 {
            continue
        }
        for k, v := range item.Fields {
            if inArray(v, []string{".*Int.*",".*Float.*"}) {
                items = append(items, fmt.Sprintf("%s:%s", key, k))
            }
        }
    }

    return items
}

func (db *Client) Close() error {
    db.client.Close()

    return nil
}

func (db *Client) GetConfTable(name string) *config.Table {
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
        tb := db.GetConfTable(id)

        fields := map[string]string{}
        times  := map[string]string{}

        if inArray(fname, tb.FieldNames) {
            fields[fname] = ftype
        }
        if inArray(fname, tb.TimeNames) && inArray(ftype, []string{"DateTime.*"}) {
            times[fname] = ftype
        }

        db.cache.Set(id, fields, times)
    }

    return nil
}

func (db *Client) GetTable(dbase, table, value string, labs []*labels.Matcher) (Table, error) {
    result := Table{
        DBase: dbase,
        Table: table,
        Value: value,
        Fields: map[string]string{},
        Times:  map[string]string{},
        Columns: []string{},
        Conditions: []string{},
    }

    item, ok := db.cache.Get(fmt.Sprintf("%s:%s", dbase, table))
    if !ok {
        return Table{}, fmt.Errorf("unable to get table attributes: %s.%s", dbase, table)
    }
    for k, v := range item.Fields {
        result.Fields[k] = v
    }
    for k, v := range item.Times {
        result.Times[k] = v
        break
    }

    for _, l := range labs {
        if l.Name == "__table__" || l.Name == "__name__" {
        //    result.Columns = append(result.Columns, fmt.Sprintf("'%s' AS %s", l.Value, l.Name))
            continue
        }

        switch l.Type {
            case labels.MatchEqual:
                result.Conditions = append(result.Conditions, fmt.Sprintf("equals(%s, '%s')", l.Name, l.Value))
            case labels.MatchNotEqual:
                result.Conditions = append(result.Conditions, fmt.Sprintf("notEquals(%s, '%s')", l.Name, l.Value))
            case labels.MatchRegexp:
                result.Conditions = append(result.Conditions, fmt.Sprintf("match(%s, '%s')", l.Name, l.Value))
            case labels.MatchNotRegexp:
                result.Conditions = append(result.Conditions, fmt.Sprintf("NOT match(%s, '%s')", l.Name, l.Value))
        }

        //result.Labels[l.Name] = "String"

        if _, ok := item.Fields[l.Name]; !ok {
            result.Fields[l.Name] = "String"
            result.Columns = append(result.Columns, fmt.Sprintf("'' AS %s", l.Name))
        }
        // else {
        //    result.Columns = append(result.Columns, l.Name)
        //}
    }

    return result, nil
}

func slitName(name string) (string, string, string) {
    dbase, table, value := "", "", ""
    vals := strings.Split(name, ":")
    if len(vals) > 0 { dbase = vals[0] }
    if len(vals) > 1 { table = vals[1] }
    if len(vals) > 2 { value = vals[2] }

    return dbase, table, value
}

func (db *Client) NewSelector(labs []*labels.Matcher) (Selector, error) {
    selector := Selector{
        Tables: map[string]Table{},
    }

    for _, l := range labs {
        if l.Name != "__table__" && l.Name != "__name__" {
            continue
        }

        switch l.Type {
            case labels.MatchEqual:
                dbase, table, value := slitName(l.Value)
                id := fmt.Sprintf("%s:%s", dbase, table)

                result, err := db.GetTable(dbase, table, value, labs)
                if err != nil { return selector, err }
                selector.Tables[id] = result

            case labels.MatchNotEqual:
                dbase, table, value := slitName(l.Value)
                id := fmt.Sprintf("%s:%s", dbase, table)

                for _, key := range db.cache.Keys() {
                    if key != id {
                        result, err := db.GetTable(dbase, table, value, labs)
                        if err != nil { return selector, err }
                        selector.Tables[key] = result
                    }
                }

            case labels.MatchRegexp, labels.MatchNotRegexp:
                keys := []string{}
                if l.Name == "__table__" { keys = db.cache.Keys() }
                if l.Name == "__name__" { keys = db.cache.Values() }
                log.Printf("[values] %v", keys)

                for _, key := range keys {
                    matched, err := regexp.MatchString(l.Value, key)
                    if err != nil {
                        return selector, err
                    }

                    dbase, table, value := slitName(key)
                    id := fmt.Sprintf("%s:%s", dbase, table)

                    if l.Type == labels.MatchRegexp && matched {
                        result, err := db.GetTable(dbase, table, value, labs)
                        if err != nil { return selector, err }
                        selector.Tables[id] = result
                    }
                    if l.Type == labels.MatchNotRegexp && !matched {
                        result, err := db.GetTable(dbase, table, value, labs)
                        if err != nil { return selector, err }
                        selector.Tables[id] = result
                    }
                }
        }
    }

    return selector, nil
}

func (db *Client) LabelValues(name string, start, end time.Time) ([]string, error) {
    var results []string

    labs := make(map[string]bool)
 
    if name == "" {

        for _, val := range db.cache.Fields() {
            v := strings.Split(val, ":")
            if len(v) != 3 { continue }
            labs[v[2]] = true
        }
        for key, _ := range labs {
            results = append(results, key)
        }

    } else if name == "__table__" {

        results = db.cache.Keys()

    } else if name == "__name__" {

        results = db.cache.Values()

    } else {
        
        for _, val := range db.cache.Values() {
            v := strings.Split(val, ":")
            if len(v) != 3 { continue }
            if name != v[2] { continue }

            item, ok := db.cache.Get(fmt.Sprintf("%s:%s", v[0], v[1]))
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

    //log.Printf("[labels] %v", results)
    //results = append(results, "__table__")

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

        selector, err := db.NewSelector(sel)
        if err != nil {
            return series, err
        }

        for _, table := range selector.Tables {

            name := fmt.Sprintf("'%s:%s' AS __table__", table.DBase, table.Table)
            if table.Value != "" {
                name = fmt.Sprintf("'%s:%s:%s' AS __name__", table.DBase, table.Table, table.Value)
            }

            timestamp := mapKeys(table.Times)[0]
            table.Conditions = append(table.Conditions, fmt.Sprintf("`%s` BETWEEN toDateTime(@start,'UTC') AND toDateTime(@end,'UTC')", timestamp))

            columns := []string{name}
            groupBy := []string{}
            for k, v := range table.Fields{
                if k == timestamp {
                    continue
                }
                if !inArray(v, []string{".*Int.*",".*Float.*"}) {
                    columns = append(columns, k)
                    groupBy = append(groupBy, k)
                }
            }

            if len(groupBy) == 0 {
                continue
            }
    
            chCtx := ch.Context(context.Background(), ch.WithParameters(ch.Parameters{}))

            query := fmt.Sprintf(
                "SELECT %s FROM `%s`.`%s` WHERE %s GROUP BY %s LIMIT 100", 
                strings.Join(columns, ", "),
                table.DBase, 
                table.Table, 
                strings.Join(table.Conditions, " AND "),
                strings.Join(groupBy, ", "),
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
                log.Printf("[error] read table %s: %v", table.Table, err)
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

            /*
            tb := db.GetTable(fmt.Sprintf("%s:%s", table.DBase, table.Table))

            if len(tb.LabelTypes) == 0 {
                tb.LabelTypes = []string{"String"}
            }
            */

            for rows.Next() {
                if err := rows.Scan(vars...); err != nil {
                    return series, err
                }

                labels := make(map[string]string)
                //labels["__name__"] = selector.Name
                
                for i, val := range vars {
                    ctype := columnTypes[i]
                    //if inArray(ctype.DatabaseTypeName(), tb.LabelTypes) && inArray(ctype.Name(), tb.LabelNames) {
                    //    labels[ctype.Name()] = reflect.ValueOf(val).Elem().String()
                    //}
                    labels[ctype.Name()] = reflect.ValueOf(val).Elem().String()
                }

                series = append(series, labels)
            }
        }

        /*
        if len(series) == 0 {
            series = append(series, map[string]string{"__name__": selector.Name})
        }
        */
    }

    return series, nil
}

func (db *Client) AggregateExpr(rtp, name string, expr parser.Node, grps []string, gr bool) (string, []string, bool, error) {
    sel, grps, gr, err := db.QueryWalk(rtp, expr, grps, gr)
    if err != nil {
        return "", grps, gr, err
    }

    groupBy := strings.Join(append(grps, "timestamp"), ", ")

    switch name {
        case "sum":
            return fmt.Sprintf("SELECT sum(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, groupBy), grps, gr, nil
        case "count":
            return fmt.Sprintf("SELECT count(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, groupBy), grps, gr, nil
        case "min":
            return fmt.Sprintf("SELECT min(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, groupBy), grps, gr, nil
        case "max":
            return fmt.Sprintf("SELECT max(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, groupBy), grps, gr, nil
        case "avg":
            return fmt.Sprintf("SELECT avg(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, groupBy), grps, gr, nil
    }

    return "", grps, gr, fmt.Errorf("operator not supported by driver: ", name)
}

func (db *Client) BinaryExpr(rtp, name string, LHS, RHS parser.Node, grps []string, gr bool) (string, []string, bool, error) {
    selL, grps, gr, err := db.QueryWalk(rtp, LHS, grps, gr)
    if err != nil {
        return "", grps, gr, err
    }

    selR, grps, gr, err := db.QueryWalk(rtp, RHS, grps, gr)
    if err != nil {
        return "", grps, gr, err
    }

    switch name {
        case "+":
            return fmt.Sprintf("SELECT (s1.value + s2.value) AS value, timestamp, __name__ FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), grps, gr, nil
        case "-":
            return fmt.Sprintf("SELECT (s1.value - s2.value) AS value, timestamp, __name__ FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), grps, gr, nil
        case "/":
            return fmt.Sprintf("SELECT (s1.value / s2.value) AS value, timestamp, __name__ FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), grps, gr, nil
        case "*":
            return fmt.Sprintf("SELECT (s1.value * s2.value) AS value, timestamp, __name__ FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), grps, gr, nil
    }

    return "", grps, gr, nil
}

func (db *Client) VectorSelector(rtp string, labs []*labels.Matcher, grps []string, gr bool) (string, []string, bool, error) {
    selector, err := db.NewSelector(labs)
    if err != nil {
        return "", grps, gr, err
    }

    for _, table := range selector.Tables {
        groupBy := []string{"timestamp"}
        timestamp := mapKeys(table.Times)[0]
        table.Conditions = append(table.Conditions, fmt.Sprintf("`%s` BETWEEN toDateTime(@start,'UTC') AND toDateTime(@end,'UTC')", timestamp))

        if rtp == "streams" {
            if gr == false {
                for key, _ := range table.Fields {
                    if key == timestamp {
                        continue
                    }
                    grps = append(grps, key)
                }
            }
            table.Columns = append(table.Columns, fmt.Sprintf("`%s` AS timestamp", timestamp))
        } else {
            if gr == false {
                for key, val := range table.Fields {
                    if key == timestamp {
                        continue
                    }
                    if inArray(val, []string{".*Int.*",".*Float.*"}) {
                        continue
                    }
                    grps = append(grps, key)
                }
            }
            table.Columns = append(table.Columns, fmt.Sprintf("toStartOfInterval(`%s`, toIntervalSecond(@step)) AS timestamp", timestamp))
        }
        
        if _, ok := table.Fields[table.Value]; ok {
            table.Columns = append(table.Columns, fmt.Sprintf("'%s:%s:%s' AS __name__", table.DBase, table.Table, table.Value))
            //groupBy = append(groupBy, "__name__")
            table.Columns = append(table.Columns, fmt.Sprintf("`%s` AS value", table.Value))
            groupBy = append(groupBy, table.Value)
        } else {
            table.Columns = append(table.Columns, fmt.Sprintf("'%s:%s' AS __table__", table.DBase, table.Table))
            //groupBy = append(groupBy, "__table__")
        }

        query := fmt.Sprintf(
            "SELECT %s FROM `%s`.`%s` WHERE %s GROUP BY %s ORDER BY timestamp",
            strings.Join(append(table.Columns, grps...), ", "),
            table.DBase,
            table.Table,
            strings.Join(table.Conditions, " AND "),
            strings.Join(append(groupBy, grps...), ", "),
        )

        return query, grps, gr, nil
    }

    return "", grps, gr, nil
}

func (db *Client) Call(rtp, name string, args parser.Expressions, grps []string, gr bool) (string, []string, bool, error) {
    //switch name {
    //    default:
    //        return "", nil, fmt.Errorf("unsupported function: %v", name)
    //}

    //for _, arg := range args {
    //    return db.QueryWalk(rtp, arg, grps)
    //}

    return "", grps, gr, fmt.Errorf("unsupported function: %v", name)
}

func (db *Client) MatrixSelector(rtp, name string, node parser.Node, grps []string, gr bool) (string, []string, bool, error) {
    log.Printf("[matrix] %v", name)

    return db.QueryWalk(rtp, node, grps, gr)
}

func (db *Client) QueryWalk(rtp string, node parser.Node, grps []string, gr bool) (string, []string, bool, error) {
    switch n := node.(type) {
        case *parser.BinaryExpr:
            return db.BinaryExpr(rtp, n.Op.String(), n.LHS, n.RHS, grps, gr)
        case *parser.AggregateExpr:
            return db.AggregateExpr(rtp, n.Op.String(), n.Expr, n.Grouping, true)
        case *parser.VectorSelector:
            return db.VectorSelector(rtp, n.LabelMatchers, grps, gr)
        case *parser.Call:
            return db.Call(rtp, n.Func.Name, n.Args, grps, gr)
        case *parser.MatrixSelector:
            return db.MatrixSelector(rtp, n.String(), n.VectorSelector, grps, gr)
        //case parser.Expressions:
        //    fmt.Printf("Expressions: %v\n", n)
        //case *parser.NumberLiteral:
        //    fmt.Printf("Literal: %v\n", n)
        //    fmt.Printf("Pos: %v\n", n.PosRange)
        default:
            return "", grps, gr, fmt.Errorf("unknown node type: %T\n", n)
    }

    return "", grps, gr, nil
}

func (db *Client) QuerySelector(rtp, sel string, grps []string, gr bool, start, end time.Time, step time.Duration) (config.ResultType, error) {
    //"matrix" | "streams"
    result := config.ResultType{
        ResultType: rtp,
        Result: []config.Result{},
    }
    resMap := make(map[string]config.Result)

    if gr == true {
        result.ResultType = "matrix"
    }

    if result.ResultType == "matrix" {
        sel = fmt.Sprintf("%s WITH FILL STEP toIntervalSecond(@step) INTERPOLATE (%s)", sel, strings.Join(grps, ", "))
    }

    log.Printf("[query] %v", sel)

    chCtx := ch.Context(context.Background(), ch.WithParameters(ch.Parameters{}))

    rows, err := db.client.QueryContext(
        chCtx, 
        sel,
        sql.Named("start", start.Format("2006-01-02 15:04:05")),
        sql.Named("end", end.Format("2006-01-02 15:04:05")),
        sql.Named("step", step.Seconds()),
    )
    log.Printf("start %v", start.Format("2006-01-02 15:04:05"))
    log.Printf("end %v", end.Format("2006-01-02 15:04:05"))
    if err != nil {
        log.Printf("[error] %v", err)
        return result, err
    }
    defer rows.Close()

    columnTypes, err := rows.ColumnTypes()
    if err != nil {
        return result, err
    }

    vars := make([]interface{}, len(columnTypes))
    for i := range columnTypes {
        vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
    }

    for rows.Next() {
        if err := rows.Scan(vars...); err != nil {
            return result, err
        }

        metric := make(map[string]string)
        value := make(map[string]string)
        value["value"] = ""
        timestamp := ""
        mtTimestamp := float64(0)
        row := []interface{}{}
        
        for i, val := range vars {
            ctype := columnTypes[i]

            if ctype.Name() == "timestamp" {
                if reflect.TypeOf(val).String() == "*time.Time" {
                    ts := reflect.ValueOf(val).Interface().(*time.Time)
                    timestamp = fmt.Sprintf("%v", ts.UnixNano())
                    ss, _ := ts.MarshalText()
                    value["time"] = string(ss)
                    mtTimestamp = float64(ts.Unix())
                }
                continue
            }

            if !inArray(ctype.Name(), []string{"value","log","level"}) {
                metric[ctype.Name()] = reflect.ValueOf(val).Elem().String()
            }

            value[ctype.Name()] = fmt.Sprintf("%v", reflect.ValueOf(val).Elem())
        }

        // Проблема при очень низком @step в toStartOfInterval
        if val, ok := value["__name__"]; ok {
            if val == "" { continue }
        }
        if val, ok := value["__table__"]; ok {
            if val == "" { continue }
        }

        if result.ResultType == "streams" {
            text, err := json.Marshal(value)
            if err != nil {
                return result, err
            }
            row = []interface{}{timestamp, string(text)}
        } else {
            row = []interface{}{mtTimestamp, fmt.Sprintf("%v", string(value["value"]))}
        }

        text, err := json.Marshal(metric)
        if err != nil {
            return result, err
        }
        hash := getHash(string(text))

        if res, ok := resMap[hash]; ok {
            res.Values = append(res.Values, row)
            resMap[hash] = res
        } else {
            if result.ResultType == "matrix" {
                resMap[hash] = config.Result{
                    Metric: metric,
                    Values: []interface{}{row},
                }
            }
            if result.ResultType == "streams" {
                resMap[hash] = config.Result{
                    Stream: metric,
                    Values: []interface{}{row},
                }
            }
        }
    }

    keys := make([]string, 0, len(resMap))
    for k := range resMap{
        keys = append(keys, k)
    }
    sort.Strings(keys)

    for _, k := range keys {
        result.Result = append(result.Result, resMap[k])
    }
  
    return result, nil
}

func (db *Client) Query(query string, start time.Time, timeout time.Duration, limit int) (config.ResultType, error) {
    //"matrix" | "vector" | "scalar" | "string"
    result := config.ResultType{
        ResultType: "vector",
        Result: []config.Result{},
    }

    return result, nil
}

func (db *Client) QueryRange(query string, start, end time.Time, step time.Duration, limit int) (config.ResultType, error) {
    if db.debug {
        log.Printf("[range] %s", query)
        log.Printf("[range] step - %v", step.Seconds())
    }

    expr, err := parser.ParseExpr(query)
    if err != nil {
        return config.ResultType{}, fmt.Errorf("promql %v", err)
    }

    sel, grps, gr, err := db.QueryWalk("", expr, nil, false)
    if err != nil {
        return config.ResultType{}, err
    }

    result, err := db.QuerySelector("matrix", sel, grps, gr, start, end, step)
    if err != nil {
        return config.ResultType{}, err
    }
  
    return result, nil
}

func (db *Client) LokiQuery(query string, start time.Time, timeout time.Duration, limit int) (config.ResultType, error) {
    //"matrix" | "vector" | "scalar" | "string"
    result := config.ResultType{
        ResultType: "vector",
        Result: []config.Result{},
    }

    return result, nil
}

func (db *Client) LokiQueryRange(query string, start, end time.Time, step time.Duration, limit int) (config.ResultType, error) {
    if db.debug {
        log.Printf("[range] %s", query)
        log.Printf("[range] step - %v", step.Seconds())
    }

    expr, err := parser.ParseExpr(query)
    if err != nil {
        return config.ResultType{}, fmt.Errorf("promql %v", err)
    }

    sel, grps, gr, err := db.QueryWalk("streams", expr, nil, false)
    if err != nil {
        return config.ResultType{}, err
    }

    result, err := db.QuerySelector("streams", sel, grps, gr, start, end, step)
    if err != nil {
        return config.ResultType{}, err
    }

    return result, nil
}