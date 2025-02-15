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
    Labels map[string]string
    Values map[string]string
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

func (c *Cache) Set(key string, labels, values, times map[string]string) error {
    c.Lock()
    defer c.Unlock()

    val, found := c.Items[key]
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
    
    c.Items[key] = val

    return nil
}

func (c *Cache) Get(key string) (Item, bool) {
    c.RLock()
    defer c.RUnlock()

    val, found := c.Items[key]
    if !found {
        return Item{}, false
    }

    return val, true
}

func (c *Cache) Keys() []string {
    c.RLock()
    defer c.RUnlock()

    var keys []string

    for key, _ := range c.Items {
        keys = append(keys, key)
    }

    return keys
}

func (c *Cache) Values(name string) []string {
    c.RLock()
    defer c.RUnlock()

    var items []string

    for key, val := range c.Items {
        if len(val.Times) > 0 {
            for v, _ := range val.Values {
                if name != "" && name != v { continue }
                items = append(items, fmt.Sprintf("%s:%s", key, v))
            }
        }
    }

    return items
}

func (c *Cache) Labels() []string {
    c.RLock()
    defer c.RUnlock()

    var items []string

    for key, val := range c.Items {
        if len(val.Times) == 0 {
            continue
        }
        if len(val.Values) == 0 {
            continue
        }
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

        db.cache.Set(id, labels, values, times)
    }

    return nil
}

func (db *Client) GetTable(dbase, table, value string, labs []*labels.Matcher) (Table, error) {
    result := Table{
        DBase: dbase,
        Table: table,
        Value: value,
        Fields: []string{},
        Values: map[string]string{},
        Labels: map[string]string{},
        Times:  map[string]string{},
        Conditions: []string{},
    }

    item, ok := db.cache.Get(fmt.Sprintf("%s:%s", dbase, table))
    if !ok {
        return result, fmt.Errorf("unable to get table attributes: %s.%s", dbase, table)
    }
    for k, v := range item.Values {
        result.Values[k] = v
    }
    for k, v := range item.Times {
        result.Times[k] = v
        break
    }

    for _, l := range labs {
        result.Labels[l.Name] = "String"

        if l.Name == "__table__" || l.Name == "__name__" {
            result.Fields = append(result.Fields, fmt.Sprintf("'%s' AS %s", l.Value, l.Name))
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

        if _, ok := item.Labels[l.Name]; !ok {
            result.Fields = append(result.Fields, fmt.Sprintf("'' AS %s", l.Name))
        } else {
            result.Fields = append(result.Fields, l.Name)
        }
    }

    return result, nil
}

func (db *Client) NewSelector(dbase, table string, labs []*labels.Matcher) (Selector, error) {
    selector := Selector{
        Tables: map[string]Table{},
    }

    for _, l := range labs {
        if l.Name == "__table__" || l.Name == "__name__" {

            vals := strings.Split(l.Value, ":")
            value := ""

            if len(vals) < 2 { continue }
            if len(vals) == 3 { value = vals[2] }
            id := fmt.Sprintf("%s:%s", vals[0], vals[1])

            switch l.Type {
                case labels.MatchEqual:
                    table, err := db.GetTable(vals[0], vals[1], value, labs)
                    if err != nil { return selector, err }
                    selector.Tables[id] = table
                case labels.MatchNotEqual:
                    for _, key := range db.cache.Keys() {
                        if key != id {
                            table, err := db.GetTable(vals[0], vals[1], value, labs)
                            if err != nil { return selector, err }
                            selector.Tables[id] = table
                        }
                    }
                case labels.MatchRegexp:
                    for _, key := range db.cache.Keys() {
                        matched, err := regexp.MatchString(l.Value, key)
                        if err != nil {
                            return selector, err
                        }
                        if matched {
                            table, err := db.GetTable(vals[0], vals[1], value, labs)
                            if err != nil { return selector, err }
                            selector.Tables[id] = table
                        }
                    }
                case labels.MatchNotRegexp:
                    for _, key := range db.cache.Keys() {
                        matched, err := regexp.MatchString(l.Value, key)
                        if err != nil {
                            return selector, err
                        }
                        if !matched {
                            table, err := db.GetTable(vals[0], vals[1], value, labs)
                            if err != nil { return selector, err }
                            selector.Tables[id] = table
                        }
                    }
            }
        }
    }

    /*
    for _, l := range labs {
        if l.Name == "__table__" || l.Name == "__name__" {
            selector.Name = l.Value
            vals := strings.Split(l.Value, ":")
            if len(vals) > 0 { selector.DBase = vals[0] }
            if len(vals) > 1 { selector.Table = vals[1] }
            if len(vals) > 2 { selector.Value = vals[2] }
            selector.Fields = append(selector.Fields, fmt.Sprintf("'%s' AS %s", l.Value, l.Name))
            selector.Labels[l.Name] = "String"
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
            selector.Labels[l.Name] = "String"
        }
    }

    if selector.DBase == "" {
        return selector, fmt.Errorf("failed to determine target data base")
    }
    if selector.Table == "" {
        return selector, fmt.Errorf("failed to determine target table")
    }

    id := fmt.Sprintf("%s:%s", selector.DBase, selector.Table)
    item, ok := db.cache.Get(id)

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
    */

    return selector, nil
}

func (db *Client) LabelValues(name string, start, end time.Time) ([]string, error) {
    var results []string

    labs := make(map[string]bool)
 
    if name == "" {

        for _, val := range db.cache.Labels() {
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
        
    
        /*
        if err := db.SetColumns(db.config.DBaseNames, db.config.TableNames); err != nil {
            return results, err
        }
        */

        results = db.cache.Values("")

    } else {
        
        for _, val := range db.cache.Values(name) {
            v := strings.Split(val, ":")
            if len(v) != 3 { continue }

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

        for _, table := range selector.Tables {

            table.Conditions = append(
                table.Conditions, 
                "1 = 1",
                //fmt.Sprintf("`%s` BETWEEN toDateTime(@start,'UTC') AND toDateTime(@end,'UTC')", mapKeys(table.Times)[0]),
            )
    
            //if table.Name == "" {
            //    return series, nil
            //}
    
            //if len(table.Labels) > 0 {
            //
            //}
    
            chCtx := ch.Context(context.Background(), ch.WithParameters(ch.Parameters{}))

            query := fmt.Sprintf(
                "SELECT %s FROM `%s`.`%s` WHERE %s GROUP BY %s LIMIT 100", 
                strings.Join(table.Fields, ", "),
                table.DBase, 
                table.Table, 
                strings.Join(table.Conditions, " AND "),
                strings.Join(mapKeys(table.Labels), ", "),
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

func (db *Client) AggregateExpr(name string, grps []string, gr bool, expr parser.Node) (string, []string, error) {
    //grouping = append(grouping, "timestamp")

    sel, grps, err := db.QueryWalk(expr, grps, gr)
    if err != nil {
        return "", grps, err
    }

    groupBy := strings.Join(append(grps, "timestamp", "__name__"), ", ")

    switch name {
        case "sum":
            return fmt.Sprintf("SELECT sum(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, groupBy), grps, nil
        case "count":
            return fmt.Sprintf("SELECT count(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, groupBy), grps, nil
        case "min":
            return fmt.Sprintf("SELECT min(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, groupBy), grps, nil
        case "max":
            return fmt.Sprintf("SELECT max(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, groupBy), grps, nil
        case "avg":
            return fmt.Sprintf("SELECT avg(value) AS value, %[2]s FROM (%[1]s) GROUP BY %[2]s ORDER BY timestamp ASC", sel, groupBy), grps, nil
    }

    return "", grps, fmt.Errorf("operator not supported by driver: ", name)
}

func (db *Client) BinaryExpr(name string, grps []string, gr bool, LHS, RHS parser.Node) (string, []string, error) {
    selL, _, err := db.QueryWalk(LHS, grps, gr)
    if err != nil {
        return "", grps, err
    }

    selR, _, err := db.QueryWalk(RHS, grps, gr)
    if err != nil {
        return "", grps, err
    }

    switch name {
        case "+":
            return fmt.Sprintf("SELECT (s1.value + s2.value) AS value, timestamp, __name__ FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), []string{}, nil
        case "-":
            return fmt.Sprintf("SELECT (s1.value - s2.value) AS value, timestamp, __name__ FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), []string{}, nil
        case "/":
            return fmt.Sprintf("SELECT (s1.value / s2.value) AS value, timestamp, __name__ FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), []string{}, nil
        case "*":
            return fmt.Sprintf("SELECT (s1.value * s2.value) AS value, timestamp, __name__ FROM (%s) AS s1, (%s) AS s2 ORDER BY timestamp ASC", selL, selR), []string{}, nil
    }

    return "", []string{}, nil
}

func (db *Client) VectorSelector(labs []*labels.Matcher, grps []string, gr bool) (string, []string, error) {

    selector, err := db.NewSelector("", "", labs)
    if err != nil {
        return "", grps, err
    }

    for _, table := range selector.Tables {

        table.Conditions = append(
            table.Conditions, 
            fmt.Sprintf("`%s` BETWEEN toDateTime(@start,'UTC') AND toDateTime(@end,'UTC')", mapKeys(table.Times)[0]),
        )

        if db.debug {
            log.Printf("[selector] %s", table)
        }

        fields := []string{
            fmt.Sprintf("`%s` AS value", table.Value),
            fmt.Sprintf("'%s:%s:%s' AS __name__", table.DBase, table.Table, table.Value),
            fmt.Sprintf("toStartOfInterval(`%s`, toIntervalSecond(@step)) AS timestamp", mapKeys(table.Times)[0]),
        }

        if _, ok := table.Values[table.Value]; !ok {
            return "", grps, fmt.Errorf("unable to find `%s` field in table: %s.%s", table.Value, table.DBase, table.Table)
        }

        if gr {
            for _, v := range grps {
                if _, ok := table.Labels[v]; !ok {
                    return "", grps, fmt.Errorf("unable to find `%s` field in table: %s.%s", v, table.DBase, table.Table)
                }
            }
        } else {
            grps = []string{}
            for k, _ := range table.Labels {
                grps = append(grps, k)
            }
        }

        query := fmt.Sprintf(
            "SELECT %s FROM `%s`.`%s` WHERE %s GROUP BY %s ORDER BY timestamp",
            strings.Join(append(fields, grps...), ", "),
            table.DBase,
            table.Table,
            strings.Join(table.Conditions, " AND "),
            strings.Join(append(grps, "value", "timestamp", "__name__"), ", "),
        )

        return query, grps, nil
    }

    return "", []string{}, nil
}

func (db *Client) QueryWalk(node parser.Node, grps []string, gr bool) (string, []string, error) {
    switch n := node.(type) {
        case *parser.BinaryExpr:
            return db.BinaryExpr(n.Op.String(), grps, gr, n.LHS, n.RHS)
        case *parser.AggregateExpr:
            return db.AggregateExpr(n.Op.String(), n.Grouping, true, n.Expr)
        case *parser.VectorSelector:
            return db.VectorSelector(n.LabelMatchers, grps, gr)
        case *parser.Call:
            return "", grps, fmt.Errorf("unsupported function: %v", n.Func.Name)
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
            return "", grps, fmt.Errorf("unknown node type: %T\n", n)
    }

    return "", grps, nil
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

    sel, grps, err := db.QueryWalk(expr, []string{}, false)
    if err != nil {
        return results, err
    }

    sel = fmt.Sprintf("%s WITH FILL STEP toIntervalSecond(@step) INTERPOLATE (%s)", sel, strings.Join(append(grps, "value"), ", "))

    if db.debug {
        log.Printf("[range] %s", query)
        log.Printf("[range] %s", sel)
    }

    chCtx := ch.Context(context.Background(), ch.WithParameters(ch.Parameters{}))

    if step.Seconds() < 10 {
        step = time.Duration(10) * time.Second
    }

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