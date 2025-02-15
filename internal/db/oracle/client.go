package oracle

import ( 
    "fmt"
    "time"
    //"net/url"
    "database/sql"
    _ "github.com/sijms/go-ora/v2"
    "github.com/ltkh/montools/internal/config/mtprom"
)

type Client struct {
    client *sql.DB
    config *config.Source
    debug bool
}

func NewClient(conf *config.Source, debug bool) (*Client, error) {
    conn, err := sql.Open("oracle", fmt.Sprintf(
        "oracle://%s:%s@%s",
        conf.Username,
        conf.Password,
        conf.Addr[0],
    ))
    if err != nil {
        return &Client{}, err
    }
    err = conn.Ping()
    if err != nil {
        return &Client{}, err
    }

    db := &Client{ client: conn, config: conf, debug: debug }

    return db, nil
}

func (db *Client) Close() error {
    db.client.Close()

    return nil
}

func (db *Client) LabelValues(name string, start, end time.Time) ([]string, error) {
    var results []string

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

    return series, nil
}

func (db *Client) Query(query string, limit int, start time.Time, timeout time.Duration) ([]config.Result, error) {
    var results []config.Result

    return results, nil
}

func (db *Client) QueryRange(query string, limit int, start, end time.Time, step time.Duration) ([]config.Result, error) {
    var results []config.Result
  
    return results, nil
}