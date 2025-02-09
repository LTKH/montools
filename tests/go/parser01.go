package main

import (
    "fmt"
    "log"

    //"github.com/prometheus/prometheus/promql"
    "github.com/prometheus/prometheus/promql/parser"

    //"github.com/prometheus/prometheus/promql"
    //"github.com/prometheus/prometheus/parser"
)

type PromQuery struct {
    BinaryExpr   string
    AggrExpr     []string
    CallFunc     string
    MatrixSel    string
    VectorSel    string
    Field        string
    PromQuery    []PromQuery
}

type PromQLQuery struct {
    Metric string
    Labels map[string]string
    Function string
    Value float64
}

/*
// MyVisitor реализует интерфейс parser.Visitor
type MyVisitor struct{}

func (v *MyVisitor) Visit(node parser.Node) parser.Node {
    // Здесь вы можете обрабатывать узлы
    switch n := node.(type) {
    case *parser.Call:
        fmt.Printf("Function: %s\n", n.Func.Name)
    case *parser.MatrixSelector:
        fmt.Printf("Matrix: %s\n", n.Name)
    case *parser.VectorSelector:
        fmt.Printf("Vector: %s\n", n.Name)
    }
    // Возвращаем узел для дальнейшего обхода
    return node
}
*/

func Walk(query PromQuery, node parser.Node) (parser.Node) {
    fmt.Printf("Walk: %s\n", "-------------------------------")
    switch n := node.(type) {
	    case *parser.BinaryExpr:
            fmt.Printf("Binary Expression: %s\n", n.Op)
			Walk(query, n.LHS)
			Walk(query, n.RHS)
		case *parser.AggregateExpr:
			fmt.Printf("Aggregate Expression: %s\n", n.Op)
            fmt.Printf("Grouping - %v\n", n.Grouping)
			Walk(query, n.Expr)
		case *parser.Call:
			fmt.Printf("Function: %s\n", n.Func.Name)
			for _, arg := range n.Args {
                Walk(query, arg)
            }
		case *parser.MatrixSelector:
			fmt.Printf("Matrix: %s\n", n.String())
			Walk(query, n.VectorSelector)
		case *parser.VectorSelector:
			fmt.Printf("Vector: %s\n", n.String())
            fmt.Printf("label - %v\n", n.LabelMatchers)
        default:
            fmt.Printf("Unknown node type: %T\n", n)
    }

    return node
}


func main() {
    // Пример PromQL запроса
    query := `count(sum(cpu_usage_active1{test="test1"}) by (test))`
	//query := `rate(cpu_usage_active3[5m])`

    // Парсим запрос
    expr, err := parser.ParseExpr(query)
    if err != nil {
        log.Fatalf("Ошибка при парсинге запроса: %v", err)
    }

    // Создаем свой визитор
    //visitor := &MyVisitor{}

    // Используем Walk для обхода дерева выражений
    //parser.Walk(visitor, expr)

	promQuery := PromQuery{}

	Walk(promQuery, expr)
}

func parsePromQL(query string) (PromQLQuery, error) {
    // Парсинг запроса
    node, err := parser.ParseExpr(query)
    if err != nil {
        return PromQLQuery{}, err
    }

    result := PromQLQuery{}
    
    // Обработка различных типов узлов
    switch n := node.(type) {
    case *parser.VectorSelector:
        result.Metric = n.Name
        //result.Labels = n.Labels
        fmt.Printf("label - %v\n", n.LabelMatchers)

    case *parser.AggregateExpr:
        //result.Function = n.Func.Name
        fmt.Printf("param - %v\n", n.Param)
        // можно добавить обработку агрегационных метрик здесь

    // Дополнительные случаи можно добавить для других типов узлов
    }

    return result, nil
}

/*
func main() {
    query := `sum(rate(http_requests_total[5m])) by (method)`
    parsedQuery, err := parsePromQL(query)
    if err != nil {
        fmt.Println("Ошибка парсинга:", err)
        return
    }
    fmt.Printf("Парсированный запрос: %+v\n", parsedQuery)
}
*/