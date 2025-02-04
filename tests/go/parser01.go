package main

import (
    "fmt"
    "log"

    //"github.com/prometheus/prometheus/promql"
    "github.com/prometheus/prometheus/promql/parser"
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
        default:
            fmt.Printf("Unknown node type: %T\n", n)
    }

    return node
}

func main() {
    // Пример PromQL запроса
    query := `sum(sum(cpu_usage_active1{test="test1"}) + count(avg(cpu_usage_active2)) * count(max(cpu_usage_active3)))`
	//query := `rate(cpu_usage_active3[5m])`

    // Парсим запрос
    expr, err := parser.ParseExpr(query)
    if err != nil {
        log.Fatalf("Ошибка при парсинге запроса: %v", err)
    }

	/*
    // Создаем свой визитор
    visitor := &MyVisitor{}

    // Используем Walk для обхода дерева выражений
    parser.Walk(visitor, expr)
	*/

	promQuery := PromQuery{}

	Walk(promQuery, expr)
}