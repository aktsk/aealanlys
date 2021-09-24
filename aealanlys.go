package aealanlys

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/aktsk/appengine-access-log-analyzer/internal/aaquery"
	"google.golang.org/api/iterator"
)

type PathGrouping interface {
	// PathToGroupingKeyBQUDF (method, path) を引数とし、なにかしらのグルーピング処理を通した文字列にする BigQuery の UDF 定義を返す
	//
	// method は大文字の表記を、path は path + query を想定しなければならない。
	// 実装例は bqmatcher package を参照
	PathToGroupingKeyBQUDF(funcname string) string
}

type PlanResult struct {
	Query               string
	TotalBytesProcessed int64
}

func constructBQQuery(table string, pathGroupBy PathGrouping) (string, error) {
	funcname := "PTN"
	bqq := pathGroupBy.PathToGroupingKeyBQUDF(funcname)
	{
		body, err := aaquery.Generate(funcname, table)
		if err != nil {
			return "", err
		}
		bqq += body
	}
	return bqq, nil
}

// AnalyzeDryrun BQに問い合わせを行い実行計画を返却する
func AnalyzeDryrun(ctx context.Context, client *bigquery.Client, table string, pathGroupBy PathGrouping) (*bigquery.JobStatistics, error) {
	q, err := constructBQQuery(table, pathGroupBy)
	if err != nil {
		return nil, fmt.Errorf("failed to construct query: %w", err)
	}
	bqq := client.Query(q)
	bqq.DryRun = true

	job, err := bqq.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run query(%s): %w", q, err)
	}
	// > Dry run is not asynchronous, so get the latest status and statistics.
	// https://cloud.google.com/bigquery/docs/dry-run-queries
	status := job.LastStatus()
	if err := status.Err(); err != nil {
		return nil, fmt.Errorf("failed to run query (%s): %w", q, err)
	}

	return status.Statistics, nil
}

func readRow(schema bigquery.Schema, values []bigquery.Value) ([]string, error) {
	if len(values) != len(schema) {
		return nil, fmt.Errorf("column length was unexpected (actual: %d, expected: %d)", len(values), len(schema))
	}
	var row []string
	for i, bqv := range values {
		fs := schema[i]
		// bigquery ライブラリの変換規則は RowIterator.Next の godoc を参照すること
		// https://pkg.go.dev/cloud.google.com/go/bigquery#RowIterator.Next
		switch fs.Type {
		case bigquery.FloatFieldType:
			row = append(row, fmt.Sprintf("%.2f", bqv))
		case bigquery.IntegerFieldType:
			row = append(row, fmt.Sprintf("%d", bqv))
		case bigquery.StringFieldType:
			row = append(row, bqv.(string))
		case bigquery.TimestampFieldType:
			row = append(row, bqv.(time.Time).Format(time.RFC3339))
		default:
			if sg, ok := (bqv).(fmt.Stringer); ok {
				row = append(row, sg.String())
				continue
			}
			return nil, fmt.Errorf("failed to convert field(%s, type=%s, value=%v) to string: missing conversion rule to string", fs.Name, fs.Type, bqv)
		}
	}
	return row, nil
}

// Analyze BQに問い合わせを行い appengine のログを解析した結果を CSV として writer に書く
func Analyze(ctx context.Context, client *bigquery.Client, table string, pathGroupBy PathGrouping, csvw io.Writer) error {
	q, err := constructBQQuery(table, pathGroupBy)
	if err != nil {
		return fmt.Errorf("failed to construct query: %w", err)
	}
	bqq := client.Query(q)

	it, err := bqq.Read(ctx)
	if err != nil {
		return fmt.Errorf("failed to read result: %w", err)
	}

	w := csv.NewWriter(csvw)
	var headerWritten bool
	for {
		var values []bigquery.Value
		if err := it.Next(&values); err != nil {
			if err == iterator.Done {
				break
			}
			return fmt.Errorf("failed to read result: %w", err)
		}
		if len(values) != len(it.Schema) {
			return fmt.Errorf("failed to read result: cell length was unexpected (actual: %d, expected: %d)", len(values), len(it.Schema))
		}

		if !headerWritten {
			// it.Schema は Next を呼ぶまで空なので、中でやる
			// > // The schema of the table. Available after the first call to Next.
			var header []string
			for _, fs := range it.Schema {
				header = append(header, fs.Name)
			}
			if err := w.Write(header); err != nil {
				return fmt.Errorf("failed to write as csv: %w", err)
			}
			headerWritten = true
		}

		row, err := readRow(it.Schema, values)
		if err != nil {
			return fmt.Errorf("failed to read result: %w", err)
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("failed to write as csv: %w", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return fmt.Errorf("failed to write as csv: %w", err)
	}
	return nil
}
