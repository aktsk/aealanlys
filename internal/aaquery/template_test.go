package aaquery_test

import (
	"testing"

	"github.com/aktsk/aealanlys/internal/aaquery"
)

func TestGenerate(t *testing.T) {
	tests := []struct {
		name             string
		groupingFuncName string
		table            string
		whereExp         string
		expected         string
		wantErr          bool
	}{
		{
			name:             "simple",
			groupingFuncName: "CONCAT",
			table:            "table_19700101",
			whereExp:         "",
			expected:         "CREATE TEMPORARY FUNCTION FMT_LATE(s FLOAT64) AS (s*1000); SELECT CONCAT(protoPayload.method, protoPayload.resource) AS methodName, COUNT(protoPayload.status) AS count, COUNTIF(protoPayload.status < 300) AS count_2xx, COUNTIF(299 < protoPayload.status AND protoPayload.status < 400) AS count_3xx, COUNTIF(399 < protoPayload.status AND protoPayload.status < 500) AS count_4xx, COUNTIF(499 < protoPayload.status) AS count_5xx, FMT_LATE(MIN(protoPayload.latency)) AS late0pctl, FMT_LATE(APPROX_QUANTILES(protoPayload.latency, 100)[OFFSET(50)]) AS late50pctl, FMT_LATE(APPROX_QUANTILES(protoPayload.latency, 100)[OFFSET(95)]) AS late95pctl, FMT_LATE(APPROX_QUANTILES(protoPayload.latency, 100)[OFFSET(99)]) AS late99pctl, FMT_LATE(MAX(protoPayload.latency)) AS late100pctl, MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts, FROM `table_19700101` GROUP BY methodName ORDER BY count DESC;",
			wantErr:          false,
		},
		{
			name:             "with where",
			groupingFuncName: "CONCAT",
			table:            "table_*",
			whereExp:         `_TABLE_SUFFIX BETWEEN '20241207' AND '20250107'`,
			expected:         "CREATE TEMPORARY FUNCTION FMT_LATE(s FLOAT64) AS (s*1000); SELECT CONCAT(protoPayload.method, protoPayload.resource) AS methodName, COUNT(protoPayload.status) AS count, COUNTIF(protoPayload.status < 300) AS count_2xx, COUNTIF(299 < protoPayload.status AND protoPayload.status < 400) AS count_3xx, COUNTIF(399 < protoPayload.status AND protoPayload.status < 500) AS count_4xx, COUNTIF(499 < protoPayload.status) AS count_5xx, FMT_LATE(MIN(protoPayload.latency)) AS late0pctl, FMT_LATE(APPROX_QUANTILES(protoPayload.latency, 100)[OFFSET(50)]) AS late50pctl, FMT_LATE(APPROX_QUANTILES(protoPayload.latency, 100)[OFFSET(95)]) AS late95pctl, FMT_LATE(APPROX_QUANTILES(protoPayload.latency, 100)[OFFSET(99)]) AS late99pctl, FMT_LATE(MAX(protoPayload.latency)) AS late100pctl, MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts, FROM `table_*` WHERE _TABLE_SUFFIX BETWEEN '20241207' AND '20250107' GROUP BY methodName ORDER BY count DESC;",
			wantErr:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := aaquery.Generate(tt.groupingFuncName, tt.table, tt.whereExp)

			if (err != nil) != tt.wantErr {
				t.Errorf("Generate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result != tt.expected {
				t.Errorf("Generate() = '%v'; want '%v'", result, tt.expected)
			}
		})
	}
}
