package aaquery

import (
	"bytes"
	"regexp"
	"strings"
	"text/template"
)

var tpl = template.Must(template.New("bquery").Parse(regexp.MustCompile(`\s+`).ReplaceAllString(`
CREATE TEMPORARY FUNCTION FMT_LATE(s FLOAT64) AS (s*1000);
SELECT
	{{.GroupingFunc}}(protoPayload.method, protoPayload.resource) AS methodName,
	COUNT(protoPayload.status) AS count,
	COUNTIF(protoPayload.status < 300) AS count_2xx,
	COUNTIF(299 < protoPayload.status AND protoPayload.status < 400) AS count_3xx,
	COUNTIF(399 < protoPayload.status) AS count_failure,
	FMT_LATE(MIN(protoPayload.latency)) AS late0pctl,
	FMT_LATE(APPROX_QUANTILES(protoPayload.latency, 100)[OFFSET(50)]) AS late50pctl,
	FMT_LATE(APPROX_QUANTILES(protoPayload.latency, 100)[OFFSET(95)]) AS late95pctl,
	FMT_LATE(APPROX_QUANTILES(protoPayload.latency, 100)[OFFSET(99)]) AS late99pctl,
	FMT_LATE(MAX(protoPayload.latency)) AS late100pctl,
	MIN(timestamp) AS min_ts,
	MAX(timestamp) AS max_ts,
FROM `+"`"+`{{.Table}}`+"`"+`
GROUP BY
	methodName
ORDER BY
	count DESC;
`, " ")))

type templateParameter struct {
	Table        string
	GroupingFunc string
}

// Generate テンプレートからクエリを生成する
//
// 使用する groupingFunc は aealanlys.PathGrouping.ToPathToGroupingKeyBQUDF のインターフェースを想定している。
// TODO: ログを sink する際に logName による絞り込みをしているかどうかを受けつけ、必要なら WHERE を追加するようにする
func Generate(groupingFuncName, table string) (string, error) {
	var b bytes.Buffer
	if err := tpl.Execute(&b, templateParameter{
		Table:        table,
		GroupingFunc: groupingFuncName,
	}); err != nil {
		return "", err
	}
	return strings.TrimSpace(b.String()), nil
}
