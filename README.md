# appengine-access-log-analyzer (`aealanlys`)

App Engine Access Log ([`google.appengine.logging.v1.RequestLog`](https://cloud.google.com/logging/docs/reference/v2/rpc/google.appengine.logging.v1#google.appengine.logging.v1.RequestLog)) Analyzer

BigQuery に sink されているアクセスログを用いてルート毎のメトリクスを確認できるようにします。
BQ上でルート毎にアクセスログをまとめるには RegExp を用いています。[bqmatcher](./bqmatcher) を参照してください。

詳細はクエリ ([internal/aaquery/template.go](./internal/aaquery/template.go)) を参照するとわかりやすいです。
