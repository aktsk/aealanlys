# appengine-access-log-analyzer (`aealanlys`)

App Engine Access Log ([`google.appengine.logging.v1.RequestLog`](https://cloud.google.com/logging/docs/reference/v2/rpc/google.appengine.logging.v1#google.appengine.logging.v1.RequestLog)) Analyzer

BigQuery に sink されているアクセスログを用いてルート毎のメトリクスを確認できるようにします。
BQ上でルート毎にアクセスログをまとめるには RegExp を用いています。[bqmatcher](./bqmatcher) を参照してください。

詳細はクエリ ([internal/aaquery/template.go](./internal/aaquery/template.go)) を参照するとわかりやすいです。

## TODO
- 複数のテーブルを指定することができず、主に1日単位でしかログを集計できません
    + デフォルトの sink 設定ではテーブル1つは1日のデータを意味します
    + 必要であればワイルドカードテーブルを使用し `_TABLE_SUFFIX` を用いて絞るようにしてください
- テーブルより小さい時間範囲でログを集計することができません
- GAE App の Version でログを絞ることができません
