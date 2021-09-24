package bqmatcher

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unsafe"

	aealanlys "github.com/aktsk/appengine-access-log-analyzer"
	"github.com/gorilla/mux"
)

// gorilla.Mux 互換のパスパターンを指定する
type GorillaMuxRoute struct {
	Name    string
	Method  string
	Pattern string
}

type matchPair struct {
	Name string
	// "{Method} {PathRegexp}" のように結合された文字列を入力とする RegExp
	RE string
}

// Matcher aealanlys.PathGrouping
type Matcher struct {
	pairs []matchPair
}

var _ aealanlys.PathGrouping = &Matcher{}

// PathToGroupingKeyBQUDF (aealanlys.PathGrouping).PathToGroupingKeyBQUDF
//
// パスからルート名に変換する BigQuery のユーザー定義関数の定義を生成する。
// このUDFはなにも引っかからなければ "{method} {path}" という文字列を返す
func (m *Matcher) PathToGroupingKeyBQUDF(funcname string) string {
	// TODO: BigQueryに処理させて動作を検証する。SELECT funcname("GET", "/") とすればテストできる
	// note: 現状40パスほどで問題なく実行できるが、いつか quota にひっかかるときがくるかもしれない
	// https://cloud.google.com/bigquery/quotas#udf_limits

	rrShorthand := "RR"
	utility := fmt.Sprintf("CREATE TEMPORARY FUNCTION %s(v STRING,rx STRING,re STRING) AS (REGEXP_REPLACE(v,rx,re));", rrShorthand)

	header := fmt.Sprintf("CREATE TEMPORARY FUNCTION %s(m STRING,p STRING) AS (", funcname)
	footer := ");"
	// 引数としての形に変形
	// - パス引数としていますがクエリは付いていれば除去
	//   `?` はメタ文字でエスケープが面倒なため文字集合を使っている
	body := fmt.Sprintf(`CONCAT(m," ",%s(p,"[?][^/]*$",""))`, rrShorthand)
	for _, p := range m.pairs {
		body = fmt.Sprintf(`%s(%s,"%s","%s")`, rrShorthand, body, p.RE, p.Name)
	}
	return utility + header + body + footer
}

// digStruct 指定されたパスで struct を掘る
//
// フレームワークがマッチに使用する RegExp を取り出す用途で用意されています。
// reflect を使っており雰囲気で書いています。
// target には *struct を渡す必要があります。struct を直接渡すと rv.Kind() が reflect.Interface になり動きません。
// また、子の struct は *struct でもいいですが **struct だと動作しません
func digStruct(target interface{}, paths []string) (reflect.Value, error) {
	// 参考:
	// - https://pod.hatenablog.com/entry/2020/08/06/234049
	// - https://qiita.com/saicologic/items/4eba162847d9b200019c
	var rv reflect.Value = reflect.ValueOf(target)
	for i, p := range paths {
		switch rv.Kind() {
		case reflect.Ptr:
			// ポインタなら deref
			rv = reflect.Indirect(rv)
		}

		if rv.Kind() != reflect.Struct {
			return reflect.Value{}, fmt.Errorf("not struct(.%s): %s", strings.Join(paths[0:i], "."), target)
		}
		priv := rv.FieldByName((p))
		if !priv.IsValid() {
			return reflect.Value{}, fmt.Errorf("invalid path(.%s)", strings.Join(paths[0:i+1], "."))
		}
		rv = reflect.NewAt(priv.Type(), unsafe.Pointer(priv.UnsafeAddr())).Elem()
	}
	return rv, nil
}

// CompileMuxRoutes ルート情報からパス名に分岐する
//
// slice の順番通りの優先度でマッチします。すなわち同じパスが複数の定義にマッチする場合添字が若いもののほうがマッチします。
// reflect によって gorilla/mux が内部的にマッチに使用している RegExp を抽出し、使用します。そのため意図せず panic する可能性があります
func CompileMuxRoutes(routes []GorillaMuxRoute) (Matcher, error) {
	var m Matcher
	for _, r := range routes {
		mr := new(mux.Route).Path(r.Pattern)
		digged, err := digStruct(mr, []string{"regexp", "path", "regexp"})
		if err != nil {
			return Matcher{}, fmt.Errorf("failed to extract RegExp from mux.Route(name=%s, pat=%s): %w", r.Name, r.Pattern, err)
		}
		re, ok := (digged.Interface()).(*regexp.Regexp)
		if !ok {
			return Matcher{}, fmt.Errorf("failed to extract RegExp from mux.Route(name=%s, pat=%s): route has not *regexp.Regexp type", r.Name, r.Pattern)
		}

		m.pairs = append(m.pairs, matchPair{
			Name: r.Name,
			RE:   fmt.Sprintf("^%s %s", r.Method, strings.TrimPrefix(re.String(), "^")),
		})
	}
	return m, nil
}
