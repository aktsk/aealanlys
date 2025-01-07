package bqmatcher_test

import (
	"testing"

	"github.com/aktsk/aealanlys/bqmatcher"
)

func TestCompileMuxRoutes(t *testing.T) {
	tests := []struct {
		name     string
		routes   []bqmatcher.GorillaMuxRoute
		funcname string
		expected string
		wantErr  bool
	}{
		{
			name: "simple",
			routes: []bqmatcher.GorillaMuxRoute{
				{Name: "index", Method: "GET", Pattern: "/"},
				{Name: "show", Method: "GET", Pattern: "/{id:[0-9]+}"},
			},
			funcname: "F",
			expected: `CREATE TEMPORARY FUNCTION RR(v STRING,rx STRING,re STRING) AS (REGEXP_REPLACE(v,rx,re));CREATE TEMPORARY FUNCTION F(m STRING,p STRING) AS (RR(RR(CONCAT(m," ",RR(p,"[?][^/]*$","")),"^GET /$","index"),"^GET /(?P<v0>[0-9]+)$","show"));`,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := bqmatcher.CompileMuxRoutes(tt.routes)
			if (err != nil) != tt.wantErr {
				t.Errorf("CompileMuxRoutes() error = %v; wantErr %v", err, tt.wantErr)
				return
			}

			actual := m.PathToGroupingKeyBQUDF(tt.funcname)
			if actual != tt.expected {
				t.Errorf("CompileMuxRoutes() = '%v'; want '%v'", actual, tt.expected)
			}
		})
	}
}
