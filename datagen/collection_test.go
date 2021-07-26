package datagen_test

import (
	"io/ioutil"
	"regexp"
	"testing"

	"github.com/feliixx/mgodatagen/datagen"
)

func TestParseConfig(t *testing.T) {

	b, err := ioutil.ReadFile("generators/testdata/ref.json")
	if err != nil {
		t.Error(err)
	}

	configTests := []struct {
		name            string
		configBytes     []byte
		ignoreMissingDB bool
		correct         bool
		errMsgRegex     *regexp.Regexp
		nbColl          int
	}{
		{
			name:            "ref.json",
			configBytes:     b,
			ignoreMissingDB: false,
			correct:         true,
			errMsgRegex:     nil,
			nbColl:          2,
		},
		{
			name: "invalid content",
			configBytes: []byte(`[{
				"database": "mgodatagen_test", 
				"collection": "test",
				"count": 1000,
				"content": { "k": invalid }
				}]`),
			ignoreMissingDB: false,
			correct:         false,
			errMsgRegex:     regexp.MustCompile("^error in configuration file: object / array / Date badly formatted: \n\n\t\t.*"),
			nbColl:          0,
		},
		{
			name: "missing database field",
			configBytes: []byte(`[{
				"collection": "test",
				"count": 1000,
				"content": {}
				}]`),
			ignoreMissingDB: false,
			correct:         false,
			errMsgRegex:     regexp.MustCompile("^error in configuration file: \n\t'collection' and 'database' fields can't be empty.*"),
			nbColl:          0,
		},
		{
			name: "count > 0",
			configBytes: []byte(`[{
				"database": "mgodatagen_test", 
				"collection": "test",
				"count": 0,
				"content": {}
				}]`),
			ignoreMissingDB: false,
			correct:         false,
			errMsgRegex:     regexp.MustCompile("^error in configuration file: \n\tfor collection.*"),
			nbColl:          0,
		},
	}

	for _, tt := range configTests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := datagen.ParseConfig(tt.configBytes, tt.ignoreMissingDB)
			if tt.correct {
				if err != nil {
					t.Errorf("expected no error for config %s: %v", tt.configBytes, err)
				}
				if tt.nbColl != len(c) {
					t.Errorf("expected %d coll but got %d", tt.nbColl, len(c))
				}
			} else {
				if err == nil {
					t.Errorf("expected an error for config %s", tt.configBytes)
				}
				if !tt.errMsgRegex.MatchString(err.Error()) {
					t.Errorf("error message should match %s, but was %v", tt.errMsgRegex.String(), err)
				}
			}
		})
	}
}
