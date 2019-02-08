package http

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thebeatapp/patron/encoding"
	"github.com/thebeatapp/patron/encoding/json"
	"github.com/thebeatapp/patron/info"
)

func Test_Route(t *testing.T) {
	info.UpdateName("Name")
	info.UpsertMetric("Name", "Description", "Counter")
	r := infoRoute()
	mux := http.NewServeMux()
	mux.HandleFunc(r.Pattern, r.Handler)

	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/info")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, json.TypeCharset, resp.Header.Get(encoding.ContentTypeHeader))

	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.NotEmpty(t, body)
}
