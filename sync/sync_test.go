package sync

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thebeatapp/patron/encoding/json"
)

func TestNewRequest(t *testing.T) {
	req := NewRequest(nil, nil, nil)
	assert.NotNil(t, req)
}

func TestRequest_Decode(t *testing.T) {
	j, err := json.Encode("string")
	assert.NoError(t, err)
	b := bytes.NewBuffer(j)
	req := NewRequest(nil, b, json.Decode)
	assert.NotNil(t, req)
	var data string
	err = req.Decode(&data)
	assert.NoError(t, err)
	assert.Equal(t, "string", data)
}

func TestNewResponse(t *testing.T) {
	rsp := NewResponse("test")
	assert.NotNil(t, rsp)
	assert.IsType(t, "test", rsp.Payload)
}
