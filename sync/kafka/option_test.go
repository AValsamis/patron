package kafka

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

func TestVersion(t *testing.T) {
	type args struct {
		version string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "success", args: args{version: sarama.V0_10_2_0.String()}, wantErr: false},
		{name: "failure, missing version", args: args{version: ""}, wantErr: true},
		{name: "failure, invalid version", args: args{version: "xxxxx"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := sarama.NewConfig()
			sp := &SyncProducer{cfg: cfg}
			err := Version(tt.args.version)(sp)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				v, err := sarama.ParseKafkaVersion(tt.args.version)
				assert.NoError(t, err)
				assert.Equal(t, v, sp.cfg.Version)
			}
		})
	}
}

func TestTimeouts(t *testing.T) {
	type args struct {
		dial time.Duration
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "success", args: args{dial: time.Second}, wantErr: false},
		{name: "fail, zero timeout", args: args{dial: 0 * time.Second}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := sarama.NewConfig()
			sp := &SyncProducer{cfg: cfg}
			err := Timeouts(tt.args.dial)(sp)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.args.dial, sp.cfg.Net.DialTimeout)
			}
		})
	}
}

func TestRequiredAcksPolicy(t *testing.T) {
	type args struct {
		requiredAcks int
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "success", args: args{requiredAcks: -1}, wantErr: false},
		{name: "success", args: args{requiredAcks: 0}, wantErr: false},
		{name: "success", args: args{requiredAcks: 1}, wantErr: false},
		{name: "invalid required acks policy", args: args{requiredAcks: 2}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ap := SyncProducer{cfg: sarama.NewConfig()}
			err := RequiredAcksPolicy(tt.args.requiredAcks)(&ap)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
