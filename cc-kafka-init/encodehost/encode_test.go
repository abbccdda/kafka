package encodehost

import "testing"

func Test_parseIpv4(t *testing.T) {
	type args struct {
		ipAddr string
	}
	tests := []struct {
		name    string
		args    args
		want    uint32
		wantErr bool
	}{
		{
			name:    "Fail on junk",
			args:    args{ipAddr: "barf"},
			wantErr: true,
		},
		{
			name:    "Fail on IPv6",
			args:    args{ipAddr: "2001:db8::68"},
			wantErr: true,
		},
		{
			name: "IPv4 succeeds",
			args: args{ipAddr: "10.15.52.189"},
			want: 168768701,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseIpv4(tt.args.ipAddr)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseIpv4() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseIpv4() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_encodeLastTwoOctetsHex(t *testing.T) {
	type args struct {
		ipv4 uint32
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Last two octets as hex",
			args: args{ipv4: 168768701},
			want: "34bd",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := encodeLastTwoOctetsHex(tt.args.ipv4); got != tt.want {
				t.Errorf("encodeLastTwoOctetsHex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEncode(t *testing.T) {
	type args struct {
		hostIp string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Encodes host IP",
			args: args{hostIp: "10.15.52.189"},
			want: "34bd",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Encode(tt.args.hostIp)
			if (err != nil) != tt.wantErr {
				t.Errorf("Encode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Encode() got = %v, want %v", got, tt.want)
			}
		})
	}
}
