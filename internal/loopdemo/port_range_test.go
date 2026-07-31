package loopdemo

import (
	"reflect"
	"testing"
)

func TestParsePortRange(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []int
		wantErr bool
	}{
		// ---------- 正常输入 ----------
		{
			name:    "single port",
			input:   "8080",
			want:    []int{8080},
			wantErr: false,
		},
		{
			name:    "minimum port",
			input:   "1",
			want:    []int{1},
			wantErr: false,
		},
		{
			name:    "maximum port",
			input:   "65535",
			want:    []int{65535},
			wantErr: false,
		},
		{
			name:    "continuous range",
			input:   "8000-8002",
			want:    []int{8000, 8001, 8002},
			wantErr: false,
		},
		{
			name:    "range with single element",
			input:   "8000-8000",
			want:    []int{8000},
			wantErr: false,
		},
		{
			name:    "comma separated singles",
			input:   "80,81,82",
			want:    []int{80, 81, 82},
			wantErr: false,
		},
		{
			name:    "mixed segments",
			input:   "8000-8002,8080,9000-9001",
			want:    []int{8000, 8001, 8002, 8080, 9000, 9001},
			wantErr: false,
		},
		{
			name:    "with spaces around comma",
			input:   "8000-8002, 8080, 9000-9001",
			want:    []int{8000, 8001, 8002, 8080, 9000, 9001},
			wantErr: false,
		},
		{
			name:    "with leading and trailing spaces",
			input:   " 8000 , 8002 ",
			want:    []int{8000, 8002},
			wantErr: false,
		},
		{
			name:    "range from minimum boundary",
			input:   "1-5",
			want:    []int{1, 2, 3, 4, 5},
			wantErr: false,
		},
		{
			name:    "range to maximum boundary",
			input:   "65530-65535",
			want:    []int{65530, 65531, 65532, 65533, 65534, 65535},
			wantErr: false,
		},

		// ---------- 错误输入 ----------
		{
			name:    "empty input",
			input:   "",
			wantErr: true,
		},
		{
			name:    "whitespace only input",
			input:   "   ",
			wantErr: true,
		},
		{
			name:    "empty segment between commas",
			input:   "8080,,9090",
			wantErr: true,
		},
		{
			name:    "trailing comma",
			input:   "8080,",
			wantErr: true,
		},
		{
			name:    "leading comma",
			input:   ",8080",
			wantErr: true,
		},
		{
			name:    "invalid port number (non-numeric)",
			input:   "abc",
			wantErr: true,
		},
		{
			name:    "invalid port number in range start",
			input:   "abc-8080",
			wantErr: true,
		},
		{
			name:    "invalid port number in range end",
			input:   "8080-xyz",
			wantErr: true,
		},
		{
			name:    "port below minimum",
			input:   "0",
			wantErr: true,
		},
		{
			name:    "port above maximum",
			input:   "65536",
			wantErr: true,
		},
		{
			name:    "range start below minimum",
			input:   "0-5",
			wantErr: true,
		},
		{
			name:    "range end above maximum",
			input:   "65000-66000",
			wantErr: true,
		},
		{
			name:    "reverse range",
			input:   "8002-8000",
			wantErr: true,
		},
		{
			name:    "duplicate single ports",
			input:   "8080,8080",
			wantErr: true,
		},
		{
			name:    "duplicate port in range overlap",
			input:   "8000-8002,8080,8001",
			wantErr: true,
		},
		{
			name:    "duplicate port range",
			input:   "8000-8002,8000-8002",
			wantErr: true,
		},
		{
			name:    "invalid range format no dash value",
			input:   "8000-",
			wantErr: true,
		},
		{
			name:    "invalid range format leading dash",
			input:   "-8000",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePortRange(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParsePortRange(%q) expected error, got %v", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Errorf("ParsePortRange(%q) unexpected error: %v", tt.input, err)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParsePortRange(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
