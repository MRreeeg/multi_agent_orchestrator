package loopdemo

import (
	"errors"
	"strconv"
	"strings"
)

// ParsePortRange 解析端口范围字符串并返回端口号列表。
//
// 支持格式:
//   - 单端口: "8080"
//   - 连续范围: "8000-8002" → [8000,8001,8002]
//   - 逗号组合: "8000-8002,8080"
//   - 可混合空格: "8000-8002, 8080"
//
// 端口范围 1~65535，反向范围返回错误，重复端口返回错误。
func ParsePortRange(spec string) ([]int, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, errors.New("empty input")
	}

	seen := make(map[int]bool)
	var result []int

	parts := strings.Split(spec, ",")
	for _, part := range parts {
		seg := strings.TrimSpace(part)
		if seg == "" {
			return nil, errors.New("empty segment in input")
		}

		if strings.Contains(seg, "-") {
			ports, err := parseRangeSegment(seg)
			if err != nil {
				return nil, err
			}
			for _, p := range ports {
				if seen[p] {
					return nil, errors.New("duplicate port: " + strconv.Itoa(p))
				}
				seen[p] = true
			}
			result = append(result, ports...)
		} else {
			port, err := strconv.Atoi(seg)
			if err != nil {
				return nil, errors.New("invalid port number: " + seg)
			}
			if port < 1 || port > 65535 {
				return nil, errors.New("port out of range: " + strconv.Itoa(port))
			}
			if seen[port] {
				return nil, errors.New("duplicate port: " + strconv.Itoa(port))
			}
			seen[port] = true
			result = append(result, port)
		}
	}

	return result, nil
}

// parseRangeSegment 解析如 "8000-8002" 的范围段。
func parseRangeSegment(seg string) ([]int, error) {
	parts := strings.SplitN(seg, "-", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, errors.New("invalid range format: " + seg)
	}

	start, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, errors.New("invalid port number: " + parts[0])
	}
	end, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, errors.New("invalid port number: " + parts[1])
	}

	if start < 1 || start > 65535 || end < 1 || end > 65535 {
		return nil, errors.New("port out of range: " + seg)
	}

	if start > end {
		return nil, errors.New("reverse range: " + seg)
	}

	var ports []int
	for i := start; i <= end; i++ {
		ports = append(ports, i)
	}
	return ports, nil
}
