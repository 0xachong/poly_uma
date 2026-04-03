package uma

import (
	"regexp"
	"strings"
)

var (
	reMarketID = regexp.MustCompile(`market_id:\s*(\d+)`)
	reTitle    = regexp.MustCompile(`title:\s*([^,]+?)(?:\s*,\s*description:|\s*$)`)
	reResData  = regexp.MustCompile(`res_data:\s*(.+?)(?:\s*,\s*\w+:|\s*$)`)
)

// ParseAncillaryData 从链上 ancillaryData 字符串解析 market_id / title / res_data。
func ParseAncillaryData(ancillary string) *AncillaryInfo {
	if ancillary == "" {
		return nil
	}
	info := &AncillaryInfo{}
	if m := reMarketID.FindStringSubmatch(ancillary); len(m) > 1 {
		info.MarketID = strings.TrimSpace(m[1])
	}
	if m := reTitle.FindStringSubmatch(ancillary); len(m) > 1 {
		info.Title = strings.TrimSpace(m[1])
	}
	if m := reResData.FindStringSubmatch(ancillary); len(m) > 1 {
		info.ResData = strings.TrimSpace(m[1])
	}
	if info.MarketID == "" && info.Title == "" && info.ResData == "" {
		return nil
	}
	return info
}
