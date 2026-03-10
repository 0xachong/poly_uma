// Package uma 定义 UMA Optimistic Oracle 链上事件类型，从 Polygon 链上 log 解析而来。
// 此包从 polyworker/types 独立复制，poly_uma 不依赖 polyworker 任何代码。
package uma

// AncillaryInfo 从 ancillaryData 解析出的结构化信息（与 Polymarket 约定一致）。
type AncillaryInfo struct {
	MarketID string // Polymarket 市场 ID，如 "1327728"
	Title    string // 命题标题
	ResData  string // 如 "p1: 0, p2: 1, p3: 0.5"
}

// QuestionInitializedEvent 命题初始化（UmaCtf Adapter 发出）。
type QuestionInitializedEvent struct {
	QuestionID       string
	RequestTimestamp uint64
	Creator          string
	AncillaryData    string
	RewardToken      string
	Reward           string
	ProposalBond     string
	BlockNumber      uint64
	TxHash           string
	ParsedAncillary  *AncillaryInfo
}

// RequestPriceEvent 请求价格（OO 发出，有人向 OO 发起一次价格请求）。
type RequestPriceEvent struct {
	Requester       string
	Identifier      string
	Timestamp       uint64
	AncillaryData   string
	Currency        string
	Reward          string
	FinalFee        string
	BlockNumber     uint64
	TxHash          string
	ParsedAncillary *AncillaryInfo
}

// ProposePriceEvent 提交命题结果（OO 发出）。
type ProposePriceEvent struct {
	Requester           string
	Proposer            string
	Identifier          string
	Timestamp           uint64
	AncillaryData       string
	ProposedPrice       string // int256，1e18=Yes 0=No
	ExpirationTimestamp uint64
	Currency            string
	BlockNumber         uint64
	TxHash              string
	ParsedAncillary     *AncillaryInfo
}

// DisputePriceEvent 提起争议（OO 发出）。
type DisputePriceEvent struct {
	Requester       string
	Proposer        string
	Disputer        string
	Identifier      string
	Timestamp       uint64
	AncillaryData   string
	ProposedPrice   string
	BlockNumber     uint64
	TxHash          string
	ParsedAncillary *AncillaryInfo
}

// QuestionResolvedEvent 命题确认关闭（UmaCtf Adapter 发出）。
type QuestionResolvedEvent struct {
	QuestionID   string
	SettledPrice string
	Payouts      []string
	BlockNumber  uint64
	TxHash       string
}

// SettleEvent 最终结算（OO 发出）。
type SettleEvent struct {
	Requester     string
	Proposer      string
	Disputer      string
	Identifier    string
	Timestamp     uint64
	AncillaryData string
	Price         string
	Payout        string
	BlockNumber   uint64
	TxHash        string
}

// Event 表示一次 UMA 链上事件（六类之一），用于订阅统一回调。
type Event struct {
	Kind     string // "QuestionInitialized" | "RequestPrice" | "ProposePrice" | "DisputePrice" | "QuestionResolved" | "Settle"
	Init     *QuestionInitializedEvent
	Request  *RequestPriceEvent
	Propose  *ProposePriceEvent
	Dispute  *DisputePriceEvent
	Resolved *QuestionResolvedEvent
	Settle   *SettleEvent
}

// TxHash 返回事件的交易哈希。
func (e *Event) TxHash() string {
	switch e.Kind {
	case "QuestionInitialized":
		return e.Init.TxHash
	case "RequestPrice":
		return e.Request.TxHash
	case "ProposePrice":
		return e.Propose.TxHash
	case "DisputePrice":
		return e.Dispute.TxHash
	case "QuestionResolved":
		return e.Resolved.TxHash
	case "Settle":
		return e.Settle.TxHash
	}
	return ""
}

// BlockNumber 返回事件所在区块号。
func (e *Event) BlockNumber() uint64 {
	switch e.Kind {
	case "QuestionInitialized":
		return e.Init.BlockNumber
	case "RequestPrice":
		return e.Request.BlockNumber
	case "ProposePrice":
		return e.Propose.BlockNumber
	case "DisputePrice":
		return e.Dispute.BlockNumber
	case "QuestionResolved":
		return e.Resolved.BlockNumber
	case "Settle":
		return e.Settle.BlockNumber
	}
	return 0
}

// QuestionID 返回事件关联的 questionID（仅 Init/Resolved 有）。
func (e *Event) QuestionID() string {
	switch e.Kind {
	case "QuestionInitialized":
		return e.Init.QuestionID
	case "QuestionResolved":
		return e.Resolved.QuestionID
	}
	return ""
}

// Price 返回事件关联的价格字符串。
func (e *Event) Price() string {
	switch e.Kind {
	case "ProposePrice":
		return e.Propose.ProposedPrice
	case "DisputePrice":
		return e.Dispute.ProposedPrice
	case "QuestionResolved":
		return e.Resolved.SettledPrice
	case "Settle":
		return e.Settle.Price
	}
	return ""
}

// Identifier 返回事件的 identifier（仅 Request/Propose/Dispute/Settle 有）。
func (e *Event) Identifier() string {
	switch e.Kind {
	case "RequestPrice":
		return e.Request.Identifier
	case "ProposePrice":
		return e.Propose.Identifier
	case "DisputePrice":
		return e.Dispute.Identifier
	case "Settle":
		return e.Settle.Identifier
	}
	return ""
}

// MarketID 从事件的 ParsedAncillary 中提取 market_id。
func (e *Event) MarketID() string {
	if e == nil {
		return ""
	}
	for _, pa := range []*AncillaryInfo{
		ancillaryOf(e.Init), ancillaryOf(e.Propose),
		ancillaryOf(e.Dispute), ancillaryOf(e.Request),
	} {
		if pa != nil && pa.MarketID != "" {
			return pa.MarketID
		}
	}
	return ""
}

type ancillaryHolder interface{ ancillary() *AncillaryInfo }

func ancillaryOf(v interface{}) *AncillaryInfo {
	switch t := v.(type) {
	case *QuestionInitializedEvent:
		if t != nil {
			return t.ParsedAncillary
		}
	case *ProposePriceEvent:
		if t != nil {
			return t.ParsedAncillary
		}
	case *DisputePriceEvent:
		if t != nil {
			return t.ParsedAncillary
		}
	case *RequestPriceEvent:
		if t != nil {
			return t.ParsedAncillary
		}
	}
	return nil
}
