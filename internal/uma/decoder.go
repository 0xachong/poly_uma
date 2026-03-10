package uma

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
)

const wordLen = 32

// ParseLog 将一条原始 EVM log 解析为 *Event，topic0 不匹配时返回 nil, nil。
func ParseLog(vLog ethtypes.Log) (*Event, error) {
	if len(vLog.Topics) == 0 {
		return nil, nil
	}
	t0 := vLog.Topics[0]
	switch t0 {
	case common.HexToHash(TopicQuestionInitialized):
		ev, err := parseInit(vLog)
		if err != nil {
			return nil, err
		}
		return &Event{Kind: "QuestionInitialized", Init: ev}, nil
	case common.HexToHash(TopicRequestPrice):
		ev, err := parseRequest(vLog)
		if err != nil {
			return nil, err
		}
		return &Event{Kind: "RequestPrice", Request: ev}, nil
	case common.HexToHash(TopicProposePrice):
		ev, err := parsePropose(vLog)
		if err != nil {
			return nil, err
		}
		return &Event{Kind: "ProposePrice", Propose: ev}, nil
	case common.HexToHash(TopicDisputePrice):
		ev, err := parseDispute(vLog)
		if err != nil {
			return nil, err
		}
		return &Event{Kind: "DisputePrice", Dispute: ev}, nil
	case common.HexToHash(TopicQuestionResolved):
		ev, err := parseResolved(vLog)
		if err != nil {
			return nil, err
		}
		return &Event{Kind: "QuestionResolved", Resolved: ev}, nil
	case common.HexToHash(TopicSettle):
		ev, err := parseSettle(vLog)
		if err != nil {
			return nil, err
		}
		return &Event{Kind: "Settle", Settle: ev}, nil
	}
	return nil, nil
}

// ScalePrice 将 uint256 整数字符串 ÷ 10^16，返回人类可读小数。
func ScalePrice(raw string) string {
	if raw == "" {
		return ""
	}
	n, ok := new(big.Int).SetString(raw, 10)
	if !ok {
		return raw
	}
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(16), nil)
	rat := new(big.Rat).SetFrac(n, divisor)
	f, _ := new(big.Float).SetPrec(64).SetRat(rat).Float64()
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// ── 内部解析函数 ─────────────────────────────────────────────────────────────

func readDynamic(data []byte, offsetWord int) ([]byte, error) {
	if len(data) < (offsetWord+1)*wordLen {
		return nil, fmt.Errorf("data too short for offset at word %d", offsetWord)
	}
	off := new(big.Int).SetBytes(data[offsetWord*wordLen : (offsetWord+1)*wordLen]).Uint64()
	if uint64(len(data)) < off+wordLen {
		return nil, fmt.Errorf("data too short for dynamic at offset %d", off)
	}
	length := new(big.Int).SetBytes(data[off : off+wordLen]).Uint64()
	if uint64(len(data)) < off+wordLen+length {
		return nil, fmt.Errorf("dynamic content exceeds data length")
	}
	return data[off+wordLen : off+wordLen+length], nil
}

func ancillaryFrom(data []byte, word int) (string, *AncillaryInfo) {
	b, err := readDynamic(data, word)
	if err != nil || len(b) == 0 {
		return "", nil
	}
	s := string(b)
	return s, ParseAncillaryData(s)
}

func parseInit(vLog ethtypes.Log) (*QuestionInitializedEvent, error) {
	if len(vLog.Topics) < 4 {
		return nil, fmt.Errorf("QuestionInitialized: expected 4 topics, got %d", len(vLog.Topics))
	}
	ev := &QuestionInitializedEvent{
		QuestionID:       "0x" + hex.EncodeToString(vLog.Topics[1][:]),
		RequestTimestamp: new(big.Int).SetBytes(vLog.Topics[2][:]).Uint64(),
		Creator:          common.BytesToAddress(vLog.Topics[3][12:]).Hex(),
		BlockNumber:      vLog.BlockNumber,
		TxHash:           vLog.TxHash.Hex(),
	}
	data := vLog.Data
	if len(data) >= 4*wordLen {
		ev.RewardToken = common.BytesToAddress(data[wordLen : 2*wordLen]).Hex()
		ev.Reward = new(big.Int).SetBytes(data[2*wordLen : 3*wordLen]).String()
		ev.ProposalBond = new(big.Int).SetBytes(data[3*wordLen : 4*wordLen]).String()
		ev.AncillaryData, ev.ParsedAncillary = ancillaryFrom(data, 0)
	}
	return ev, nil
}

func parseRequest(vLog ethtypes.Log) (*RequestPriceEvent, error) {
	if len(vLog.Topics) < 4 {
		return nil, fmt.Errorf("RequestPrice: expected 4 topics, got %d", len(vLog.Topics))
	}
	ev := &RequestPriceEvent{
		Requester:   common.BytesToAddress(vLog.Topics[1][12:]).Hex(),
		Identifier:  "0x" + hex.EncodeToString(vLog.Topics[2][:]),
		Timestamp:   new(big.Int).SetBytes(vLog.Topics[3][:]).Uint64(),
		BlockNumber: vLog.BlockNumber,
		TxHash:      vLog.TxHash.Hex(),
	}
	data := vLog.Data
	if len(data) >= 4*wordLen {
		ev.Currency = common.BytesToAddress(data[wordLen : 2*wordLen]).Hex()
		ev.Reward = new(big.Int).SetBytes(data[2*wordLen : 3*wordLen]).String()
		ev.FinalFee = new(big.Int).SetBytes(data[3*wordLen : 4*wordLen]).String()
		ev.AncillaryData, ev.ParsedAncillary = ancillaryFrom(data, 0)
	}
	return ev, nil
}

func parsePropose(vLog ethtypes.Log) (*ProposePriceEvent, error) {
	if len(vLog.Topics) < 3 {
		return nil, fmt.Errorf("ProposePrice: expected 3 topics, got %d", len(vLog.Topics))
	}
	ev := &ProposePriceEvent{
		Requester:   common.BytesToAddress(vLog.Topics[1][12:]).Hex(),
		Proposer:    common.BytesToAddress(vLog.Topics[2][12:]).Hex(),
		BlockNumber: vLog.BlockNumber,
		TxHash:      vLog.TxHash.Hex(),
	}
	data := vLog.Data
	if len(data) >= 6*wordLen {
		ev.Identifier = "0x" + hex.EncodeToString(data[0:wordLen])
		ev.Timestamp = new(big.Int).SetBytes(data[wordLen : 2*wordLen]).Uint64()
		ev.ProposedPrice = new(big.Int).SetBytes(data[3*wordLen : 4*wordLen]).String()
		ev.ExpirationTimestamp = new(big.Int).SetBytes(data[4*wordLen : 5*wordLen]).Uint64()
		ev.Currency = common.BytesToAddress(data[5*wordLen : 6*wordLen]).Hex()
		ev.AncillaryData, ev.ParsedAncillary = ancillaryFrom(data, 2)
	}
	return ev, nil
}

func parseDispute(vLog ethtypes.Log) (*DisputePriceEvent, error) {
	if len(vLog.Topics) < 4 {
		return nil, fmt.Errorf("DisputePrice: expected 4 topics, got %d", len(vLog.Topics))
	}
	ev := &DisputePriceEvent{
		Requester:   common.BytesToAddress(vLog.Topics[1][12:]).Hex(),
		Proposer:    common.BytesToAddress(vLog.Topics[2][12:]).Hex(),
		Disputer:    common.BytesToAddress(vLog.Topics[3][12:]).Hex(),
		BlockNumber: vLog.BlockNumber,
		TxHash:      vLog.TxHash.Hex(),
	}
	data := vLog.Data
	if len(data) >= 4*wordLen {
		ev.Identifier = "0x" + hex.EncodeToString(data[0:wordLen])
		ev.Timestamp = new(big.Int).SetBytes(data[wordLen : 2*wordLen]).Uint64()
		ev.ProposedPrice = new(big.Int).SetBytes(data[3*wordLen : 4*wordLen]).String()
		ev.AncillaryData, ev.ParsedAncillary = ancillaryFrom(data, 2)
	}
	return ev, nil
}

func parseResolved(vLog ethtypes.Log) (*QuestionResolvedEvent, error) {
	if len(vLog.Topics) < 3 {
		return nil, fmt.Errorf("QuestionResolved: expected 3 topics, got %d", len(vLog.Topics))
	}
	ev := &QuestionResolvedEvent{
		QuestionID:   "0x" + hex.EncodeToString(vLog.Topics[1][:]),
		SettledPrice: new(big.Int).SetBytes(vLog.Topics[2][:]).String(),
		BlockNumber:  vLog.BlockNumber,
		TxHash:       vLog.TxHash.Hex(),
	}
	data := vLog.Data
	if len(data) >= 2*wordLen {
		off := new(big.Int).SetBytes(data[0:wordLen]).Uint64()
		if uint64(len(data)) >= off+wordLen {
			count := new(big.Int).SetBytes(data[off : off+wordLen]).Uint64()
			for i := uint64(0); i < count && uint64(len(data)) >= off+wordLen+(i+1)*wordLen; i++ {
				ev.Payouts = append(ev.Payouts,
					new(big.Int).SetBytes(data[off+wordLen+i*wordLen:off+wordLen+(i+1)*wordLen]).String())
			}
		}
	}
	return ev, nil
}

func parseSettle(vLog ethtypes.Log) (*SettleEvent, error) {
	if len(vLog.Topics) < 4 {
		return nil, fmt.Errorf("Settle: expected 4 topics, got %d", len(vLog.Topics))
	}
	ev := &SettleEvent{
		Requester:   common.BytesToAddress(vLog.Topics[1][12:]).Hex(),
		Proposer:    common.BytesToAddress(vLog.Topics[2][12:]).Hex(),
		Disputer:    common.BytesToAddress(vLog.Topics[3][12:]).Hex(),
		BlockNumber: vLog.BlockNumber,
		TxHash:      vLog.TxHash.Hex(),
	}
	data := vLog.Data
	if len(data) >= 5*wordLen {
		ev.Identifier = "0x" + hex.EncodeToString(data[0:wordLen])
		ev.Timestamp = new(big.Int).SetBytes(data[wordLen : 2*wordLen]).Uint64()
		ev.Price = new(big.Int).SetBytes(data[3*wordLen : 4*wordLen]).String()
		ev.Payout = new(big.Int).SetBytes(data[4*wordLen : 5*wordLen]).String()
		ev.AncillaryData, _ = ancillaryFrom(data, 2)
	}
	return ev, nil
}
