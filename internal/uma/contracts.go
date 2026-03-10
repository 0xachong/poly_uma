package uma

// Polygon 主网合约地址
const (
	// UMA CTF Adapter v3（UmaCtf Adapter，发出 QuestionInitialized / QuestionResolved）
	PolygonUmaCtfAdapterV3 = "0x157Ce2d672854c848c9b79C49a8Cc6cc89176a49"
	// UMA Optimistic Oracle（发出 RequestPrice / ProposePrice / DisputePrice / Settle）
	PolygonOptimisticOracle = "0xCB1822859cEF82Cd2Eb4E6276C7916e692995130"
	// UMA Adapter v2（旧版，可选订阅历史）
	PolygonUmaCtfAdapterV2 = "0x6A9D222616C90FcA5754cd1333cFD9b7fb6a4F74"
)

// UMA OO 六类事件的 Topic0（事件签名 keccak256）
const (
	TopicQuestionInitialized = "0xeee0897acd6893adcaf2ba5158191b3601098ab6bece35c5d57874340b64c5b7"
	TopicRequestPrice        = "0xf1679315ff325c257a944e0ca1bfe7b26616039e9511f9610d4ba3eca851027b"
	TopicProposePrice        = "0x6e51dd00371aabffa82cd401592f76ed51e98a9ea4b58751c70463a2c78b5ca1"
	TopicDisputePrice        = "0x5165909c3d1c01c5d1e121ac6f6d01dda1ba24bc9e1f975b5a375339c15be7f3"
	TopicQuestionResolved    = "0x566c3fbdd12dd86bb341787f6d531f79fd7ad4ce7e3ae2d15ac0ca1b601af9df"
	TopicSettle              = "0x3f384afb4bd9f0aef0298c80399950011420eb33b0e1a750b20966270247b9a0"
)

// AllTopics 用于 FilterQuery，一次订阅/拉取全部六类事件。
var AllTopics = []string{
	TopicQuestionInitialized,
	TopicRequestPrice,
	TopicProposePrice,
	TopicDisputePrice,
	TopicQuestionResolved,
	TopicSettle,
}
