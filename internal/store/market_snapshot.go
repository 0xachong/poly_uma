package store

// MarketTag is the compact, stable representation used for routing. Matching
// must use ID; Label and Slug are display/debug metadata only.
type MarketTag struct {
	ID    string `json:"id"`
	Label string `json:"label,omitempty"`
	Slug  string `json:"slug,omitempty"`
}

// MarketSnapshot contains all market data needed by downstream workers. The
// active catalog stores one immutable instance and indexes it by market_id.
type MarketSnapshot struct {
	MarketID              string      `json:"market_id"`
	ConditionID           string      `json:"condition_id"`
	Question              string      `json:"question"`
	Slug                  string      `json:"slug,omitempty"`
	Description           string      `json:"description,omitempty"`
	PolymarketEventID     string      `json:"event_id,omitempty"`
	PolymarketEventTitle  string      `json:"event_title,omitempty"`
	PolymarketEventSlug   string      `json:"event_slug,omitempty"`
	Tags                  []MarketTag `json:"tags,omitempty"`
	Category              string      `json:"category,omitempty"`
	SportsMarketType      string      `json:"sports_market_type,omitempty"`
	TokenIDs              []string    `json:"token_ids,omitempty"`
	Outcomes              []string    `json:"outcomes,omitempty"`
	OutcomePrices         []float64   `json:"outcome_prices,omitempty"`
	Active                bool        `json:"active"`
	Closed                bool        `json:"closed"`
	AcceptingOrders       bool        `json:"accepting_orders"`
	EnableOrderBook       bool        `json:"enable_order_book"`
	UMAResolutionStatus   string      `json:"uma_resolution_status,omitempty"`
	UMAResolutionStatuses []string    `json:"uma_resolution_statuses,omitempty"`
	TakerBaseFee          int         `json:"taker_base_fee,omitempty"`
	RetentionClass        string      `json:"retention_class,omitempty"`
	GammaUpdatedAtMS      int64       `json:"gamma_updated_at_ms,omitempty"`
	CatalogSyncedAtUS     int64       `json:"catalog_synced_at_us"`
}
