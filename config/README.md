# Polymarket tag dictionary

`polymarket_tags.json` is a human-readable lookup from stable Polymarket tag ID
to its current label and slug. The master, slave, filters, and resident market
catalog do not load this file; runtime routing uses tag IDs only.

Refresh it from Gamma when operators need an up-to-date dictionary:

```sh
python3 tools/update_polymarket_tag_map.py
```

Example lookup:

```sh
jq '."1"' config/polymarket_tags.json
```
