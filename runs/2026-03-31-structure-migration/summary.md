# Structure Migration Verification

## Purpose

Verify that the active pipeline still runs after moving code and artifacts into the structured repository layout.

## Command

```powershell
python .\src\pipeline\common_fixed_point.py
```

## Outputs

- tables: [data/derived/tables.common_node_fixed_point.json](C:/projects/tables/data/derived/tables.common_node_fixed_point.json)
- report: [data/reports/report.common_node_fixed_point.json](C:/projects/tables/data/reports/report.common_node_fixed_point.json)
- forced bits: [data/derived/bits.common_node_fixed_point.forced.json](C:/projects/tables/data/derived/bits.common_node_fixed_point.forced.json)
- rewrite map: [data/derived/bits.common_node_fixed_point.rewrite_map.json](C:/projects/tables/data/derived/bits.common_node_fixed_point.rewrite_map.json)
- components: [data/derived/bits.common_node_fixed_point.components.json](C:/projects/tables/data/derived/bits.common_node_fixed_point.components.json)
- dropped tables: [data/derived/tables.common_node_fixed_point.dropped_included.json](C:/projects/tables/data/derived/tables.common_node_fixed_point.dropped_included.json)
- pair relations: [data/derived/pairs.common_node_fixed_point.relations.json](C:/projects/tables/data/derived/pairs.common_node_fixed_point.relations.json)
- nodes: [data/derived/nodes.common_node_fixed_point.json](C:/projects/tables/data/derived/nodes.common_node_fixed_point.json)

## Result

- productive rounds: `6`
- rounds including final check: `7`
- final tables: `124408`
- final bits: `124488`
- rewritten original bits: `4017`
- forced original bits: `262`
