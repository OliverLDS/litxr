Schema-v5 extends schema-v4. Keep every existing V4 key and value type unchanged.

source_detail is optional source-grounded detail. When present it must contain:

- schema_version: `v5`.
- coverage: equations, benchmark_tables, precise_wording, and methodological_disputes; each is complete, partial, or not_applicable.
- evidence_items: reusable source-located evidence objects. Every quantitative claim must be represented here or in a benchmark table with a source locator and conditions.
- equation_cards: exact LaTex, symbol meanings, assumptions, source locator, and drafting guidance. Use source_locator fields section, page, label, table, figure, equation, and appendix. Put equation numbers in label.
- benchmark_tables: preserve columns, rows, metrics, conditions, and scope boundaries structurally. Use the legacy V4 tables field only as a concise projection where useful; do not replace its compatible shape.
- wording_cards: short excerpts only, never more than 25 words. Prefer faithful_paraphrase in prose.
- methodological_disputes: the paper position, alternative, evidence on both sides, conditions, unresolved point, and editorial rule.
- unresolved_detail_gaps: short statements of source detail that remains unavailable.
- safe_for_digest_only_drafting: false whenever relevant coverage is partial or the needed equation, proof, table, or methodological nuance is not captured.

Every item in evidence_items, equation_cards, benchmark_tables, wording_cards, and methodological_disputes must include supports_v4. Each supports_v4 object names an existing V4 field. Include item_index only when grounding an ordered V4 array, such as key_findings or ranked_contributions. Do not include item_index for scalar fields such as theoretical_mechanism.

Use evidence_items as the primary reusable evidence layer. Preserve negative results, ablations, appendix caveats, and scope boundaries when they materially qualify a V4 claim. For large appendix sweeps, mark a benchmark table secondary while preserving machine-readable rows only when they are useful for future drafting.
