Schema-v5 extends schema-v4. Keep every existing V4 key and value type unchanged.

The V4 synthesis contract remains mandatory: summary, motivation,
research_questions, paper_structure, methods, research_data,
identification_strategy, main_variables, key_findings, limitations,
theoretical_mechanism, empirical_setting, descriptive_statistics_summary,
standardized_findings_summary, contribution_type, ranked_contributions,
likely_reader_misconceptions, business_relevance_pathway, evidence_strength,
evidence_shape, keywords, notes, tables, research_target_github_links,
anchor_references, and citation_logic_nodes retain their V4 meanings.
In particular, identification_strategy: how the paper supports its empirical or causal claim through design, comparison, triangulation, benchmark protocol, or the reason no causal identification is claimed.

Required V4 nested shapes and enums:

- ranked_contributions is an array of objects, never an array of strings. Every item must have exactly the required keys `rank` (positive integer), `contribution_type` (string), `contribution` (non-empty string), and `reason` (string). Example: `{ "rank": 1, "contribution_type": "method", "contribution": "Introduces the method.", "reason": "It is the paper's central technical contribution." }`.
- evidence_shape is an object with `evidence_mode`, `evidence_basis`, `inference_type`, `strength_level`, and `limitations`.
- evidence_shape.evidence_mode must be exactly one of: `empirical_quantitative`, `empirical_qualitative`, `experimental`, `simulation`, `benchmark`, `theoretical_model`, `conceptual_argument`, `methodological_demonstration`, `review_synthesis`, `policy_analysis`, `descriptive`, `none`, `unknown`.
- evidence_shape.inference_type must be exactly one of: `causal`, `associational`, `predictive`, `descriptive`, `mechanistic`, `formal`, `interpretive`, `comparative`, `normative`, `synthetic`, `not_applicable`, `unknown`.
- evidence_shape.strength_level must be exactly one of: `very_low`, `low`, `medium`, `high`, `very_high`, `not_applicable`, `unknown`.
- Do not combine enum values into new tokens such as `benchmark_and_execution_validation` or `descriptive_comparative`. Choose the one best-fitting canonical value, and put any nuance in evidence_basis or limitations.

source_detail is optional source-grounded detail. When present it must contain:

- schema_version: `v5`.
- coverage: equations, benchmark_tables, precise_wording, and methodological_disputes; each is complete, partial, or not_applicable.
- evidence_items: reusable source-located evidence objects. Every item must contain `evidence_id`, `claim`, `evidence_type`, `source_locator`, `conditions`, `limitation`, and `supports_v4`. `evidence_type` must be one of `theorem`, `experiment`, `ablation`, `benchmark`, `case_study`, or `author_limit`. Every quantitative claim must be represented here or in a benchmark table with a source locator and conditions.
- equation_cards: every item must contain `equation_id`, `latex`, `display_name`, `source_locator`, `symbols`, `assumptions`, `role_in_argument`, `plain_language_interpretation`, `supports_v4`, and `drafting_guidance`. Use source_locator fields section, page, label, table, figure, equation, and appendix. Put equation numbers in label.
- benchmark_tables: every item must contain `table_id`, `title`, `source_locator`, `task_or_dataset`, `metric_definitions`, `experimental_conditions`, `columns`, `rows`, `author_reported_takeaway`, `digest_interpretation_boundary`, and `supports_v4`. Preserve columns, rows, metrics, conditions, and scope boundaries structurally. Use the legacy V4 tables field only as a concise projection where useful; do not replace its compatible shape.
- wording_cards: every item must contain `wording_id`, `purpose`, `short_verbatim_excerpt`, `max_excerpt_words`, `faithful_paraphrase`, `why_precision_matters`, `source_locator`, and `supports_v4`. Short excerpts only, never more than 25 words. Prefer faithful_paraphrase in prose.
- methodological_disputes: every item must contain `dispute_id`, `question`, `paper_position`, `alternative_position_or_interpretation`, `source_of_alternative`, `evidence_for_paper_position`, `evidence_for_alternative`, `conditions_that_change_the_answer`, `unresolved_point`, `editorial_rule`, and `supports_v4`.
- unresolved_detail_gaps: short statements of source detail that remains unavailable.
- safe_for_digest_only_drafting: false whenever relevant coverage is partial or the needed equation, proof, table, or methodological nuance is not captured.

Every item in evidence_items, equation_cards, benchmark_tables, wording_cards, and methodological_disputes must include supports_v4. Each supports_v4 object names an existing V4 field. Include item_index only when grounding an ordered V4 array, such as key_findings or ranked_contributions. Do not include item_index for scalar fields such as theoretical_mechanism.

Use evidence_items as the primary reusable evidence layer. Preserve negative results, ablations, appendix caveats, and scope boundaries when they materially qualify a V4 claim. For large appendix sweeps, mark a benchmark table secondary while preserving machine-readable rows only when they are useful for future drafting.
