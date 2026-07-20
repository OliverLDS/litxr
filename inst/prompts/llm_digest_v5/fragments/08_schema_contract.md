Schema-v5 retains the complete schema-v4 contract plus one optional field: `formulas`.

Core metadata:
- schema_version: must be "v5".
- ref_id: the reference id from the prompt metadata.
- digest_revision, extraction_mode, prompt_version, and model_hint: keep the template values.
- paper_type: one canonical paper nature from the accepted vocabulary.

Core analysis:
- summary, motivation, research_questions, paper_structure, methods, key_findings, limitations,
  theoretical_mechanism, keywords, and notes retain their schema-v4 meanings.

Empirical fields:
- research_data: data sources, sample period, sample region, unit of observation, numeric
  sample_size when it is a real count, and sample_size_note for narrative or ambiguous context.
- identification_strategy: how the paper supports its empirical or causal claim through design,
  comparison logic, triangulation, qualitative process tracing, benchmark protocol, or the reason
  no causal identification is claimed.
- main_variables: dependent, independent, control, and mechanism variables. Use empty arrays when
  not applicable.
- empirical_setting, descriptive_statistics_summary, and standardized_findings_summary retain
  their schema-v4 meanings.

Contribution and evidence:
- contribution_type: unordered contribution labels. Prefer theory_building, theory_testing,
  conceptual_framework, empirical_evidence, causal_evidence, measurement, method, algorithm,
  benchmark, system_architecture, replication, literature_synthesis, policy_implication,
  business_implication, research_agenda, or other.
- ranked_contributions: an array of objects, never strings. Every object contains rank,
  contribution_type, contribution, and reason.
- evidence_strength: short legacy strength assessment.
- evidence_shape: general evidence description. evidence_mode must be empirical_quantitative,
  empirical_qualitative, experimental, simulation, benchmark, theoretical_model,
  conceptual_argument, methodological_demonstration, review_synthesis, policy_analysis,
  descriptive, none, or unknown. inference_type must be causal, associational, predictive,
  descriptive, mechanistic, formal, interpretive, comparative, normative, synthetic,
  not_applicable, or unknown. strength_level must be very_low, low, medium, high, very_high,
  not_applicable, or unknown.

Reader and business fields:
- likely_reader_misconceptions: common ways a reader might misunderstand or overgeneralize the
  paper.
- business_relevance_pathway: concrete pathways connecting the contribution to decisions,
  operations, governance, product design, market strategy, risk management, or workflows.

Structured extraction fields:
- tables: an array of table objects. Use [] when no table can be represented reliably. Each table
  object includes table_id, title, source_location, columns, rows, and notes.
- research_target_github_links: GitHub repositories that are the paper's research target or
  artifact. Use [] unless applicable. Each object includes url, category_tags, research_role,
  description, and evidence_context. Accepted URLs begin with https://github.com/,
  https://gist.github.com/, or https://github.gatech.edu/.

Inline retrieval blocks:
- anchor_references: up to three prior works that anchor, motivate, compare with, or frame the
  paper.
- citation_logic_nodes: reusable citation-ready semantic claims this paper can support in future
  writing.

Formula extraction:
- formulas is an optional array of formula objects. Use [] when the paper has no formula worth
  preserving or when the source does not support a reliable extraction. Every formula object must
  contain formula_id, latex, display_name, source_location, symbols,
  plain_language_interpretation, and assumptions.
- formula_id is a short unique id, for example equation_1. symbols is an array of objects, each
  with symbol and meaning. assumptions is an array of conditions or scope limits.

Do not include source_detail, evidence cards, benchmark-table copies, wording cards,
methodological disputes, coverage declarations, cross-links, or drafting-safety flags.
