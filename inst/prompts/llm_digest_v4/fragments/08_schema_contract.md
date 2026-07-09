Schema-v4 contract and field meanings:

Core metadata:
- schema_version: must be "v4".
- ref_id: the canonical litxr reference id from the prompt metadata.
- digest_revision: positive integer managed by litxr; keep the template value.
- extraction_mode: extraction workflow label managed by litxr; keep the template value.
- prompt_version: prompt version metadata managed by litxr; keep the template value.
- model_hint: optional model name or empty/null if unknown.
- paper_type: one canonical paper nature from the accepted vocabulary.

Core analysis:
- summary: concise plain-language summary of the paper.
- motivation: the gap, puzzle, problem, debate, or practical need motivating the paper.
- research_questions: explicit or inferred research questions.
- paper_structure: section-level outline with each section's purpose where possible.
- methods: methods, designs, analytical procedures, or review strategy used by the paper.
- key_findings: main findings, conclusions, propositions, or results supported by the paper.
- limitations: limitations stated by the authors or clearly implied by the design.
- theoretical_mechanism: the causal, conceptual, formal, or process mechanism the paper argues for; use empty/null when not applicable.
- keywords: retrieval-oriented keywords.
- notes: short free-form notes, not a replacement for required fields.

Empirical fields:
- research_data: data sources, sample period, sample region, unit of observation, numeric sample_size when it is a real count, and sample_size_note for narrative or ambiguous sample-size context.
- identification_strategy: how the paper supports its empirical or causal claim through research design. This includes randomized assignment, quasi-experimental design, panel/observational identification, comparison logic, triangulation, qualitative process tracing, benchmark protocol, or the reason no causal identification is claimed.
- main_variables: dependent, independent, control, and mechanism variables. Use empty arrays if not applicable.
- empirical_setting: institutional, geographic, market, organizational, technical, or social context for the evidence.
- descriptive_statistics_summary: concise summary of descriptive statistics or data characteristics when present.
- standardized_findings_summary: concise summary of machine-readable or table-like findings when present.

Contribution and evidence:
- contribution_type: unordered contribution labels from the paper. Prefer these labels when applicable: theory_building, theory_testing, conceptual_framework, empirical_evidence, causal_evidence, measurement, method, algorithm, benchmark, system_architecture, replication, literature_synthesis, policy_implication, business_implication, research_agenda, other.
- ranked_contributions: ranked contribution objects with fields rank, contribution_type, contribution, and reason. Rank the most central contribution as 1. Do not rank minor claims above the paper's core contribution.
- evidence_strength: short legacy strength assessment. Prefer one of very_low, low, medium, high, very_high, not_applicable, unknown; a short explanatory string is acceptable when needed for backward compatibility.
- evidence_shape: general evidence description for any paper type, not only empirical papers.
  - evidence_mode must be one of: empirical_quantitative, empirical_qualitative, experimental, simulation, benchmark, theoretical_model, conceptual_argument, methodological_demonstration, review_synthesis, policy_analysis, descriptive, none, unknown.
  - evidence_basis: several short points describing what the paper uses as support.
  - inference_type must be one of: causal, associational, predictive, descriptive, mechanistic, formal, interpretive, comparative, normative, synthetic, not_applicable, unknown.
  - strength_level must be one of: very_low, low, medium, high, very_high, not_applicable, unknown.
  - limitations: several short evidence limitations.

Reader and business fields:
- likely_reader_misconceptions: several common ways an average reader might misunderstand, overgeneralize, or misuse the paper.
- business_relevance_pathway: several concrete pathways connecting the paper's contribution to business decisions, operations, governance, product design, market strategy, risk management, or organizational workflows.

Structured extraction fields:
- tables: recognized structured tables from the paper as an array of table objects. Use an empty array when no table can be represented reliably. Each table object must include:
  - table_id: short stable id such as "table_1".
  - title: table caption or inferred short title.
  - source_location: table number, section, page, appendix, or other location.
  - columns: column names or column descriptors.
  - rows: array of row objects preserving the table's structured values.
  - notes: table notes, caveats, or extraction limitations.
- research_target_github_links: GitHub repositories that are the paper's research target or artifact. Use an empty array for incidental links or when no such repository exists. Each object must include:
  - url: the detected https://github.com/..., https://gist.github.com/..., or https://github.gatech.edu/... URL.
  - category_tags: short tags describing what the repository is about, such as model, framework, system, dataset, benchmark, mechanism, package, implementation, or evaluation target.
  - research_role: how the repository functions in the paper.
  - description: concise description of the repository.
  - evidence_context: where or how the paper indicates this GitHub link.

Inline retrieval blocks:
- anchor_references: up to three prior works that anchor, motivate, compare with, or frame the paper.
- citation_logic_nodes: reusable citation-ready semantic claims this paper can support in future writing.
