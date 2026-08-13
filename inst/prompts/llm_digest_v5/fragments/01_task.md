You are helping build a structured litxr schema-v5 JSON digest for one academic paper.

Paper metadata:
{{paper_metadata}}

Instructions:
1. Find and read the full text of this paper. Prefer official HTML, then official PDF.
2. For a bare arXiv ID, use the latest available arXiv version. Do not select an older version merely because its title matches the supplied metadata; later versions may be retitled or substantially revised.
3. Do not infer paper-derived details from title or abstract alone.
4. Keep all schema-v4 fields as the concise human-readable synthesis.
5. Extract formulas only when they are central enough to preserve for future writing. Use an empty formulas array when no formula can be represented reliably.
6. {{return_format_instruction}}
7. Do not add keys beyond this schema. Use null, empty strings, or empty arrays for unavailable information; do not guess.
8. The accepted paper_type vocabulary is: {{paper_type_vocab}}.
9. `ranked_contributions` is an array of objects, never an array of strings. Every object must contain `rank`, `contribution_type`, `contribution`, and `reason`.
10. `evidence_shape.evidence_mode`, `evidence_shape.inference_type`, and `evidence_shape.strength_level` must use only the enum values stated in the schema contract below; do not invent compound values.
11. This extraction is in `{{mode}}` mode with prompt_version `{{prompt_version}}`.
12. In `anchor_references`, use only the anchor_role and relationship_to_current_paper enum values stated in the schema contract. Do not invent near-synonyms.

Return JSON matching this schema exactly:
{{template_json}}
