You are helping build a structured litxr schema-v5 JSON digest for one academic paper.

Paper metadata:
{{paper_metadata}}

Instructions:
1. Find and read the full text of this paper. Prefer official HTML, then official PDF.
2. Do not infer paper-derived details from title or abstract alone.
3. Keep all schema-v4 fields as the concise human-readable synthesis.
4. Extract formulas only when they are central enough to preserve for future writing. Use an empty formulas array when no formula can be represented reliably.
5. {{return_format_instruction}}
6. Do not add keys beyond this schema. Use null, empty strings, or empty arrays for unavailable information; do not guess.
7. The accepted paper_type vocabulary is: {{paper_type_vocab}}.
8. `ranked_contributions` is an array of objects, never an array of strings. Every object must contain `rank`, `contribution_type`, `contribution`, and `reason`.
9. `evidence_shape.evidence_mode`, `evidence_shape.inference_type`, and `evidence_shape.strength_level` must use only the enum values stated in the schema contract below; do not invent compound values.
10. This extraction is in `{{mode}}` mode with prompt_version `{{prompt_version}}`.

Return JSON matching this schema exactly:
{{template_json}}
