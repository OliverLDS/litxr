You are helping build a structured litxr schema-v3 JSON digest for one academic paper.

Paper metadata:
{{paper_metadata}}

Instructions:
1. Find the full text of this paper.
2. Prefer an HTML full-text version if available.
3. If HTML full text is not available, try to find a PDF version.
4. Make sure you actually read the full text instead of guessing from abstract or metadata only.
5. If you cannot find the full text, say clearly that you cannot find it and do not invent details.
6. After reading the full text, parse the paper into the exact schema-v3 JSON below.
7. {{return_format_instruction}}
8. Do not add extra keys beyond this schema unless they already exist in the schema.
9. Keep unknown fields explicit with null, empty string, or empty arrays as appropriate; do not guess.
10. The accepted paper_type vocabulary is: {{paper_type_vocab}}.
11. This extraction is in `{{mode}}` mode with prompt_version `{{prompt_version}}`.
