Populate the inline digest blocks explicitly when supported by the paper:
- anchor_references
- citation_logic_nodes
- tables
- research_target_github_links

Use these inline blocks to provide up to three anchor references, reusable citation logic sentences, recognized structured table data, and research-target GitHub links.

Follow these field rules strictly:
- Do not collapse multiple meanings into one field.
- Do not put the whole explanation into citation_key and leave the other anchor fields empty.
- Do not leave claim_sentence empty or copy node_id into claim_sentence.
- Do not repeat the same citation_logic_node just to attach different tags. Keep one node per semantic claim and combine all relevant tags into that node's tags array.
- For tables, only extract a table when the paper presents table-like structured data that can be represented as rows and columns. Preserve the table meaning, column names, row values, source location, and notes; use an empty array when no structured table can be recognized reliably.
- For research_target_github_links, include a GitHub URL only when the GitHub repository is the paper's research target or artifact, such as a model, framework, system, dataset, benchmark, mechanism, package, or implementation introduced, studied, or evaluated by the paper. Accepted URL hosts are https://github.com/, https://gist.github.com/, and https://github.gatech.edu/. Do not include incidental citations or background links. Add category_tags describing what the GitHub repository is about.
- If a field is unsupported by the paper, use null, empty string, or empty array instead of guessing.
