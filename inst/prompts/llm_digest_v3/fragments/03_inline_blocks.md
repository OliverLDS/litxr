Populate the optional inline v3 blocks explicitly when supported by the paper:
- anchor_references
- citation_logic_nodes

Use these inline blocks to provide up to three anchor references and reusable citation logic sentences.

Follow these field rules strictly:
- Do not collapse multiple meanings into one field.
- Do not put the whole explanation into citation_key and leave the other anchor fields empty.
- Do not leave claim_sentence empty or copy node_id into claim_sentence.
- Do not repeat the same citation_logic_node just to attach different tags. Keep one node per semantic claim and combine all relevant tags into that node's tags array.
- If a field is unsupported by the paper, use null, empty string, or empty array instead of guessing.
