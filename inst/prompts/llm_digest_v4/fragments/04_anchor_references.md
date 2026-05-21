Anchor reference field guidance:
- citation_key: short citation key such as boyd_2011_admm
- anchor_title: the prior-work title or short citation text
- anchor_ref_id: if the anchor reference has a detected arXiv id or DOI, use canonical form such as arxiv:2401.01234 or doi:10.1000/example; otherwise use null
- anchor_role: one role label such as methodological_foundation, conceptual_foundation, review_anchor, main_comparison
- reason: why this prior work matters for understanding the current paper
- relationship_to_current_paper: one relationship such as builds_on, extends, compares_with, uses_as_context
- Do not fabricate anchor_ref_id. Only provide an arXiv id or DOI when it is explicitly detected from the paper, reference list, metadata, or reliable source.

Anchor reference example:
```json
[{"anchor_rank":1,"citation_key":"boyd_2011_admm","anchor_title":"Distributed Optimization and Statistical Learning via the Alternating Direction Method of Multipliers","anchor_ref_id":"doi:10.1561/2200000016","anchor_role":"methodological_foundation","reason":"Provides the ADMM background underlying the optimization workflow discussed in the paper.","relationship_to_current_paper":"builds_on","confidence":"high"}]
```
