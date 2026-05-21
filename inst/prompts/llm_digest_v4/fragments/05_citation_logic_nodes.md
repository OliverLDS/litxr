Citation logic node field guidance:
- claim_sentence: a reusable citation-ready sentence
- logic_type: choose exactly one controlled relation label from the allowed vocabulary below:
{{citation_logic_type_vocab}}
- subject_text: the main subject of the claim
- object_text: the main object or outcome of the claim
- modifier_text: qualifier, condition, mechanism, or relation detail when useful
- citation_use: when a future writer should cite this node

Citation logic node example:
```json
[{"node_id":"node_1","claim_sentence":"Agentic workflows can reduce manual coordination burdens when reasoning, tool use, and validation are decomposed across specialized agents.","logic_type":"A_reduces_B","subject_text":"agentic workflows with specialized agents","object_text":"manual coordination burdens","modifier_text":"when reasoning, tool use, and validation are decomposed across agent roles","evidence_role":"conceptual_argument","confidence":"medium","page_or_section":"Section 3","quote_support":"","citation_use":"Use when arguing that agentic workflow design can reduce coordination-heavy manual work.","tags":["agentic workflow","coordination","specialized agents"]}]
```
