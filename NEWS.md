# litxr 0.0.2

- Added explicit journal upsert behavior with conflict logging for unusual
  metadata replacements.
- Added `fst`-backed journal indexes and `litxr_rebuild_journal_index()` for
  faster local reads.
- Improved Crossref and arXiv parsing robustness across payload shape
  variations.
- Added arXiv request pacing, retry handling, and `litxr_repair_journal()` for
  bounded repair and backfill runs.
- Added `scripts/repair_arxiv.R` as a small CLI wrapper for shell-driven arXiv
  repair jobs.
- Clarified local storage layout, including future `pdf/`, `md/`, and `llm/`
  directions.

# litxr 0.0.1

- Added project initialization with local `.litxr/config.yaml` support.
- Added journal-centric local storage and BibTeX export helpers.
- Added arXiv and Crossref parsing helpers for a unified reference schema.
- Added cursor-paginated Crossref journal sync support.
- Added README guidance, package documentation, and initial test coverage.
