# Configuration

`litxr` reads its project config path from `LITXR_CONFIG`.

## Setup

Add this to `.Renviron` and restart R:

```sh
LITXR_CONFIG=/absolute/path/to/project/config.yaml
```

Then initialize the file once:

```r
library(litxr)
litxr_init()
```

If the file already exists, `litxr_init()` refuses to overwrite it.

## Config Shape

The canonical top-level schema is:

```yaml
version: 1
project:
  name: my_project_name
  data_root: data/literature
collections:
  - collection_id: journal_of_finance
    collection_type: journal
    title: Journal of Finance
    remote_channel: crossref
    local_path: data/literature/journal_of_finance
    metadata:
      publisher: Wiley
      issn_print: 0022-1082
      issn_electronic: 1540-6261
    sync:
      filters:
        issn: 0022-1082
  - collection_id: arxiv_cs_ai
    collection_type: arxiv_category
    title: arXiv cs.AI
    remote_channel: arxiv
    local_path: data/literature/arxiv_cs_ai
    metadata:
      archive: arXiv
      category: cs.AI
    sync:
      search_query: cat:cs.AI
      rows: 100
      delay_seconds: 3
```

## Path Semantics

- `project.data_root` is the project-level data root.
- `collection.local_path` is the collection-local storage root.
- Relative paths are resolved relative to the folder containing `config.yaml`.
- `project.data_root: .` means “use the folder containing `config.yaml`”.
- `project.data_root: ~` means “use the home directory”.

## Collection Types

Current examples:

- `journal`
- `arxiv_category`
- `manual_batch`

The config is collection-first. Old `journals:` configs are still normalized on
read for backward compatibility.

## Config APIs

Useful functions:

- `litxr_config_path()`
- `litxr_init()`
- `litxr_read_config()`
- `litxr_list_collections()`
- `litxr_list_journals()` for backward compatibility

## Notes

- YAML indentation may use spaces or tabs.
- The default `project.name` is inferred from the config file's project root.
- Operational sync state is not stored in `config.yaml`.
- Sync history is stored separately under `project.data_root/index/`.
