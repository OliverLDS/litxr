.litxr_prompt_fragment_root <- function(schema_version = "v4") {
  root <- system.file(
    "prompts",
    paste0("llm_digest_", schema_version),
    package = "litxr",
    mustWork = FALSE
  )
  if (!nzchar(root)) {
    root <- file.path(getwd(), "inst", "prompts", paste0("llm_digest_", schema_version))
  }
  if (!dir.exists(root)) {
    stop("LLM digest prompt resources not found for schema_version = ", schema_version, ".", call. = FALSE)
  }
  root
}

.litxr_read_prompt_fragment <- function(root, file_name) {
  path <- file.path(root, "fragments", file_name)
  if (!file.exists(path)) {
    stop("Prompt fragment not found: ", path, call. = FALSE)
  }
  paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
}

.litxr_render_prompt_fragment <- function(text, values) {
  rendered <- text
  for (name in names(values)) {
    token <- paste0("{{", name, "}}")
    replacement <- as.character(values[[name]] %||% "")
    hits <- gregexpr(token, rendered, fixed = TRUE)[[1]]
    if (identical(hits[[1]], -1L)) {
      next
    }
    hit_lengths <- attr(hits, "match.length", exact = TRUE)
    starts <- as.integer(hits)
    ends <- starts + as.integer(hit_lengths) - 1L
    out <- character()
    cursor <- 1L
    for (i in seq_along(starts)) {
      if (starts[[i]] > cursor) {
        out <- c(out, substr(rendered, cursor, starts[[i]] - 1L))
      }
      out <- c(out, replacement)
      cursor <- ends[[i]] + 1L
    }
    if (cursor <= nchar(rendered)) {
      out <- c(out, substr(rendered, cursor, nchar(rendered)))
    }
    rendered <- paste0(out, collapse = "")
  }
  rendered
}

.litxr_select_llm_digest_prompt_fragments <- function(mode = "create", schema_version = "v4") {
  fragments <- c(
    "01_task.md",
    "02_source_lookup.md",
    "03_inline_blocks.md",
    "04_anchor_references.md",
    "05_citation_logic_nodes.md",
    "06_research_data.md",
    "07_field_semantics.md"
  )
  if (identical(schema_version, "v4")) {
    fragments <- c(fragments, "08_schema_contract.md")
  }
  if (identical(mode, "revise")) {
    fragments <- c(fragments, "08_revision.md")
  }
  c(fragments, "09_output_contract.md")
}

.litxr_prompt_scalar <- function(x, default = NA_character_) {
  if (is.null(x) || !length(x) || is.na(x[[1]]) || !nzchar(as.character(x[[1]]))) {
    return(default)
  }
  as.character(x[[1]])
}

.litxr_llm_digest_prompt_metadata <- function(ref_id, ref) {
  title <- .litxr_prompt_scalar(ref$title, ref_id)
  doi <- .litxr_prompt_scalar(ref$doi)
  source <- .litxr_prompt_scalar(ref$source)
  source_id <- .litxr_prompt_scalar(ref$source_id)
  lines <- c(sprintf("title: %s", title), sprintf("ref_id: %s", ref_id))
  if (!is.na(doi)) {
    lines <- c(lines, sprintf("doi: %s", doi))
  }
  if (!is.na(source_id) && !identical(source_id, ref_id)) {
    source_label <- if (is.na(source) || !nzchar(source)) "source" else source
    lines <- c(lines, sprintf("%s_id: %s", source_label, source_id))
  }
  paste(lines, collapse = "\n")
}

.litxr_llm_digest_prompt_source_hint <- function(ref_id) {
  if (!startsWith(ref_id, "arxiv:")) {
    return("Use reliable full-text sources for this paper. Prefer publisher or official repository pages when available.")
  }
  arxiv_id <- sub("^arxiv:", "", ref_id)
  paste(c(
    "ArXiv-specific hint:",
    sprintf("- HTML full text: https://arxiv.org/html/%s", arxiv_id),
    sprintf("- PDF full text: https://arxiv.org/pdf/%s", arxiv_id),
    "Use these direct links first before exploring other resources."
  ), collapse = "\n")
}

.litxr_llm_digest_return_format_instruction <- function(return_format = "download_json_file") {
  return_format <- match.arg(
    as.character(return_format),
    c("download_json_file", "inline_raw_json", "markdown_fenced_json")
  )

  switch(
    return_format,
    download_json_file = "Return a downloadable JSON file named litxr_schema.json.",
    inline_raw_json = paste(
      "Return raw JSON only, with no Markdown fence, no explanation, and no surrounding text."
    ),
    markdown_fenced_json = "Return the JSON inside a fenced Markdown code block with json after the opening backticks."
  )
}

#' Build the interactive LLM digest prompt
#'
#' Builds the schema-aware prompt used by the interactive ChatGPT digest
#' workflow. The prompt text is assembled from package prompt fragments under
#' `inst/prompts/`, while reference lookup, schema-template rendering, arXiv
#' source hints, and revise-mode digest inclusion stay in R code.
#'
#' @param ref_id Canonical reference identifier.
#' @param config Optional parsed config list or config path.
#' @param mode Either `"create"` or `"revise"`.
#' @param schema_version Digest schema version. Supported values are `"v3"`
#'   and `"v4"`.
#' @param prompt_version Prompt-template version metadata to include in the
#'   prompt.
#' @param return_format Expected response format from the external LLM. Either
#'   `"download_json_file"`, `"inline_raw_json"`, or `"markdown_fenced_json"`.
#'
#' @return A single character string containing the full prompt.
#' @export
litxr_llm_digest_prompt <- function(ref_id, config = NULL, mode = c("create", "revise"), schema_version = "v4", prompt_version = "v4.0", return_format = c("download_json_file", "inline_raw_json", "markdown_fenced_json")) {
  ref_id <- as.character(ref_id)
  if (!length(ref_id) || is.na(ref_id[[1]]) || !nzchar(ref_id[[1]])) {
    stop("`ref_id` is required.", call. = FALSE)
  }
  ref_id <- ref_id[[1]]
  if (!(schema_version %in% c("v3", "v4"))) {
    stop("Only `schema_version = \"v3\"` or `\"v4\"` is supported by `litxr_llm_digest_prompt()`.", call. = FALSE)
  }
  mode <- match.arg(mode)
  return_format <- match.arg(return_format)
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) {
    cfg <- litxr_read_config()
  }

  ref <- litxr_find_refs(ref_id = ref_id, config = cfg)
  if (!nrow(ref)) {
    stop("Reference not found in local litxr cache: ", ref_id, call. = FALSE)
  }
  if (nrow(ref) > 1L) {
    stop("Expected exactly one reference row for ", ref_id, " but found ", nrow(ref), ".", call. = FALSE)
  }

  existing_digest <- litxr_read_llm_digest(ref_id, cfg)
  if (identical(mode, "create") && !is.null(existing_digest)) {
    stop("A local digest already exists for this ref_id. Use `mode = \"revise\"` to improve it.", call. = FALSE)
  }
  if (identical(mode, "revise") && is.null(existing_digest)) {
    stop("`mode = \"revise\"` requires an existing local digest for ref_id: ", ref_id, call. = FALSE)
  }

  template <- litxr_llm_digest_template(ref_id, schema_version = schema_version)
  template_json <- jsonlite::toJSON(
    template,
    auto_unbox = TRUE,
    pretty = TRUE,
    null = "null",
    na = "null"
  )
  existing_digest_json <- if (identical(mode, "revise")) {
    jsonlite::toJSON(existing_digest, auto_unbox = TRUE, pretty = TRUE, null = "null", na = "null")
  } else {
    ""
  }

  values <- list(
    paper_metadata = .litxr_llm_digest_prompt_metadata(ref_id, ref),
    source_lookup_hint = .litxr_llm_digest_prompt_source_hint(ref_id),
    paper_type_vocab = paste(litxr_paper_type_levels(), collapse = ", "),
    citation_logic_type_vocab = paste(paste0("- ", .litxr_citation_logic_type_levels()), collapse = "\n"),
    mode = mode,
    prompt_version = as.character(prompt_version),
    return_format_instruction = .litxr_llm_digest_return_format_instruction(return_format),
    existing_digest_json = existing_digest_json,
    template_json = as.character(template_json)
  )

  root <- .litxr_prompt_fragment_root(schema_version)
  fragments <- .litxr_select_llm_digest_prompt_fragments(mode, schema_version = schema_version)
  rendered <- vapply(
    fragments,
    function(file_name) {
      .litxr_render_prompt_fragment(.litxr_read_prompt_fragment(root, file_name), values)
    },
    character(1)
  )
  paste0(paste(rendered, collapse = "\n\n"), "\n")
}
