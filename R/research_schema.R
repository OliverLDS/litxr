#' Canonical paper type levels
#'
#' @return Character vector of canonical paper-type levels.
#' @export
litxr_paper_type_levels <- function() {
  c(
    "theoretical",
    "empirical_archival",
    "empirical_experimental",
    "empirical_survey",
    "empirical_case_study",
    "methodological",
    "review",
    "meta_analysis",
    "dataset",
    "policy_report",
    "book",
    "unknown"
  )
}

.litxr_paper_type_alias_map <- function() {
  canonical <- litxr_paper_type_levels()
  aliases <- c(
    theory = "theoretical",
    theoretical_paper = "theoretical",
    archival = "empirical_archival",
    archive = "empirical_archival",
    empirical_archive = "empirical_archival",
    empirical_archival_study = "empirical_archival",
    experimental = "empirical_experimental",
    experiment = "empirical_experimental",
    empirical_experiment = "empirical_experimental",
    survey = "empirical_survey",
    survey_based = "empirical_survey",
    questionnaire = "empirical_survey",
    case = "empirical_case_study",
    case_study = "empirical_case_study",
    empirical_case = "empirical_case_study",
    method = "methodological",
    methods = "methodological",
    methodology = "methodological",
    literature_review = "review",
    review_article = "review",
    systematic_review = "review",
    meta = "meta_analysis",
    meta_analytic = "meta_analysis",
    data = "dataset",
    data_paper = "dataset",
    database = "dataset",
    report = "policy_report",
    policy = "policy_report",
    policy_brief = "policy_report",
    policy_study = "policy_report",
    monograph = "book",
    text = "book",
    na = "unknown",
    n_a = "unknown",
    none = "unknown",
    missing = "unknown",
    unclear = "unknown"
  )
  stats::setNames(c(canonical, unname(aliases)), c(canonical, names(aliases)))
}

.litxr_paper_type_key <- function(x) {
  key <- tolower(trimws(as.character(x)))
  key[is.na(key)] <- ""
  key <- gsub("[[:space:]-]+", "_", key)
  key <- gsub("_+", "_", key)
  key <- gsub("^_+|_+$", "", key)
  key
}

#' Normalize paper-type values
#'
#' Lowercases, trims, replaces spaces and hyphens with underscores, maps common
#' aliases, and returns `"unknown"` for missing or empty values.
#'
#' @param x Character vector of paper-type values.
#'
#' @return Character vector of normalized paper types.
#' @export
litxr_normalize_paper_type <- function(x) {
  map <- .litxr_paper_type_alias_map()
  key <- .litxr_paper_type_key(x)
  out <- unname(map[key])
  out[is.na(out) | !nzchar(out)] <- "unknown"
  out
}

#' Validate paper-type values
#'
#' Accepts canonical values, common aliases, and missing/empty inputs. Invalid
#' non-empty inputs throw an error.
#'
#' @param x Character vector of paper-type values.
#'
#' @return Invisibly returns `TRUE` on success.
#' @export
litxr_validate_paper_type <- function(x) {
  key <- .litxr_paper_type_key(x)
  allowed <- names(.litxr_paper_type_alias_map())
  bad <- nzchar(key) & !(key %in% allowed)
  if (any(bad)) {
    stop(
      "Invalid paper_type value(s): ",
      paste(unique(as.character(x[bad])), collapse = ", "),
      ". Allowed canonical values are: ",
      paste(litxr_paper_type_levels(), collapse = ", "),
      call. = FALSE
    )
  }
  invisible(TRUE)
}

.litxr_llm_digest_schema_version <- function(digest) {
  version <- digest[["schema_version"]]
  if (is.null(version) || !length(version) || is.na(version[[1]]) || !nzchar(trimws(as.character(version[[1]])))) {
    return("v1")
  }
  as.character(version[[1]])
}

.litxr_llm_digest_template_v1 <- function(ref_id) {
  list(
    ref_id = as.character(ref_id),
    summary = NA_character_,
    motivation = NA_character_,
    research_questions = character(),
    methods = character(),
    sample = list(
      description = NA_character_,
      size = NA_character_,
      period = NA_character_
    ),
    key_findings = character(),
    limitations = character(),
    keywords = character(),
    notes = NA_character_,
    generated_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
  )
}

.litxr_llm_digest_template_v2 <- function(ref_id) {
  list(
    schema_version = "v2",
    ref_id = as.character(ref_id),
    digest_revision = 1L,
    derived_from_revision = NULL,
    extraction_mode = "unknown",
    prompt_version = NA_character_,
    model_hint = NA_character_,
    paper_type = "unknown",
    summary = NA_character_,
    motivation = NA_character_,
    research_questions = character(),
    paper_structure = character(),
    methods = character(),
    research_data = list(
      data_sources = character(),
      sample_period = NA_character_,
      sample_region = NA_character_,
      unit_of_observation = NA_character_,
      sample_size = NULL
    ),
    identification_strategy = NA_character_,
    main_variables = list(
      dependent_variables = character(),
      independent_variables = character(),
      control_variables = character(),
      mechanism_variables = character()
    ),
    key_findings = character(),
    limitations = character(),
    theoretical_mechanism = NA_character_,
    empirical_setting = NA_character_,
    descriptive_statistics_summary = NA_character_,
    standardized_findings_summary = NA_character_,
    contribution_type = character(),
    evidence_strength = NA_character_,
    keywords = character(),
    notes = NA_character_,
    generated_at = format(Sys.time(), tz = "UTC", usetz = TRUE),
    updated_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
  )
}

.litxr_first_nonnull <- function(...) {
  values <- list(...)
  for (value in values) {
    if (!is.null(value) && length(value)) {
      return(value)
    }
  }
  NULL
}

.litxr_normalize_llm_digest_for_write <- function(digest, ref_id = NULL) {
  version <- if (is.null(digest[["schema_version"]]) || !length(digest[["schema_version"]])) {
    "v2"
  } else {
    .litxr_llm_digest_schema_version(digest)
  }
  payload <- .litxr_llm_digest_template_v2(.litxr_first_nonnull(ref_id, digest$ref_id, ""))
  if (!identical(version, "v2") && !is.null(digest[["schema_version"]]) && length(digest[["schema_version"]])) {
    payload[["schema_version"]] <- as.character(digest[["schema_version"]][[1]])
  }

  for (name in intersect(names(payload), names(digest))) {
    payload[[name]] <- digest[[name]]
  }
  payload$ref_id <- as.character(.litxr_first_nonnull(ref_id, payload$ref_id, ""))
  payload$paper_type <- litxr_normalize_paper_type(.litxr_first_nonnull(payload$paper_type, NA_character_))
  if (!identical(version, "v2")) {
    payload$digest_revision <- 1L
    payload$derived_from_revision <- NULL
    payload$extraction_mode <- "legacy"
    payload$prompt_version <- NA_character_
    payload$model_hint <- NA_character_
    payload$updated_at <- payload$generated_at %||% format(Sys.time(), tz = "UTC", usetz = TRUE)
  } else {
    payload$digest_revision <- suppressWarnings(as.integer(.litxr_first_nonnull(payload$digest_revision, 1L)))
    if (is.na(payload$digest_revision) || payload$digest_revision < 1L) payload$digest_revision <- 1L
    derived <- .litxr_first_nonnull(payload$derived_from_revision, NULL)
    if (!is.null(derived)) {
      derived <- suppressWarnings(as.integer(derived[[1]]))
      if (is.na(derived) || derived < 1L) derived <- NULL
    }
    payload$derived_from_revision <- derived
    payload$extraction_mode <- as.character(.litxr_first_nonnull(payload$extraction_mode, "unknown"))
    if (!nzchar(payload$extraction_mode[[1]])) payload$extraction_mode <- "unknown"
    payload$prompt_version <- as.character(.litxr_first_nonnull(payload$prompt_version, NA_character_))
    payload$model_hint <- as.character(.litxr_first_nonnull(payload$model_hint, NA_character_))
    payload$generated_at <- as.character(.litxr_first_nonnull(payload$generated_at, format(Sys.time(), tz = "UTC", usetz = TRUE)))
    payload$updated_at <- as.character(.litxr_first_nonnull(payload$updated_at, payload$generated_at, format(Sys.time(), tz = "UTC", usetz = TRUE)))
  }
  payload
}

.litxr_postprocess_llm_digest_read <- function(digest) {
  if (!is.list(digest)) {
    stop("LLM digest payload must be a named list.", call. = FALSE)
  }
  if (!("schema_version" %in% names(digest))) {
    digest$schema_version <- "v1"
  }
  if (identical(.litxr_llm_digest_schema_version(digest), "v2")) {
    if (!("digest_revision" %in% names(digest)) || is.null(digest$digest_revision)) {
      digest$digest_revision <- 1L
    }
    if (!("derived_from_revision" %in% names(digest))) {
      digest$derived_from_revision <- NULL
    }
    if (!("extraction_mode" %in% names(digest)) || is.null(digest$extraction_mode) || !length(digest$extraction_mode)) {
      digest$extraction_mode <- "legacy"
    }
    if (!("prompt_version" %in% names(digest))) {
      digest$prompt_version <- NA_character_
    }
    if (!("model_hint" %in% names(digest))) {
      digest$model_hint <- NA_character_
    }
    if (!("updated_at" %in% names(digest))) {
      digest$updated_at <- digest$generated_at %||% NA_character_
    }
  }
  if ("paper_type" %in% names(digest)) {
    digest$paper_type <- litxr_normalize_paper_type(digest$paper_type)
  }
  digest
}

.litxr_llm_digest_is_v2 <- function(digest) {
  identical(.litxr_llm_digest_schema_version(digest), "v2")
}

.litxr_llm_digest_required_fields <- function(schema_version) {
  if (identical(schema_version, "v2")) {
    return(c(
      "schema_version", "ref_id", "digest_revision",
      "extraction_mode", "prompt_version", "model_hint", "paper_type", "summary", "motivation",
      "research_questions", "paper_structure", "methods", "research_data",
      "identification_strategy", "main_variables", "key_findings",
      "limitations", "theoretical_mechanism", "empirical_setting",
      "descriptive_statistics_summary", "standardized_findings_summary",
      "contribution_type", "evidence_strength", "keywords", "notes",
      "generated_at", "updated_at"
    ))
  }
  c(
    "ref_id", "summary", "motivation", "research_questions", "methods",
    "sample", "key_findings", "limitations", "keywords", "notes",
    "generated_at"
  )
}

.litxr_validate_list_fields <- function(digest, fields) {
  for (field in fields) {
    value <- digest[[field]]
    if (is.list(value)) {
      value <- unlist(value, use.names = FALSE)
    }
    if (!(is.character(value) || is.null(value))) {
      stop("LLM digest field `", field, "` must be a character vector.", call. = FALSE)
    }
  }
}

.litxr_validate_named_list_fields <- function(x, required_fields, field_name, character_vector_fields = character(), numeric_scalar_fields = character()) {
  if (!is.list(x)) {
    stop("LLM digest field `", field_name, "` must be a named list.", call. = FALSE)
  }
  missing <- setdiff(required_fields, names(x))
  if (length(missing)) {
    stop(
      "LLM digest field `", field_name, "` is missing required fields: ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }
  for (name in character_vector_fields) {
    value <- x[[name]]
    if (is.list(value)) {
      value <- unlist(value, use.names = FALSE)
    }
    if (!(is.character(value) || is.null(value))) {
      stop("LLM digest field `", field_name, "$", name, "` must be a character vector.", call. = FALSE)
    }
  }
  for (name in numeric_scalar_fields) {
    value <- x[[name]]
    if (is.list(value) && !length(value)) {
      value <- NULL
    }
    if (is.character(value) && length(value) == 1L && (is.na(value) || identical(value, "NA") || !nzchar(value))) {
      value <- NULL
    }
    if (!(is.null(value) || is.numeric(value) || is.integer(value) || (length(value) == 1L && is.na(value)))) {
      stop("LLM digest field `", field_name, "$", name, "` must be numeric or NA.", call. = FALSE)
    }
  }
}

.litxr_project_findings_dir <- function(cfg) {
  file.path(.litxr_project_root(cfg), "findings")
}

.litxr_ensure_project_findings_dir <- function(cfg) {
  dir_path <- .litxr_project_findings_dir(cfg)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }
  dir_path
}

.litxr_standardized_findings_paths <- function(cfg) {
  list(
    main = file.path(.litxr_project_findings_dir(cfg), "standardized_findings.fst"),
    delta = file.path(.litxr_project_findings_dir(cfg), "standardized_findings_delta.fst")
  )
}

.litxr_descriptive_stats_paths <- function(cfg) {
  list(
    main = file.path(.litxr_project_findings_dir(cfg), "descriptive_statistics.fst"),
    delta = file.path(.litxr_project_findings_dir(cfg), "descriptive_statistics_delta.fst")
  )
}

.litxr_empty_standardized_findings <- function() {
  data.table::data.table(
    ref_id = character(),
    finding_id = character(),
    paper_type = character(),
    research_question = character(),
    hypothesis = character(),
    outcome_variable = character(),
    treatment_variable = character(),
    moderator_variable = character(),
    method = character(),
    sample = character(),
    sample_period = character(),
    sample_region = character(),
    effect_direction = character(),
    effect_size = numeric(),
    standard_error = numeric(),
    t_stat = numeric(),
    p_value = numeric(),
    confidence_interval_low = numeric(),
    confidence_interval_high = numeric(),
    unit = character(),
    model_specification = character(),
    finding_text = character(),
    evidence_strength = character(),
    extraction_method = character(),
    extracted_at = character(),
    notes = character()
  )
}

.litxr_empty_descriptive_stats <- function() {
  data.table::data.table(
    ref_id = character(),
    table_id = character(),
    variable = character(),
    label = character(),
    n = numeric(),
    mean = numeric(),
    sd = numeric(),
    min = numeric(),
    p25 = numeric(),
    median = numeric(),
    p75 = numeric(),
    max = numeric(),
    unit = character(),
    sample = character(),
    sample_period = character(),
    source_table = character(),
    extraction_method = character(),
    extracted_at = character(),
    notes = character()
  )
}

.litxr_as_research_dt <- function(x) {
  if (inherits(x, "data.table")) {
    return(data.table::copy(x))
  }
  if (is.data.frame(x)) {
    return(data.table::as.data.table(x))
  }
  if (is.list(x)) {
    return(data.table::as.data.table(x))
  }
  stop("Input must be a data.frame, data.table, or named list.", call. = FALSE)
}

.litxr_align_columns <- function(dt, template) {
  for (name in setdiff(names(template), names(dt))) {
    prototype <- template[[name]]
    dt[[name]] <- if (is.list(prototype)) {
      rep(list(NULL), nrow(dt))
    } else if (is.integer(prototype)) {
      rep(NA_integer_, nrow(dt))
    } else if (is.numeric(prototype)) {
      rep(NA_real_, nrow(dt))
    } else if (is.logical(prototype)) {
      rep(NA, nrow(dt))
    } else {
      rep(NA_character_, nrow(dt))
    }
  }
  dt[, names(template), with = FALSE]
}

.litxr_normalize_standardized_findings <- function(x) {
  dt <- .litxr_align_columns(.litxr_as_research_dt(x), .litxr_empty_standardized_findings())
  dt <- data.table::copy(dt)
  char_cols <- setdiff(names(dt), c(
    "effect_size", "standard_error", "t_stat", "p_value",
    "confidence_interval_low", "confidence_interval_high"
  ))
  for (name in char_cols) {
    dt[[name]] <- as.character(dt[[name]])
  }
  num_cols <- c("effect_size", "standard_error", "t_stat", "p_value", "confidence_interval_low", "confidence_interval_high")
  for (name in num_cols) {
    dt[[name]] <- as.numeric(dt[[name]])
  }
  dt$paper_type <- litxr_normalize_paper_type(dt$paper_type)
  dt
}

.litxr_normalize_descriptive_stats <- function(x) {
  dt <- .litxr_align_columns(.litxr_as_research_dt(x), .litxr_empty_descriptive_stats())
  dt <- data.table::copy(dt)
  char_cols <- setdiff(names(dt), c("n", "mean", "sd", "min", "p25", "median", "p75", "max"))
  for (name in char_cols) {
    dt[[name]] <- as.character(dt[[name]])
  }
  num_cols <- c("n", "mean", "sd", "min", "p25", "median", "p75", "max")
  for (name in num_cols) {
    dt[[name]] <- as.numeric(dt[[name]])
  }
  dt
}

.litxr_key_string <- function(dt, cols) {
  do.call(paste, c(lapply(cols, function(col) {
    value <- as.character(dt[[col]])
    value[is.na(value)] <- ""
    value
  }), list(sep = "\r")))
}

.litxr_upsert_table_by_key <- function(existing, incoming, key_cols) {
  if (!nrow(existing)) {
    return(data.table::copy(incoming))
  }
  if (!nrow(incoming)) {
    return(data.table::copy(existing))
  }
  existing <- data.table::copy(existing)
  incoming <- data.table::copy(incoming)
  keep_existing <- !(.litxr_key_string(existing, key_cols) %in% .litxr_key_string(incoming, key_cols))
  out <- data.table::rbindlist(list(existing[keep_existing, ], incoming), fill = TRUE)
  key <- .litxr_key_string(out, key_cols)
  out[!duplicated(key), ]
}

.litxr_read_fst_or_empty <- function(path, empty_fun) {
  if (!file.exists(path)) {
    return(empty_fun())
  }
  data.table::as.data.table(fst::read_fst(path, as.data.table = TRUE))
}

.litxr_research_query_keep <- function(values, query) {
  text <- tolower(values)
  q <- tolower(as.character(query[[1]]))
  !is.na(text) & grepl(q, text, fixed = TRUE)
}

#' Template row for standardized findings
#'
#' @return One-row `data.table` with the standardized findings schema.
#' @export
litxr_standardized_findings_template <- function() {
  data.table::data.table(
    ref_id = "",
    finding_id = "",
    paper_type = "unknown",
    research_question = NA_character_,
    hypothesis = NA_character_,
    outcome_variable = NA_character_,
    treatment_variable = NA_character_,
    moderator_variable = NA_character_,
    method = NA_character_,
    sample = NA_character_,
    sample_period = NA_character_,
    sample_region = NA_character_,
    effect_direction = NA_character_,
    effect_size = NA_real_,
    standard_error = NA_real_,
    t_stat = NA_real_,
    p_value = NA_real_,
    confidence_interval_low = NA_real_,
    confidence_interval_high = NA_real_,
    unit = NA_character_,
    model_specification = NA_character_,
    finding_text = NA_character_,
    evidence_strength = NA_character_,
    extraction_method = NA_character_,
    extracted_at = NA_character_,
    notes = NA_character_
  )
}

#' Validate standardized findings rows
#'
#' @param findings Data frame, data.table, or named list of standardized
#'   findings rows.
#'
#' @return Invisibly returns `TRUE` on success.
#' @export
litxr_validate_standardized_findings <- function(findings) {
  dt <- .litxr_normalize_standardized_findings(findings)
  bad_ref <- is.na(dt$ref_id) | !nzchar(trimws(dt$ref_id))
  if (any(bad_ref)) {
    stop("Standardized findings require non-empty `ref_id` values.", call. = FALSE)
  }
  bad_id <- is.na(dt$finding_id) | !nzchar(trimws(dt$finding_id))
  if (any(bad_id)) {
    stop("Standardized findings require non-empty `finding_id` values.", call. = FALSE)
  }
  litxr_validate_paper_type(dt$paper_type)
  invisible(TRUE)
}

#' Write standardized findings rows to the project delta store
#'
#' @param findings Data frame, data.table, or named list of standardized
#'   findings rows.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the delta fst path.
#' @export
litxr_write_standardized_findings <- function(findings, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  rows <- .litxr_normalize_standardized_findings(findings)
  litxr_validate_standardized_findings(rows)
  .litxr_ensure_project_findings_dir(cfg)
  paths <- .litxr_standardized_findings_paths(cfg)
  existing <- .litxr_read_fst_or_empty(paths$delta, .litxr_empty_standardized_findings)
  merged <- .litxr_upsert_table_by_key(existing, rows, c("ref_id", "finding_id"))
  .litxr_write_fst_atomic(as.data.frame(merged), paths$delta)
  invisible(paths$delta)
}

#' Read standardized findings from main and delta stores
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param ref_ids Optional character vector of `ref_id` values to keep.
#'
#' @return `data.table` of standardized findings.
#' @export
litxr_read_standardized_findings <- function(config = NULL, ref_ids = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  paths <- .litxr_standardized_findings_paths(cfg)
  main <- .litxr_read_fst_or_empty(paths$main, .litxr_empty_standardized_findings)
  delta <- .litxr_read_fst_or_empty(paths$delta, .litxr_empty_standardized_findings)
  out <- .litxr_upsert_table_by_key(main, delta, c("ref_id", "finding_id"))
  out <- .litxr_normalize_standardized_findings(out)
  if (!is.null(ref_ids) && length(ref_ids)) {
    out <- out[out$ref_id %in% as.character(ref_ids), ]
  }
  out
}

#' Find standardized findings rows
#'
#' @param query Optional substring query over text fields.
#' @param ref_id Optional exact `ref_id` filter.
#' @param finding_id Optional exact `finding_id` filter.
#' @param paper_type Optional paper-type filter.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Filtered `data.table`.
#' @export
litxr_find_standardized_findings <- function(query = NULL, ref_id = NULL, finding_id = NULL, paper_type = NULL, config = NULL) {
  out <- litxr_read_standardized_findings(config = config, ref_ids = ref_id)
  if (!nrow(out)) {
    return(out)
  }
  if (!is.null(finding_id) && length(finding_id)) {
    out <- out[out$finding_id %in% as.character(finding_id), ]
  }
  if (!is.null(paper_type) && length(paper_type)) {
    keep_types <- litxr_normalize_paper_type(paper_type)
    out <- out[out$paper_type %in% keep_types, ]
  }
  if (!is.null(query) && nzchar(as.character(query[[1]]))) {
    text <- paste(
      out$research_question,
      out$hypothesis,
      out$outcome_variable,
      out$treatment_variable,
      out$moderator_variable,
      out$method,
      out$sample,
      out$sample_period,
      out$sample_region,
      out$effect_direction,
      out$model_specification,
      out$finding_text,
      out$evidence_strength,
      out$notes
    )
    out <- out[.litxr_research_query_keep(text, query), ]
  }
  out
}

#' Compact standardized findings delta into the main store
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the main fst path.
#' @export
litxr_compact_standardized_findings <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_ensure_project_findings_dir(cfg)
  paths <- .litxr_standardized_findings_paths(cfg)
  main <- .litxr_read_fst_or_empty(paths$main, .litxr_empty_standardized_findings)
  delta <- .litxr_read_fst_or_empty(paths$delta, .litxr_empty_standardized_findings)
  merged <- .litxr_upsert_table_by_key(
    .litxr_normalize_standardized_findings(main),
    .litxr_normalize_standardized_findings(delta),
    c("ref_id", "finding_id")
  )
  .litxr_write_fst_atomic(as.data.frame(merged), paths$main)
  if (file.exists(paths$delta)) {
    unlink(paths$delta)
  }
  invisible(paths$main)
}

#' Template row for descriptive statistics
#'
#' @return One-row `data.table` with the descriptive statistics schema.
#' @export
litxr_descriptive_stats_template <- function() {
  data.table::data.table(
    ref_id = "",
    table_id = "",
    variable = "",
    label = NA_character_,
    n = NA_real_,
    mean = NA_real_,
    sd = NA_real_,
    min = NA_real_,
    p25 = NA_real_,
    median = NA_real_,
    p75 = NA_real_,
    max = NA_real_,
    unit = NA_character_,
    sample = NA_character_,
    sample_period = NA_character_,
    source_table = NA_character_,
    extraction_method = NA_character_,
    extracted_at = NA_character_,
    notes = NA_character_
  )
}

#' Validate descriptive statistics rows
#'
#' @param stats Data frame, data.table, or named list of descriptive-statistics
#'   rows.
#'
#' @return Invisibly returns `TRUE` on success.
#' @export
litxr_validate_descriptive_stats <- function(stats) {
  dt <- .litxr_normalize_descriptive_stats(stats)
  bad_ref <- is.na(dt$ref_id) | !nzchar(trimws(dt$ref_id))
  if (any(bad_ref)) {
    stop("Descriptive statistics require non-empty `ref_id` values.", call. = FALSE)
  }
  bad_table <- is.na(dt$table_id) | !nzchar(trimws(dt$table_id))
  if (any(bad_table)) {
    stop("Descriptive statistics require non-empty `table_id` values.", call. = FALSE)
  }
  bad_var <- is.na(dt$variable) | !nzchar(trimws(dt$variable))
  if (any(bad_var)) {
    stop("Descriptive statistics require non-empty `variable` values.", call. = FALSE)
  }
  invisible(TRUE)
}

#' Write descriptive statistics rows to the project delta store
#'
#' @param stats Data frame, data.table, or named list of descriptive-statistics
#'   rows.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the delta fst path.
#' @export
litxr_write_descriptive_stats <- function(stats, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  rows <- .litxr_normalize_descriptive_stats(stats)
  litxr_validate_descriptive_stats(rows)
  .litxr_ensure_project_findings_dir(cfg)
  paths <- .litxr_descriptive_stats_paths(cfg)
  existing <- .litxr_read_fst_or_empty(paths$delta, .litxr_empty_descriptive_stats)
  merged <- .litxr_upsert_table_by_key(existing, rows, c("ref_id", "table_id", "variable"))
  .litxr_write_fst_atomic(as.data.frame(merged), paths$delta)
  invisible(paths$delta)
}

#' Read descriptive statistics from main and delta stores
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param ref_ids Optional character vector of `ref_id` values to keep.
#'
#' @return `data.table` of descriptive statistics.
#' @export
litxr_read_descriptive_stats <- function(config = NULL, ref_ids = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  paths <- .litxr_descriptive_stats_paths(cfg)
  main <- .litxr_read_fst_or_empty(paths$main, .litxr_empty_descriptive_stats)
  delta <- .litxr_read_fst_or_empty(paths$delta, .litxr_empty_descriptive_stats)
  out <- .litxr_upsert_table_by_key(main, delta, c("ref_id", "table_id", "variable"))
  out <- .litxr_normalize_descriptive_stats(out)
  if (!is.null(ref_ids) && length(ref_ids)) {
    out <- out[out$ref_id %in% as.character(ref_ids), ]
  }
  out
}

#' Find descriptive statistics rows
#'
#' @param query Optional substring query over text fields.
#' @param ref_id Optional exact `ref_id` filter.
#' @param table_id Optional exact `table_id` filter.
#' @param variable Optional exact `variable` filter.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Filtered `data.table`.
#' @export
litxr_find_descriptive_stats <- function(query = NULL, ref_id = NULL, table_id = NULL, variable = NULL, config = NULL) {
  out <- litxr_read_descriptive_stats(config = config, ref_ids = ref_id)
  if (!nrow(out)) {
    return(out)
  }
  if (!is.null(table_id) && length(table_id)) {
    out <- out[out$table_id %in% as.character(table_id), ]
  }
  if (!is.null(variable) && length(variable)) {
    out <- out[out$variable %in% as.character(variable), ]
  }
  if (!is.null(query) && nzchar(as.character(query[[1]]))) {
    text <- paste(
      out$variable,
      out$label,
      out$unit,
      out$sample,
      out$sample_period,
      out$source_table,
      out$notes
    )
    out <- out[.litxr_research_query_keep(text, query), ]
  }
  out
}

#' Compact descriptive statistics delta into the main store
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the main fst path.
#' @export
litxr_compact_descriptive_stats <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_ensure_project_findings_dir(cfg)
  paths <- .litxr_descriptive_stats_paths(cfg)
  main <- .litxr_read_fst_or_empty(paths$main, .litxr_empty_descriptive_stats)
  delta <- .litxr_read_fst_or_empty(paths$delta, .litxr_empty_descriptive_stats)
  merged <- .litxr_upsert_table_by_key(
    .litxr_normalize_descriptive_stats(main),
    .litxr_normalize_descriptive_stats(delta),
    c("ref_id", "table_id", "variable")
  )
  .litxr_write_fst_atomic(as.data.frame(merged), paths$main)
  if (file.exists(paths$delta)) {
    unlink(paths$delta)
  }
  invisible(paths$main)
}

#' Rebuild standardized findings main store from current local tables
#'
#' Reads the current main and delta tables, normalizes and deduplicates them,
#' rewrites the main fst, and clears the delta fst.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the rebuilt main fst path.
#' @export
litxr_rebuild_standardized_findings <- function(config = NULL) {
  litxr_compact_standardized_findings(config = config)
}

#' Rebuild descriptive statistics main store from current local tables
#'
#' Reads the current main and delta tables, normalizes and deduplicates them,
#' rewrites the main fst, and clears the delta fst.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the rebuilt main fst path.
#' @export
litxr_rebuild_descriptive_stats <- function(config = NULL) {
  litxr_compact_descriptive_stats(config = config)
}

#' Read project-level research schema coverage
#'
#' Summarizes markdown, LLM digest, standardized findings, and descriptive
#' statistics coverage for canonical references.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param collection_id Optional collection membership filter.
#' @param ref_ids Optional character vector of reference ids to keep.
#'
#' @return `data.table` with one row per reference and research-schema coverage
#'   flags and counts.
#' @export
litxr_read_research_schema_status <- function(config = NULL, collection_id = NULL, ref_ids = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  refs <- litxr_read_references(cfg)
  if (!nrow(refs)) {
    return(data.table::data.table(
      ref_id = character(),
      title = character(),
      entry_type = character(),
      collection_ids = character(),
      has_md = logical(),
      has_llm_digest = logical(),
      llm_schema_version = character(),
      llm_paper_type = character(),
      has_standardized_findings = logical(),
      n_standardized_findings = integer(),
      has_descriptive_stats = logical(),
      n_descriptive_stats = integer()
    ))
  }

  links <- litxr_read_reference_collections(cfg)
  status <- litxr_read_enrichment_status(cfg)
  digests <- litxr_read_llm_digests(cfg)
  findings <- litxr_read_standardized_findings(cfg)
  desc_stats <- litxr_read_descriptive_stats(cfg)

  collection_map <- if (nrow(links)) {
    split_ids <- split(as.character(links$collection_id), as.character(links$ref_id))
    data.table::data.table(
      ref_id = names(split_ids),
      collection_ids = vapply(split_ids, function(x) paste(sort(unique(x)), collapse = ","), character(1))
    )
  } else {
    data.table::data.table(ref_id = character(), collection_ids = character())
  }

  finding_counts <- if (nrow(findings)) {
    counts <- table(as.character(findings$ref_id))
    data.table::data.table(
      ref_id = names(counts),
      n_standardized_findings = as.integer(counts)
    )
  } else {
    data.table::data.table(ref_id = character(), n_standardized_findings = integer())
  }
  desc_counts <- if (nrow(desc_stats)) {
    counts <- table(as.character(desc_stats$ref_id))
    data.table::data.table(
      ref_id = names(counts),
      n_descriptive_stats = as.integer(counts)
    )
  } else {
    data.table::data.table(ref_id = character(), n_descriptive_stats = integer())
  }

  out <- data.table::data.table(
    ref_id = as.character(refs$ref_id),
    title = if ("title" %in% names(refs)) as.character(refs$title) else NA_character_,
    entry_type = if ("entry_type" %in% names(refs)) as.character(refs$entry_type) else NA_character_
  )
  out <- merge(out, collection_map, by = "ref_id", all.x = TRUE)
  out <- merge(
    out,
    data.table::data.table(
      ref_id = as.character(status$ref_id),
      has_md = as.logical(status$has_md),
      has_llm_digest = as.logical(status$has_llm_digest)
    ),
    by = "ref_id",
    all.x = TRUE
  )
  if (nrow(digests)) {
    out <- merge(
      out,
      data.table::data.table(
        ref_id = as.character(digests$ref_id),
        llm_schema_version = as.character(digests$schema_version),
        llm_paper_type = as.character(digests$paper_type)
      ),
      by = "ref_id",
      all.x = TRUE
    )
  } else {
    out$llm_schema_version <- NA_character_
    out$llm_paper_type <- NA_character_
  }
  out <- merge(out, finding_counts, by = "ref_id", all.x = TRUE)
  out <- merge(out, desc_counts, by = "ref_id", all.x = TRUE)

  out$collection_ids[is.na(out$collection_ids)] <- ""
  out$has_md[is.na(out$has_md)] <- FALSE
  out$has_llm_digest[is.na(out$has_llm_digest)] <- FALSE
  out$n_standardized_findings[is.na(out$n_standardized_findings)] <- 0L
  out$n_descriptive_stats[is.na(out$n_descriptive_stats)] <- 0L
  out$has_standardized_findings <- out$n_standardized_findings > 0L
  out$has_descriptive_stats <- out$n_descriptive_stats > 0L

  if (!is.null(collection_id) && nzchar(as.character(collection_id[[1]]))) {
    keep_id <- as.character(collection_id[[1]])
    out <- out[vapply(strsplit(out$collection_ids, ",", fixed = TRUE), function(x) keep_id %in% x, logical(1)), ]
  }
  if (!is.null(ref_ids) && length(ref_ids)) {
    out <- out[out$ref_id %in% as.character(ref_ids), ]
  }

  out[, c(
    "ref_id", "title", "entry_type", "collection_ids",
    "has_md", "has_llm_digest", "llm_schema_version", "llm_paper_type",
    "has_standardized_findings", "n_standardized_findings",
    "has_descriptive_stats", "n_descriptive_stats"
  ), with = FALSE]
}

.litxr_filter_missing_research_status <- function(status_dt, missing_type) {
  switch(
    missing_type,
    llm_digest = status_dt[!status_dt$has_llm_digest, ],
    standardized_findings = status_dt[!status_dt$has_standardized_findings, ],
    descriptive_stats = status_dt[!status_dt$has_descriptive_stats, ],
    stop("Unsupported missing type: ", missing_type, call. = FALSE)
  )
}

#' Find references missing LLM digests
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param collection_id Optional collection membership filter.
#' @param ref_ids Optional character vector of reference ids to keep.
#'
#' @return Filtered `data.table` from `litxr_read_research_schema_status()`.
#' @export
litxr_find_refs_missing_llm_digest <- function(config = NULL, collection_id = NULL, ref_ids = NULL) {
  .litxr_filter_missing_research_status(
    litxr_read_research_schema_status(config = config, collection_id = collection_id, ref_ids = ref_ids),
    "llm_digest"
  )
}

#' Find references missing standardized findings
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param collection_id Optional collection membership filter.
#' @param ref_ids Optional character vector of reference ids to keep.
#'
#' @return Filtered `data.table` from `litxr_read_research_schema_status()`.
#' @export
litxr_find_refs_missing_standardized_findings <- function(config = NULL, collection_id = NULL, ref_ids = NULL) {
  .litxr_filter_missing_research_status(
    litxr_read_research_schema_status(config = config, collection_id = collection_id, ref_ids = ref_ids),
    "standardized_findings"
  )
}

#' Find references missing descriptive statistics
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param collection_id Optional collection membership filter.
#' @param ref_ids Optional character vector of reference ids to keep.
#'
#' @return Filtered `data.table` from `litxr_read_research_schema_status()`.
#' @export
litxr_find_refs_missing_descriptive_stats <- function(config = NULL, collection_id = NULL, ref_ids = NULL) {
  .litxr_filter_missing_research_status(
    litxr_read_research_schema_status(config = config, collection_id = collection_id, ref_ids = ref_ids),
    "descriptive_stats"
  )
}

#' Upgrade one or more LLM digests to the current v2 schema
#'
#' Reads existing digest JSON files and rewrites them through the current digest
#' write path so legacy v1 digests become explicit schema-v2 payloads.
#'
#' @param ref_ids Character vector of reference ids to upgrade. When omitted,
#'   all existing project-level digests are upgraded.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the upgraded ref ids.
#' @export
litxr_upgrade_llm_digests <- function(ref_ids = NULL, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()

  targets <- if (!is.null(ref_ids) && length(ref_ids)) {
    unique(as.character(ref_ids))
  } else {
    digests <- litxr_read_llm_digests(cfg)
    unique(as.character(digests$ref_id))
  }
  targets <- targets[!is.na(targets) & nzchar(targets)]
  if (!length(targets)) {
    return(invisible(character()))
  }

  for (ref_id in targets) {
    digest <- litxr_read_llm_digest(ref_id, cfg)
    if (is.null(digest)) next
    if (!.litxr_llm_digest_is_v2(digest)) {
      digest$schema_version <- "v2"
      digest$extraction_mode <- "legacy"
      litxr_write_llm_digest(ref_id, digest, cfg, keep_history = FALSE, bump_revision = FALSE)
    }
  }

  invisible(targets)
}
