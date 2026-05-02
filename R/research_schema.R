#' Canonical paper type levels
#'
#' @return Character vector of canonical paper-type levels.
#' @export
litxr_paper_type_levels <- function() {
  c(
    "theoretical",
    "conceptual",
    "empirical_archival",
    "empirical_experimental",
    "empirical_survey",
    "empirical_qualitative",
    "empirical_case_study",
    "empirical_mixed_methods",
    "methodological",
    "computational",
    "simulation",
    "benchmark",
    "system_design",
    "review_narrative",
    "review_systematic",
    "review_scoping",
    "meta_analysis",
    "replication",
    "registered_report",
    "study_protocol",
    "policy_analysis",
    "perspective",
    "commentary",
    "review",
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
    conceptual_framework = "conceptual",
    conceptual_paper = "conceptual",
    conceptual_model = "conceptual",
    archival = "empirical_archival",
    archive = "empirical_archival",
    empirical_archive = "empirical_archival",
    empirical_archival_study = "empirical_archival",
    archival_empirical = "empirical_archival",
    archival_study = "empirical_archival",
    observational = "empirical_archival",
    experimental = "empirical_experimental",
    experiment = "empirical_experimental",
    empirical_experiment = "empirical_experimental",
    randomized_experiment = "empirical_experimental",
    field_experiment = "empirical_experimental",
    lab_experiment = "empirical_experimental",
    survey = "empirical_survey",
    survey_based = "empirical_survey",
    questionnaire = "empirical_survey",
    survey_research = "empirical_survey",
    qualitative = "empirical_qualitative",
    interview = "empirical_qualitative",
    ethnography = "empirical_qualitative",
    grounded_theory = "empirical_qualitative",
    fieldwork = "empirical_qualitative",
    discourse_analysis = "empirical_qualitative",
    case = "empirical_case_study",
    case_study = "empirical_case_study",
    empirical_case = "empirical_case_study",
    case_analysis = "empirical_case_study",
    mixed = "empirical_mixed_methods",
    mixed_method = "empirical_mixed_methods",
    mixed_methods = "empirical_mixed_methods",
    method = "methodological",
    methods = "methodological",
    methodology = "methodological",
    methodological_paper = "methodological",
    measurement_method = "methodological",
    computation = "computational",
    computational_modeling = "computational",
    machine_learning = "computational",
    ml = "computational",
    algorithmic = "computational",
    simulate = "simulation",
    simulation_study = "simulation",
    monte_carlo = "simulation",
    montecarlo = "simulation",
    agent_based_model = "simulation",
    scenario_simulation = "simulation",
    benchmarking = "benchmark",
    benchmark_study = "benchmark",
    leaderboard = "benchmark",
    evaluation_benchmark = "benchmark",
    task_benchmark = "benchmark",
    architecture = "system_design",
    system_architecture = "system_design",
    system_design_paper = "system_design",
    platform_design = "system_design",
    implementation = "system_design",
    narrative_review = "review_narrative",
    literature_review = "review_narrative",
    review_article = "review_narrative",
    overview = "review_narrative",
    systematic_review = "review_systematic",
    systematic_literature_review = "review_systematic",
    scoping_review = "review_scoping",
    mapping_review = "review_scoping",
    evidence_map = "review_scoping",
    meta = "meta_analysis",
    meta_analytic = "meta_analysis",
    meta_analysis = "meta_analysis",
    replication = "replication",
    reproduction = "replication",
    reproducibility = "replication",
    replicated = "replication",
    replication_study = "replication",
    reanalysis = "replication",
    preregistered = "registered_report",
    pre_registered = "registered_report",
    pre_registered_report = "registered_report",
    registered_report = "registered_report",
    stage1 = "registered_report",
    protocol = "study_protocol",
    trial_protocol = "study_protocol",
    review_protocol = "study_protocol",
    study_design = "study_protocol",
    policy_analysis = "policy_analysis",
    data = "dataset",
    data_paper = "dataset",
    database = "dataset",
    dataset_paper = "dataset",
    report = "policy_report",
    policy = "policy_analysis",
    policy_brief = "policy_analysis",
    policy_study = "policy_analysis",
    policy_paper = "policy_analysis",
    governance_analysis = "policy_analysis",
    regulatory_analysis = "policy_analysis",
    regulation_analysis = "policy_analysis",
    perspective = "perspective",
    viewpoint = "perspective",
    opinion = "commentary",
    commentary = "commentary",
    comment = "commentary",
    response = "commentary",
    editorial = "commentary",
    letter = "commentary",
    note = "commentary",
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

.litxr_llm_digest_template_v3 <- function(ref_id) {
  x <- .litxr_llm_digest_template_v2(ref_id)
  x$schema_version <- "v3"
  x$anchor_references <- list()
  x$citation_logic_nodes <- list()
  x
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

.litxr_has_inline_llm_tables <- function(digest) {
  any(c("anchor_references", "citation_logic_nodes") %in% names(digest))
}

.litxr_inline_llm_table_legacy_field_names <- function(field_name) {
  if (identical(field_name, "anchor_references")) {
    return(c("citation_key", "anchor_title", "reason", "relationship_to_current_paper"))
  }
  if (identical(field_name, "citation_logic_nodes")) {
    return(c("logic_type", "claim_sentence"))
  }
  character()
}

.litxr_inline_llm_table_legacy_transpose <- function(value, field_name, ref_id = NULL) {
  row_names <- .litxr_inline_llm_table_legacy_field_names(field_name)
  if (!length(row_names) || !is.list(value) || !length(value)) {
    return(data.table::data.table())
  }
  v_names <- unique(unlist(lapply(value, function(x) names(x)[grepl("^V\\d+$", names(x))]), use.names = FALSE))
  v_names <- sort(v_names, na.last = TRUE)
  if (!length(v_names)) {
    return(data.table::data.table())
  }
  ref_value <- as.character(.litxr_first_nonnull(ref_id, value[[1]]$ref_id, ""))
  rows <- lapply(v_names, function(v_name) {
    row <- list(ref_id = ref_value)
    if (identical(field_name, "anchor_references")) {
      row$anchor_rank <- suppressWarnings(as.integer(sub("^V", "", v_name)))
      row$citation_key <- as.character(value[[1]][[v_name]] %||% NA_character_)
      row$anchor_title <- as.character(value[[2]][[v_name]] %||% NA_character_)
      row$reason <- as.character(value[[3]][[v_name]] %||% NA_character_)
      row$relationship_to_current_paper <- as.character(value[[4]][[v_name]] %||% NA_character_)
      row$anchor_authors <- NA_character_
      row$anchor_year <- NA_integer_
      row$anchor_ref_id <- NA_character_
      row$anchor_role <- NA_character_
      row$confidence <- NA_character_
      row$created_at <- NA_character_
      row$updated_at <- NA_character_
    } else if (identical(field_name, "citation_logic_nodes")) {
      row$node_id <- paste0("node_", sub("^V", "", v_name))
      row$logic_type <- as.character(value[[1]][[v_name]] %||% NA_character_)
      row$claim_sentence <- as.character(value[[2]][[v_name]] %||% NA_character_)
      row$subject_text <- NA_character_
      row$object_text <- NA_character_
      row$modifier_text <- NA_character_
      row$evidence_role <- NA_character_
      row$confidence <- NA_character_
      row$page_or_section <- NA_character_
      row$quote_support <- NA_character_
      row$citation_use <- NA_character_
      row$tags <- NA_character_
      row$created_at <- NA_character_
      row$updated_at <- NA_character_
    }
    row
  })
  dt <- data.table::rbindlist(rows, fill = TRUE)
  if (identical(field_name, "anchor_references")) {
    dt <- .litxr_align_columns(dt, .litxr_empty_anchor_references())
    dt <- .litxr_normalize_anchor_references(dt)
  } else if (identical(field_name, "citation_logic_nodes")) {
    dt <- .litxr_align_columns(dt, .litxr_empty_citation_logic_nodes())
    dt <- .litxr_normalize_citation_logic_nodes(dt)
  }
  dt
}

.litxr_inline_llm_table_to_dt <- function(value, ref_id = NULL, field_name = NULL) {
  if (is.null(value) || !length(value)) {
    return(data.table::data.table())
  }
  if (!is.null(field_name) && is.list(value) && length(value) && all(vapply(value, is.list, logical(1L)))) {
    inner_names <- unique(unlist(lapply(value, names), use.names = FALSE))
    if (any(grepl("^V\\d+$", inner_names, perl = TRUE)) && !any(inner_names %in% c(
      "anchor_rank", "citation_key", "anchor_title", "anchor_authors", "anchor_year",
      "anchor_ref_id", "anchor_role", "reason", "relationship_to_current_paper",
      "confidence", "created_at", "updated_at", "node_id", "claim_sentence",
      "logic_type", "subject_text", "object_text", "modifier_text", "evidence_role",
      "page_or_section", "quote_support", "citation_use", "tags"
    ))) {
      return(.litxr_inline_llm_table_legacy_transpose(value, field_name, ref_id = ref_id))
    }
  }
  dt <- .litxr_as_research_dt(value)
  dt <- data.table::copy(dt)
  if (!is.null(field_name) && nrow(dt) > 0L) {
    raw_names <- setdiff(names(dt), "ref_id")
    if (length(raw_names) && all(grepl("^V\\d+$", raw_names, perl = TRUE)) && !any(raw_names %in% c(
      "anchor_rank", "citation_key", "anchor_title", "anchor_authors", "anchor_year",
      "anchor_ref_id", "anchor_role", "reason", "relationship_to_current_paper",
      "confidence", "created_at", "updated_at", "node_id", "claim_sentence",
      "logic_type", "subject_text", "object_text", "modifier_text", "evidence_role",
      "page_or_section", "quote_support", "citation_use", "tags"
    ))) {
      rows <- lapply(seq_len(nrow(dt)), function(i) as.list(dt[i, , drop = FALSE]))
      return(.litxr_inline_llm_table_legacy_transpose(rows, field_name, ref_id = ref_id))
    }
  }
  if (!("ref_id" %in% names(dt))) {
    dt$ref_id <- as.character(ref_id %||% "")
  } else {
    dt$ref_id <- as.character(dt$ref_id)
    dt$ref_id[is.na(dt$ref_id) | !nzchar(trimws(dt$ref_id))] <- as.character(ref_id %||% "")
  }
  data.table::setcolorder(dt, c("ref_id", setdiff(names(dt), "ref_id")))
  dt
}

.litxr_normalize_llm_digest_for_write <- function(digest, ref_id = NULL) {
  version <- if (is.null(digest[["schema_version"]]) || !length(digest[["schema_version"]])) {
    if (.litxr_has_inline_llm_tables(digest)) "v3" else "v2"
  } else {
    .litxr_llm_digest_schema_version(digest)
  }
  template_fun <- if (identical(version, "v3") || .litxr_has_inline_llm_tables(digest)) {
    .litxr_llm_digest_template_v3
  } else {
    .litxr_llm_digest_template_v2
  }
  payload <- template_fun(.litxr_first_nonnull(ref_id, digest$ref_id, ""))
  if (!identical(version, "v2") && !is.null(digest[["schema_version"]]) && length(digest[["schema_version"]])) {
    payload[["schema_version"]] <- as.character(digest[["schema_version"]][[1]])
  }

  for (name in intersect(names(payload), names(digest))) {
    payload[[name]] <- digest[[name]]
  }
  payload$ref_id <- as.character(.litxr_first_nonnull(ref_id, payload$ref_id, ""))
  payload$paper_type <- litxr_normalize_paper_type(.litxr_first_nonnull(payload$paper_type, NA_character_))
  if ("anchor_references" %in% names(payload) || "anchor_references" %in% names(digest)) {
    payload$anchor_references <- .litxr_inline_llm_table_to_dt(
      .litxr_first_nonnull(digest$anchor_references, payload$anchor_references),
      ref_id = payload$ref_id,
      field_name = "anchor_references"
    )
  }
  if ("citation_logic_nodes" %in% names(payload) || "citation_logic_nodes" %in% names(digest)) {
    payload$citation_logic_nodes <- .litxr_inline_llm_table_to_dt(
      .litxr_first_nonnull(digest$citation_logic_nodes, payload$citation_logic_nodes),
      ref_id = payload$ref_id,
      field_name = "citation_logic_nodes"
    )
  }
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
  if (identical(.litxr_llm_digest_schema_version(digest), "v2") || identical(.litxr_llm_digest_schema_version(digest), "v3")) {
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
    if (!("anchor_references" %in% names(digest))) {
      digest$anchor_references <- list()
    } else if (identical(.litxr_llm_digest_schema_version(digest), "v3")) {
      digest$anchor_references <- .litxr_inline_llm_table_to_dt(
        digest$anchor_references,
        ref_id = digest$ref_id %||% NULL,
        field_name = "anchor_references"
      )
    }
    if (!("citation_logic_nodes" %in% names(digest))) {
      digest$citation_logic_nodes <- list()
    } else if (identical(.litxr_llm_digest_schema_version(digest), "v3")) {
      digest$citation_logic_nodes <- .litxr_inline_llm_table_to_dt(
        digest$citation_logic_nodes,
        ref_id = digest$ref_id %||% NULL,
        field_name = "citation_logic_nodes"
      )
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

.litxr_llm_digest_v3_optional_empirical_fields <- function() {
  c(
    "identification_strategy",
    "empirical_setting",
    "descriptive_statistics_summary",
    "standardized_findings_summary"
  )
}

.litxr_llm_digest_empirical_paper_type <- function(paper_type) {
  paper_type <- litxr_normalize_paper_type(paper_type)
  nzchar(paper_type) & startsWith(paper_type, "empirical_")
}

.litxr_llm_digest_field_is_present <- function(value) {
  if (is.null(value) || !length(value)) {
    return(FALSE)
  }
  if (is.list(value)) {
    if (!length(value)) {
      return(FALSE)
    }
    if (length(value) == 1L) {
      return(.litxr_llm_digest_field_is_present(value[[1]]))
    }
    return(any(vapply(value, .litxr_llm_digest_field_is_present, logical(1L))))
  }
  if (is.character(value)) {
    value <- value[!is.na(value)]
    if (!length(value)) {
      return(FALSE)
    }
    return(any(nzchar(trimws(value))))
  }
  if (is.numeric(value) || is.integer(value)) {
    value <- value[!is.na(value)]
    return(length(value) > 0L)
  }
  TRUE
}

.litxr_warn_v3_missing_empirical_fields <- function(digest) {
  if (!.litxr_llm_digest_empirical_paper_type(digest$paper_type)) {
    return(invisible(TRUE))
  }
  optional <- .litxr_llm_digest_v3_optional_empirical_fields()
  missing <- optional[!vapply(optional, function(field) .litxr_llm_digest_field_is_present(digest[[field]]), logical(1L))]
  if (length(missing)) {
    warning(
      "LLM digest paper_type `",
      litxr_normalize_paper_type(digest$paper_type),
      "` suggests empirical fields, but the following optional fields are missing or empty: ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }
  invisible(TRUE)
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
  if (identical(schema_version, "v3")) {
    return(c(
      "schema_version", "ref_id", "digest_revision",
      "extraction_mode", "prompt_version", "model_hint", "paper_type", "summary", "motivation",
      "research_questions", "paper_structure", "methods", "research_data",
      "main_variables", "key_findings",
      "limitations", "theoretical_mechanism",
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

.litxr_validate_inline_llm_table_field <- function(value, field_name, validator, ref_id = NULL) {
  if (is.null(value) || !length(value)) {
    return(invisible(TRUE))
  }
  dt <- .litxr_inline_llm_table_to_dt(value)
  if (!nrow(dt)) {
    return(invisible(TRUE))
  }
  if ("ref_id" %in% names(dt)) {
    bad_ref <- is.na(dt$ref_id) | !nzchar(trimws(dt$ref_id))
    if (any(bad_ref) && !is.null(ref_id) && nzchar(as.character(ref_id[[1]]))) {
      dt$ref_id[bad_ref] <- as.character(ref_id[[1]])
      bad_ref <- is.na(dt$ref_id) | !nzchar(trimws(dt$ref_id))
    }
    if (any(bad_ref)) {
      stop("LLM digest field `", field_name, "` contains empty `ref_id` values.", call. = FALSE)
    }
  }
  if (identical(field_name, "anchor_references")) {
    dt <- .litxr_normalize_anchor_references(dt)
  } else if (identical(field_name, "citation_logic_nodes")) {
    dt <- .litxr_normalize_citation_logic_nodes(dt)
  }
  if (is.function(validator)) {
    invisible(TRUE)
  }
  invisible(TRUE)
}

.litxr_validate_inline_llm_table_field <- function(value, field_name, validator, ref_id = NULL) {
  if (is.null(value) || !length(value)) {
    return(invisible(TRUE))
  }
  dt <- .litxr_inline_llm_table_to_dt(value)
  if (!nrow(dt)) {
    return(invisible(TRUE))
  }
  if ("ref_id" %in% names(dt)) {
    bad_ref <- is.na(dt$ref_id) | !nzchar(trimws(dt$ref_id))
    if (any(bad_ref) && !is.null(ref_id) && nzchar(as.character(ref_id[[1]]))) {
      dt$ref_id[bad_ref] <- as.character(ref_id[[1]])
      bad_ref <- is.na(dt$ref_id) | !nzchar(trimws(dt$ref_id))
    }
    if (any(bad_ref)) {
      stop("LLM digest field `", field_name, "` contains empty `ref_id` values.", call. = FALSE)
    }
  }
  invisible(TRUE)
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

.litxr_anchor_references_paths <- function(cfg) {
  list(
    main = file.path(.litxr_project_findings_dir(cfg), "anchor_references.fst"),
    delta = file.path(.litxr_project_findings_dir(cfg), "anchor_references_delta.fst")
  )
}

.litxr_citation_logic_nodes_paths <- function(cfg) {
  list(
    main = file.path(.litxr_project_findings_dir(cfg), "citation_logic_nodes.fst"),
    delta = file.path(.litxr_project_findings_dir(cfg), "citation_logic_nodes_delta.fst")
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

.litxr_empty_anchor_references <- function() {
  data.table::data.table(
    ref_id = character(),
    anchor_rank = integer(),
    citation_key = character(),
    anchor_title = character(),
    anchor_authors = character(),
    anchor_year = integer(),
    anchor_ref_id = character(),
    anchor_role = character(),
    reason = character(),
    relationship_to_current_paper = character(),
    confidence = character(),
    created_at = character(),
    updated_at = character()
  )
}

.litxr_empty_citation_logic_nodes <- function() {
  data.table::data.table(
    ref_id = character(),
    node_id = character(),
    claim_sentence = character(),
    logic_type = character(),
    subject_text = character(),
    object_text = character(),
    modifier_text = character(),
    evidence_role = character(),
    confidence = character(),
    page_or_section = character(),
    quote_support = character(),
    citation_use = character(),
    tags = character(),
    created_at = character(),
    updated_at = character()
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

.litxr_scalar_or_default <- function(x, default = NA_character_) {
  if (is.null(x) || !length(x)) {
    return(default)
  }
  value <- as.character(x[[1]])
  if (is.na(value) || !nzchar(trimws(value))) {
    return(default)
  }
  value
}

.litxr_collapse_chr <- function(x) {
  if (is.null(x) || !length(x)) {
    return(NA_character_)
  }
  if (is.list(x)) {
    x <- unlist(x, use.names = FALSE)
  }
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(trimws(x))]
  if (!length(x)) {
    return(NA_character_)
  }
  paste(unique(trimws(x)), collapse = "; ")
}

.litxr_key_key <- function(x) {
  tolower(trimws(as.character(x)))
}

.litxr_normalize_anchor_reference_role <- function(x) {
  map <- c(
    theoretical_foundation = "theoretical_foundation",
    conceptual_foundation = "conceptual_foundation",
    methodological_foundation = "methodological_foundation",
    empirical_benchmark = "empirical_benchmark",
    main_comparison = "main_comparison",
    motivation = "motivation",
    contrasting_view = "contrasting_view",
    problem_origin = "problem_origin",
    application_context = "application_context",
    review_anchor = "review_anchor",
    policy_context = "policy_context",
    unknown = "unknown",
    theoretical = "theoretical_foundation",
    theory = "theoretical_foundation",
    conceptual = "conceptual_foundation",
    conceptual_framework = "conceptual_foundation",
    methodological = "methodological_foundation",
    method = "methodological_foundation",
    empirical = "empirical_benchmark",
    benchmark = "empirical_benchmark",
    main = "main_comparison",
    comparison = "main_comparison",
    main_comparison = "main_comparison",
    motivation = "motivation",
    contrasting = "contrasting_view",
    contrast = "contrasting_view",
    contrasting_view = "contrasting_view",
    problem = "problem_origin",
    problem_origin = "problem_origin",
    application = "application_context",
    application_context = "application_context",
    review = "review_anchor",
    review_anchor = "review_anchor",
    policy = "policy_context",
    policy_context = "policy_context",
    unknown = "unknown"
  )
  key <- .litxr_key_key(x)
  out <- unname(map[key])
  out[is.na(out) | !nzchar(out)] <- "unknown"
  out
}

.litxr_validate_anchor_reference_role <- function(x) {
  normalized <- .litxr_normalize_anchor_reference_role(x)
  bad <- !(normalized %in% c(
    "theoretical_foundation", "conceptual_foundation", "methodological_foundation",
    "empirical_benchmark", "main_comparison", "motivation", "contrasting_view",
    "problem_origin", "application_context", "review_anchor", "policy_context", "unknown"
  ))
  if (any(bad)) {
    stop("Invalid anchor role value(s): ", paste(unique(as.character(x[bad])), collapse = ", "), call. = FALSE)
  }
  invisible(TRUE)
}

.litxr_normalize_relationship_to_current_paper <- function(x) {
  map <- c(
    builds_on = "builds_on",
    extends = "extends",
    tests = "tests",
    applies = "applies",
    compares_with = "compares_with",
    contradicts = "contradicts",
    critiques = "critiques",
    replicates = "replicates",
    generalizes = "generalizes",
    narrows = "narrows",
    uses_as_context = "uses_as_context",
    unknown = "unknown",
    builds_on = "builds_on",
    build_on = "builds_on",
    extends = "extends",
    extension = "extends",
    tests = "tests",
    test = "tests",
    applies = "applies",
    apply = "applies",
    compares_with = "compares_with",
    compare_with = "compares_with",
    contradicts = "contradicts",
    contradict = "contradicts",
    critiques = "critiques",
    critique = "critiques",
    replicates = "replicates",
    replicate = "replicates",
    generalizes = "generalizes",
    generalize = "generalizes",
    narrows = "narrows",
    narrow = "narrows",
    uses_as_context = "uses_as_context",
    use_as_context = "uses_as_context",
    unknown = "unknown"
  )
  key <- .litxr_key_key(x)
  out <- unname(map[key])
  out[is.na(out) | !nzchar(out)] <- "unknown"
  out
}

.litxr_validate_relationship_to_current_paper <- function(x) {
  normalized <- .litxr_normalize_relationship_to_current_paper(x)
  bad <- !(normalized %in% c(
    "builds_on", "extends", "tests", "applies", "compares_with", "contradicts",
    "critiques", "replicates", "generalizes", "narrows", "uses_as_context", "unknown"
  ))
  if (any(bad)) {
    stop("Invalid relationship_to_current_paper value(s): ", paste(unique(as.character(x[bad])), collapse = ", "), call. = FALSE)
  }
  invisible(TRUE)
}

.litxr_anchor_reference_levels <- function() {
  c(
    "theoretical_foundation",
    "conceptual_foundation",
    "methodological_foundation",
    "empirical_benchmark",
    "main_comparison",
    "motivation",
    "contrasting_view",
    "problem_origin",
    "application_context",
    "review_anchor",
    "policy_context",
    "unknown"
  )
}

.litxr_citation_logic_type_levels <- function() {
  c(
    "A_improves_B",
    "A_reduces_B",
    "A_causes_B",
    "A_contributes_to_B",
    "A_is_associated_with_B",
    "A_predicts_B",
    "A_mediates_B_and_C",
    "A_moderates_B_and_C",
    "A_mitigates_effect_of_B_on_C",
    "A_amplifies_effect_of_B_on_C",
    "A_has_limitation_B",
    "A_faces_challenge_B",
    "A_creates_risk_B",
    "A_requires_B",
    "A_depends_on_B",
    "A_constrains_B",
    "A_enables_B",
    "A_outperforms_B",
    "A_underperforms_B",
    "A_is_better_than_B_for_C",
    "A_is_worse_than_B_for_C",
    "A_extends_B",
    "A_contradicts_B",
    "A_supports_B",
    "A_represents_B",
    "A_is_proxy_for_B",
    "A_measures_B",
    "A_operationalizes_B",
    "A_can_be_classified_into_B_C_D",
    "A_consists_of_B_C_D",
    "A_has_dimension_B_C_D",
    "A_introduces_B",
    "A_proposes_B",
    "A_reviews_B",
    "A_synthesizes_B",
    "A_provides_evidence_for_B",
    "A_provides_counterevidence_to_B",
    "unknown"
  )
}

.litxr_normalize_citation_logic_type <- function(x) {
  map <- c(
    improves = "A_improves_B",
    reduce = "A_reduces_B",
    reduces = "A_reduces_B",
    causes = "A_causes_B",
    contribute = "A_contributes_to_B",
    contributes = "A_contributes_to_B",
    associated = "A_is_associated_with_B",
    associated_with = "A_is_associated_with_B",
    predicts = "A_predicts_B",
    predict = "A_predicts_B",
    mediates = "A_mediates_B_and_C",
    moderates = "A_moderates_B_and_C",
    mitigates = "A_mitigates_effect_of_B_on_C",
    amplifies = "A_amplifies_effect_of_B_on_C",
    limitation = "A_has_limitation_B",
    challenge = "A_faces_challenge_B",
    risk = "A_creates_risk_B",
    requires = "A_requires_B",
    depends = "A_depends_on_B",
    constrains = "A_constrains_B",
    enables = "A_enables_B",
    outperforms = "A_outperforms_B",
    underperforms = "A_underperforms_B",
    better_than = "A_is_better_than_B_for_C",
    worse_than = "A_is_worse_than_B_for_C",
    extends = "A_extends_B",
    contradicts = "A_contradicts_B",
    supports = "A_supports_B",
    represents = "A_represents_B",
    proxy = "A_is_proxy_for_B",
    measures = "A_measures_B",
    operationalizes = "A_operationalizes_B",
    classifies = "A_can_be_classified_into_B_C_D",
    consists = "A_consists_of_B_C_D",
    dimension = "A_has_dimension_B_C_D",
    introduces = "A_introduces_B",
    proposes = "A_proposes_B",
    reviews = "A_reviews_B",
    synthesizes = "A_synthesizes_B",
    evidence = "A_provides_evidence_for_B",
    counterevidence = "A_provides_counterevidence_to_B",
    unknown = "unknown"
  )
  canonical <- .litxr_citation_logic_type_levels()
  map <- c(stats::setNames(canonical, tolower(canonical)), map)
  key <- .litxr_key_key(x)
  out <- unname(map[key])
  out[is.na(out) | !nzchar(out)] <- "unknown"
  out
}

.litxr_validate_citation_logic_type <- function(x) {
  normalized <- .litxr_normalize_citation_logic_type(x)
  bad <- !(normalized %in% .litxr_citation_logic_type_levels())
  if (any(bad)) {
    stop("Invalid logic_type value(s): ", paste(unique(as.character(x[bad])), collapse = ", "), call. = FALSE)
  }
  invisible(TRUE)
}

.litxr_evidence_role_levels <- function() {
  c(
    "main_finding",
    "secondary_finding",
    "theoretical_argument",
    "conceptual_argument",
    "methodological_claim",
    "review_synthesis",
    "policy_argument",
    "background_context",
    "counterevidence",
    "limitation_note",
    "example",
    "unknown"
  )
}

.litxr_normalize_evidence_role <- function(x) {
  map <- c(
    main_finding = "main_finding",
    secondary_finding = "secondary_finding",
    theoretical_argument = "theoretical_argument",
    conceptual_argument = "conceptual_argument",
    methodological_claim = "methodological_claim",
    review_synthesis = "review_synthesis",
    policy_argument = "policy_argument",
    background_context = "background_context",
    counterevidence = "counterevidence",
    limitation_note = "limitation_note",
    example = "example",
    unknown = "unknown",
    main_finding = "main_finding",
    secondary_finding = "secondary_finding",
    theoretical_argument = "theoretical_argument",
    conceptual_argument = "conceptual_argument",
    methodological_claim = "methodological_claim",
    review_synthesis = "review_synthesis",
    policy_argument = "policy_argument",
    background_context = "background_context",
    counterevidence = "counterevidence",
    limitation = "limitation_note",
    limitation_note = "limitation_note",
    example = "example",
    unknown = "unknown"
  )
  key <- .litxr_key_key(x)
  out <- unname(map[key])
  out[is.na(out) | !nzchar(out)] <- "unknown"
  out
}

.litxr_validate_evidence_role <- function(x) {
  normalized <- .litxr_normalize_evidence_role(x)
  bad <- !(normalized %in% .litxr_evidence_role_levels())
  if (any(bad)) {
    stop("Invalid evidence_role value(s): ", paste(unique(as.character(x[bad])), collapse = ", "), call. = FALSE)
  }
  invisible(TRUE)
}

.litxr_confidence_levels <- function() {
  c("low", "medium", "high", "unknown")
}

.litxr_normalize_confidence <- function(x) {
  map <- c(
    low = "low",
    medium = "medium",
    high = "high",
    unknown = "unknown"
  )
  key <- .litxr_key_key(x)
  out <- unname(map[key])
  out[is.na(out) | !nzchar(out)] <- "unknown"
  out
}

.litxr_validate_confidence <- function(x) {
  normalized <- .litxr_normalize_confidence(x)
  bad <- !(normalized %in% .litxr_confidence_levels())
  if (any(bad)) {
    stop("Invalid confidence value(s): ", paste(unique(as.character(x[bad])), collapse = ", "), call. = FALSE)
  }
  invisible(TRUE)
}

.litxr_normalize_anchor_references <- function(x) {
  dt <- .litxr_align_columns(.litxr_as_research_dt(x), .litxr_empty_anchor_references())
  dt <- data.table::copy(dt)
  char_cols <- setdiff(names(dt), c("anchor_rank", "anchor_year"))
  for (name in char_cols) {
    dt[[name]] <- as.character(dt[[name]])
  }
  dt$anchor_rank <- as.integer(dt$anchor_rank)
  dt$anchor_year <- as.integer(dt$anchor_year)
  dt$ref_id <- as.character(dt$ref_id)
  dt$anchor_title <- as.character(dt$anchor_title)
  dt$anchor_authors <- as.character(dt$anchor_authors)
  dt$anchor_ref_id <- as.character(dt$anchor_ref_id)
  dt$reason <- as.character(dt$reason)
  dt$created_at <- as.character(dt$created_at)
  dt$updated_at <- as.character(dt$updated_at)
  dt$citation_key <- as.character(dt$citation_key)
  dt$anchor_role <- .litxr_normalize_anchor_reference_role(dt$anchor_role)
  dt$relationship_to_current_paper <- .litxr_normalize_relationship_to_current_paper(dt$relationship_to_current_paper)
  dt$confidence <- .litxr_normalize_confidence(dt$confidence)
  dt
}

.litxr_normalize_citation_logic_nodes <- function(x) {
  dt <- .litxr_align_columns(.litxr_as_research_dt(x), .litxr_empty_citation_logic_nodes())
  dt <- data.table::copy(dt)
  for (name in names(dt)) {
    dt[[name]] <- as.character(dt[[name]])
  }
  dt$ref_id <- as.character(dt$ref_id)
  dt$node_id <- as.character(dt$node_id)
  dt$claim_sentence <- as.character(dt$claim_sentence)
  dt$logic_type <- .litxr_normalize_citation_logic_type(dt$logic_type)
  dt$subject_text <- as.character(dt$subject_text)
  dt$object_text <- as.character(dt$object_text)
  dt$modifier_text <- as.character(dt$modifier_text)
  dt$evidence_role <- .litxr_normalize_evidence_role(dt$evidence_role)
  dt$confidence <- .litxr_normalize_confidence(dt$confidence)
  dt$page_or_section <- as.character(dt$page_or_section)
  dt$quote_support <- as.character(dt$quote_support)
  dt$citation_use <- as.character(dt$citation_use)
  dt$tags <- vapply(dt$tags, .litxr_collapse_chr, character(1))
  dt$created_at <- as.character(dt$created_at)
  dt$updated_at <- as.character(dt$updated_at)
  dt
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

#' Template row for anchor references
#'
#' @return One-row `data.table` with the anchor references schema.
#' @export
litxr_anchor_references_template <- function() {
  data.table::data.table(
    ref_id = "",
    anchor_rank = 1L,
    citation_key = NA_character_,
    anchor_title = NA_character_,
    anchor_authors = NA_character_,
    anchor_year = NA_integer_,
    anchor_ref_id = NA_character_,
    anchor_role = "unknown",
    reason = NA_character_,
    relationship_to_current_paper = "unknown",
    confidence = "unknown",
    created_at = NA_character_,
    updated_at = NA_character_
  )
}

#' Validate anchor references rows
#'
#' @param anchors Data frame, data.table, or named list of anchor-reference
#'   rows.
#'
#' @return Invisibly returns `TRUE` on success.
#' @export
litxr_validate_anchor_references <- function(anchors) {
  dt <- .litxr_normalize_anchor_references(anchors)
  bad_ref <- is.na(dt$ref_id) | !nzchar(trimws(dt$ref_id))
  if (any(bad_ref)) {
    stop("Anchor references require non-empty `ref_id` values.", call. = FALSE)
  }
  bad_rank <- is.na(dt$anchor_rank) | dt$anchor_rank < 1L
  if (any(bad_rank)) {
    stop("Anchor references require positive integer `anchor_rank` values.", call. = FALSE)
  }
  if (any(duplicated(dt[, c("ref_id", "anchor_rank"), with = FALSE])) ) {
    stop("Anchor references require unique `ref_id` + `anchor_rank` pairs.", call. = FALSE)
  }
  .litxr_validate_anchor_reference_role(dt$anchor_role)
  .litxr_validate_relationship_to_current_paper(dt$relationship_to_current_paper)
  .litxr_validate_confidence(dt$confidence)
  invisible(TRUE)
}

#' Write anchor references rows to the project delta store
#'
#' @param anchors Data frame, data.table, or named list of anchor-reference
#'   rows.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the delta fst path.
#' @export
litxr_write_anchor_references <- function(anchors, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  rows <- .litxr_normalize_anchor_references(anchors)
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  rows$created_at[is.na(rows$created_at) | !nzchar(rows$created_at)] <- now
  rows$updated_at[is.na(rows$updated_at) | !nzchar(rows$updated_at)] <- now
  litxr_validate_anchor_references(rows)
  .litxr_ensure_project_findings_dir(cfg)
  paths <- .litxr_anchor_references_paths(cfg)
  existing <- .litxr_read_fst_or_empty(paths$delta, .litxr_empty_anchor_references)
  merged <- .litxr_upsert_table_by_key(existing, rows, c("ref_id", "anchor_rank"))
  .litxr_write_fst_atomic(as.data.frame(merged), paths$delta)
  invisible(paths$delta)
}

#' Read anchor references from main and delta stores
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param ref_ids Optional character vector of `ref_id` values to keep.
#'
#' @return `data.table` of anchor references.
#' @export
litxr_read_anchor_references <- function(config = NULL, ref_ids = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  paths <- .litxr_anchor_references_paths(cfg)
  main <- .litxr_read_fst_or_empty(paths$main, .litxr_empty_anchor_references)
  delta <- .litxr_read_fst_or_empty(paths$delta, .litxr_empty_anchor_references)
  out <- .litxr_upsert_table_by_key(main, delta, c("ref_id", "anchor_rank"))
  out <- .litxr_normalize_anchor_references(out)
  if (!is.null(ref_ids) && length(ref_ids)) {
    out <- out[out$ref_id %in% as.character(ref_ids), ]
  }
  out
}

#' Find anchor references rows
#'
#' @param query Optional substring query over text fields.
#' @param ref_id Optional exact `ref_id` filter.
#' @param anchor_rank Optional exact `anchor_rank` filter.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Filtered `data.table`.
#' @export
litxr_find_anchor_references <- function(query = NULL, ref_id = NULL, anchor_rank = NULL, config = NULL) {
  out <- litxr_read_anchor_references(config = config, ref_ids = ref_id)
  if (!nrow(out)) {
    return(out)
  }
  if (!is.null(anchor_rank) && length(anchor_rank)) {
    out <- out[out$anchor_rank %in% as.integer(anchor_rank), ]
  }
  if (!is.null(query) && nzchar(as.character(query[[1]]))) {
    text <- paste(
      out$citation_key,
      out$anchor_title,
      out$anchor_authors,
      out$anchor_ref_id,
      out$anchor_role,
      out$reason,
      out$relationship_to_current_paper,
      out$confidence,
      out$created_at,
      out$updated_at
    )
    out <- out[.litxr_research_query_keep(text, query), ]
  }
  out
}

#' Compact anchor references delta into the main store
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the main fst path.
#' @export
litxr_compact_anchor_references <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_ensure_project_findings_dir(cfg)
  paths <- .litxr_anchor_references_paths(cfg)
  main <- .litxr_read_fst_or_empty(paths$main, .litxr_empty_anchor_references)
  delta <- .litxr_read_fst_or_empty(paths$delta, .litxr_empty_anchor_references)
  merged <- .litxr_upsert_table_by_key(
    .litxr_normalize_anchor_references(main),
    .litxr_normalize_anchor_references(delta),
    c("ref_id", "anchor_rank")
  )
  .litxr_write_fst_atomic(as.data.frame(merged), paths$main)
  if (file.exists(paths$delta)) {
    unlink(paths$delta)
  }
  invisible(paths$main)
}

#' Rebuild anchor references main store from current local tables
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the rebuilt main fst path.
#' @export
litxr_rebuild_anchor_references <- function(config = NULL) {
  litxr_compact_anchor_references(config = config)
}

#' Template row for citation logic nodes
#'
#' @return One-row `data.table` with the citation logic nodes schema.
#' @export
litxr_citation_logic_nodes_template <- function() {
  data.table::data.table(
    ref_id = "",
    node_id = "",
    claim_sentence = NA_character_,
    logic_type = "unknown",
    subject_text = NA_character_,
    object_text = NA_character_,
    modifier_text = NA_character_,
    evidence_role = "unknown",
    confidence = "unknown",
    page_or_section = NA_character_,
    quote_support = NA_character_,
    citation_use = NA_character_,
    tags = NA_character_,
    created_at = NA_character_,
    updated_at = NA_character_
  )
}

#' Validate citation logic node rows
#'
#' @param nodes Data frame, data.table, or named list of citation-logic rows.
#'
#' @return Invisibly returns `TRUE` on success.
#' @export
litxr_validate_citation_logic_nodes <- function(nodes) {
  dt <- .litxr_normalize_citation_logic_nodes(nodes)
  bad_ref <- is.na(dt$ref_id) | !nzchar(trimws(dt$ref_id))
  if (any(bad_ref)) {
    stop("Citation logic nodes require non-empty `ref_id` values.", call. = FALSE)
  }
  bad_node <- is.na(dt$node_id) | !nzchar(trimws(dt$node_id))
  if (any(bad_node)) {
    stop("Citation logic nodes require non-empty `node_id` values.", call. = FALSE)
  }
  bad_claim <- is.na(dt$claim_sentence) | !nzchar(trimws(dt$claim_sentence))
  if (any(bad_claim)) {
    stop("Citation logic nodes require non-empty `claim_sentence` values.", call. = FALSE)
  }
  if (any(duplicated(dt[, c("ref_id", "node_id"), with = FALSE]))) {
    stop("Citation logic nodes require unique `ref_id` + `node_id` pairs.", call. = FALSE)
  }
  .litxr_validate_citation_logic_type(dt$logic_type)
  .litxr_validate_evidence_role(dt$evidence_role)
  .litxr_validate_confidence(dt$confidence)
  invisible(TRUE)
}

#' Write citation logic nodes to the project delta store
#'
#' @param nodes Data frame, data.table, or named list of citation-logic rows.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the delta fst path.
#' @export
litxr_write_citation_logic_nodes <- function(nodes, config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  rows <- .litxr_normalize_citation_logic_nodes(nodes)
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  rows$created_at[is.na(rows$created_at) | !nzchar(rows$created_at)] <- now
  rows$updated_at[is.na(rows$updated_at) | !nzchar(rows$updated_at)] <- now
  litxr_validate_citation_logic_nodes(rows)
  .litxr_ensure_project_findings_dir(cfg)
  paths <- .litxr_citation_logic_nodes_paths(cfg)
  existing <- .litxr_read_fst_or_empty(paths$delta, .litxr_empty_citation_logic_nodes)
  merged <- .litxr_upsert_table_by_key(existing, rows, c("ref_id", "node_id"))
  .litxr_write_fst_atomic(as.data.frame(merged), paths$delta)
  invisible(paths$delta)
}

#' Read citation logic nodes from main and delta stores
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param ref_ids Optional character vector of `ref_id` values to keep.
#'
#' @return `data.table` of citation logic nodes.
#' @export
litxr_read_citation_logic_nodes <- function(config = NULL, ref_ids = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  paths <- .litxr_citation_logic_nodes_paths(cfg)
  main <- .litxr_read_fst_or_empty(paths$main, .litxr_empty_citation_logic_nodes)
  delta <- .litxr_read_fst_or_empty(paths$delta, .litxr_empty_citation_logic_nodes)
  out <- .litxr_upsert_table_by_key(main, delta, c("ref_id", "node_id"))
  out <- .litxr_normalize_citation_logic_nodes(out)
  if (!is.null(ref_ids) && length(ref_ids)) {
    out <- out[out$ref_id %in% as.character(ref_ids), ]
  }
  out
}

#' Find citation logic nodes
#'
#' @param query Optional substring query over text fields.
#' @param ref_id Optional exact `ref_id` filter.
#' @param node_id Optional exact `node_id` filter.
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Filtered `data.table`.
#' @export
litxr_find_citation_logic_nodes <- function(query = NULL, ref_id = NULL, node_id = NULL, config = NULL) {
  out <- litxr_read_citation_logic_nodes(config = config, ref_ids = ref_id)
  if (!nrow(out)) {
    return(out)
  }
  if (!is.null(node_id) && length(node_id)) {
    out <- out[out$node_id %in% as.character(node_id), ]
  }
  if (!is.null(query) && nzchar(as.character(query[[1]]))) {
    text <- paste(
      out$claim_sentence,
      out$logic_type,
      out$subject_text,
      out$object_text,
      out$modifier_text,
      out$evidence_role,
      out$confidence,
      out$page_or_section,
      out$quote_support,
      out$citation_use,
      out$tags,
      out$created_at,
      out$updated_at
    )
    out <- out[.litxr_research_query_keep(text, query), ]
  }
  out
}

#' Compact citation logic nodes delta into the main store
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the main fst path.
#' @export
litxr_compact_citation_logic_nodes <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  .litxr_ensure_project_findings_dir(cfg)
  paths <- .litxr_citation_logic_nodes_paths(cfg)
  main <- .litxr_read_fst_or_empty(paths$main, .litxr_empty_citation_logic_nodes)
  delta <- .litxr_read_fst_or_empty(paths$delta, .litxr_empty_citation_logic_nodes)
  merged <- .litxr_upsert_table_by_key(
    .litxr_normalize_citation_logic_nodes(main),
    .litxr_normalize_citation_logic_nodes(delta),
    c("ref_id", "node_id")
  )
  .litxr_write_fst_atomic(as.data.frame(merged), paths$main)
  if (file.exists(paths$delta)) {
    unlink(paths$delta)
  }
  invisible(paths$main)
}

#' Rebuild citation logic nodes main store from current local tables
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#'
#' @return Invisibly returns the rebuilt main fst path.
#' @export
litxr_rebuild_citation_logic_nodes <- function(config = NULL) {
  litxr_compact_citation_logic_nodes(config = config)
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
      n_descriptive_stats = integer(),
      has_anchor_references = logical(),
      n_anchor_references = integer(),
      has_citation_logic_nodes = logical(),
      n_citation_logic_nodes = integer()
    ))
  }

  links <- litxr_read_reference_collections(cfg)
  status <- litxr_read_enrichment_status(cfg)
  digests <- litxr_read_llm_digests(cfg)
  findings <- litxr_read_standardized_findings(cfg)
  desc_stats <- litxr_read_descriptive_stats(cfg)
  anchors <- litxr_read_anchor_references(cfg)
  logic_nodes <- litxr_read_citation_logic_nodes(cfg)

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
  anchor_counts <- if (nrow(anchors)) {
    counts <- table(as.character(anchors$ref_id))
    data.table::data.table(
      ref_id = names(counts),
      n_anchor_references = as.integer(counts)
    )
  } else {
    data.table::data.table(ref_id = character(), n_anchor_references = integer())
  }
  logic_counts <- if (nrow(logic_nodes)) {
    counts <- table(as.character(logic_nodes$ref_id))
    data.table::data.table(
      ref_id = names(counts),
      n_citation_logic_nodes = as.integer(counts)
    )
  } else {
    data.table::data.table(ref_id = character(), n_citation_logic_nodes = integer())
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
  out <- merge(out, anchor_counts, by = "ref_id", all.x = TRUE)
  out <- merge(out, logic_counts, by = "ref_id", all.x = TRUE)

  out$collection_ids[is.na(out$collection_ids)] <- ""
  out$has_md[is.na(out$has_md)] <- FALSE
  out$has_llm_digest[is.na(out$has_llm_digest)] <- FALSE
  out$n_standardized_findings[is.na(out$n_standardized_findings)] <- 0L
  out$n_descriptive_stats[is.na(out$n_descriptive_stats)] <- 0L
  out$n_anchor_references[is.na(out$n_anchor_references)] <- 0L
  out$n_citation_logic_nodes[is.na(out$n_citation_logic_nodes)] <- 0L
  out$has_standardized_findings <- out$n_standardized_findings > 0L
  out$has_descriptive_stats <- out$n_descriptive_stats > 0L
  out$has_anchor_references <- out$n_anchor_references > 0L
  out$has_citation_logic_nodes <- out$n_citation_logic_nodes > 0L

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
    "has_descriptive_stats", "n_descriptive_stats",
    "has_anchor_references", "n_anchor_references",
    "has_citation_logic_nodes", "n_citation_logic_nodes"
  ), with = FALSE]
}

.litxr_filter_missing_research_status <- function(status_dt, missing_type) {
  switch(
    missing_type,
    llm_digest = status_dt[!status_dt$has_llm_digest, ],
    standardized_findings = status_dt[!status_dt$has_standardized_findings, ],
    descriptive_stats = status_dt[!status_dt$has_descriptive_stats, ],
    anchor_references = status_dt[!status_dt$has_anchor_references, ],
    citation_logic_nodes = status_dt[!status_dt$has_citation_logic_nodes, ],
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

#' Find references missing anchor references
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param collection_id Optional collection membership filter.
#' @param ref_ids Optional character vector of reference ids to keep.
#'
#' @return Filtered `data.table` from `litxr_read_research_schema_status()`.
#' @export
litxr_find_refs_missing_anchor_references <- function(config = NULL, collection_id = NULL, ref_ids = NULL) {
  .litxr_filter_missing_research_status(
    litxr_read_research_schema_status(config = config, collection_id = collection_id, ref_ids = ref_ids),
    "anchor_references"
  )
}

#' Find references missing citation logic nodes
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_CONFIG`.
#' @param collection_id Optional collection membership filter.
#' @param ref_ids Optional character vector of reference ids to keep.
#'
#' @return Filtered `data.table` from `litxr_read_research_schema_status()`.
#' @export
litxr_find_refs_missing_citation_logic_nodes <- function(config = NULL, collection_id = NULL, ref_ids = NULL) {
  .litxr_filter_missing_research_status(
    litxr_read_research_schema_status(config = config, collection_id = collection_id, ref_ids = ref_ids),
    "citation_logic_nodes"
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
