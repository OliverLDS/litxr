.litxr_llm_schema_release_path <- function() {
  path <- system.file("extdata", "llm_schema_release.yaml", package = "litxr", mustWork = FALSE)
  if (!nzchar(path)) {
    path <- file.path(getwd(), "inst", "extdata", "llm_schema_release.yaml")
  }
  if (!file.exists(path)) {
    stop("LLM schema release metadata file not found: ", path, call. = FALSE)
  }
  path
}

.litxr_validate_llm_schema_release_info <- function(x) {
  required <- c(
    "schema_version",
    "prompt_version",
    "schema_release_date",
    "schema_release_tag",
    "schema_release_notes"
  )

  if (!is.list(x)) {
    stop("LLM schema release metadata must parse to a named list.", call. = FALSE)
  }

  missing <- setdiff(required, names(x))
  if (length(missing)) {
    stop(
      "LLM schema release metadata is missing required fields: ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }

  for (field in required) {
    value <- x[[field]]
    if (is.list(value) && length(value)) {
      value <- value[[1]]
    }
    value <- as.character(value %||% NA_character_)
    if (!length(value) || is.na(value[[1]]) || !nzchar(trimws(value[[1]]))) {
      stop("LLM schema release metadata field `", field, "` must be a non-empty scalar.", call. = FALSE)
    }
  }

  date_value <- as.character(x$schema_release_date[[1]] %||% x$schema_release_date)
  if (!grepl("^\\d{4}-\\d{2}-\\d{2}$", date_value)) {
    stop("LLM schema release metadata field `schema_release_date` must use YYYY-MM-DD.", call. = FALSE)
  }

  invisible(TRUE)
}

#' Read machine-readable LLM digest schema release metadata
#'
#' Returns the package's explicit metadata for the active LLM digest schema
#' contract. The metadata is stored in `inst/extdata/llm_schema_release.yaml`
#' and is intended for machine-facing workflows that need a stable answer for
#' the currently active schema release.
#'
#' @return Named list with `schema_version`, `prompt_version`,
#'   `schema_release_date`, `schema_release_tag`, and
#'   `schema_release_notes`.
#' @export
litxr_llm_schema_release_info <- function() {
  info <- yaml::read_yaml(.litxr_llm_schema_release_path())
  .litxr_validate_llm_schema_release_info(info)

  list(
    schema_version = as.character(info$schema_version[[1]] %||% info$schema_version),
    prompt_version = as.character(info$prompt_version[[1]] %||% info$prompt_version),
    schema_release_date = as.character(info$schema_release_date[[1]] %||% info$schema_release_date),
    schema_release_tag = as.character(info$schema_release_tag[[1]] %||% info$schema_release_tag),
    schema_release_notes = as.character(info$schema_release_notes[[1]] %||% info$schema_release_notes)
  )
}
