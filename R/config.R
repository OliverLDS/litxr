#' Return the default litxr config path
#'
#' `litxr` keeps the user-specific project config in `.litxr/config.yaml` at the
#' project root so the package can keep local sync settings out of version
#' control.
#'
#' @param path Project root.
#'
#' @return Character scalar path.
#' @export
litxr_config_path <- function(path = ".") {
  file.path(normalizePath(path, winslash = "/", mustWork = FALSE), ".litxr", "config.yaml")
}

#' Initialize a litxr project
#'
#' Creates the `.litxr` config directory, writes a starter `config.yaml` when it
#' does not exist yet, and makes sure the local config path is gitignored.
#'
#' @param path Project root.
#' @param overwrite Whether to overwrite an existing config file.
#'
#' @return Invisibly returns the config path.
#' @export
litxr_init <- function(path = ".", overwrite = FALSE) {
  root <- normalizePath(path, winslash = "/", mustWork = FALSE)
  config_dir <- file.path(root, ".litxr")
  config_path <- litxr_config_path(root)

  if (!dir.exists(config_dir)) {
    dir.create(config_dir, recursive = TRUE, showWarnings = FALSE)
  }

  if (!file.exists(config_path) || isTRUE(overwrite)) {
    yaml::write_yaml(.litxr_default_config(root), config_path)
  }

  .litxr_ensure_gitignore(root, ".litxr/config.yaml")
  invisible(config_path)
}

#' Read a litxr config file
#'
#' @param path Project root or direct path to a config file.
#'
#' @return Parsed config list.
#' @export
litxr_read_config <- function(path = ".") {
  config_path <- path
  if (!grepl("config\\.ya?ml$", path)) {
    config_path <- litxr_config_path(path)
  }

  if (!file.exists(config_path)) {
    stop("Config file not found: ", config_path, call. = FALSE)
  }

  cfg <- yaml::read_yaml(config_path)
  attr(cfg, "config_root") <- dirname(config_path)
  .litxr_validate_config(cfg, config_path)
}

#' List journals registered in the config
#'
#' @param config Parsed config list or a path that `litxr_read_config()` accepts.
#'
#' @return `data.table` of journal registrations.
#' @export
litxr_list_journals <- function(config = ".") {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  journals <- cfg$journals

  data.table::rbindlist(lapply(journals, function(x) {
    data.table::data.table(
      journal_id = x$journal_id,
      title = x$title,
      remote_channel = x$remote_channel,
      local_path = x$local_path
    )
  }), fill = TRUE)
}

#' @noRd
.litxr_journal_issns <- function(journal) {
  values <- c(
    journal$metadata$issn_print,
    journal$metadata$issn_electronic,
    journal$sync$filters$issn
  )

  values <- unique(stats::na.omit(as.character(values)))
  values[nzchar(values)]
}

.litxr_default_config <- function(root) {
  list(
    version = 1L,
    project = list(
      name = basename(normalizePath(root, winslash = "/", mustWork = FALSE)),
      data_root = "data/literature"
    ),
    journals = list(
      list(
        journal_id = "journal_of_finance",
        title = "Journal of Finance",
        remote_channel = "crossref",
        local_path = "data/literature/journal_of_finance",
        metadata = list(
          publisher = "Wiley",
          issn_print = "0022-1082",
          issn_electronic = "1540-6261"
        ),
        sync = list(
          filters = list(
            issn = "0022-1082"
          ),
          limit = 100L
        )
      ),
      list(
        journal_id = "journal_of_financial_economics",
        title = "Journal of Financial Economics",
        remote_channel = "crossref",
        local_path = "data/literature/journal_of_financial_economics",
        metadata = list(
          publisher = "Elsevier",
          issn_print = "0304-405X"
        ),
        sync = list(
          filters = list(
            issn = "0304-405X"
          ),
          limit = 100L
        )
      )
    )
  )
}

.litxr_validate_config <- function(cfg, config_path) {
  if (is.null(cfg) || !is.list(cfg)) {
    stop("Invalid config in ", config_path, ": expected a YAML object.", call. = FALSE)
  }

  if (is.null(cfg$journals) || !length(cfg$journals)) {
    stop("Invalid config in ", config_path, ": `journals` must contain at least one journal.", call. = FALSE)
  }

  required_fields <- c("journal_id", "title", "remote_channel", "local_path")
  for (journal in cfg$journals) {
    missing <- required_fields[vapply(required_fields, function(field) {
      is.null(journal[[field]]) || !nzchar(as.character(journal[[field]]))
    }, logical(1))]

    if (length(missing)) {
      stop(
        "Invalid config in ", config_path, ": journal entry is missing ",
        paste(missing, collapse = ", "), ".",
        call. = FALSE
      )
    }
  }

  cfg
}

.litxr_ensure_gitignore <- function(root, entry) {
  gitignore_path <- file.path(root, ".gitignore")
  current <- if (file.exists(gitignore_path)) readLines(gitignore_path, warn = FALSE) else character()

  if (!(entry %in% current)) {
    writeLines(c(current, entry), gitignore_path)
  }
}
