#' Return the litxr config path
#'
#' `litxr` resolves the config file as `file.path(LITXR_DATA_ROOT,
#' "config.yaml")`.
#'
#' @return Character scalar path.
#' @export
litxr_config_path <- function() {
  data_root <- Sys.getenv("LITXR_DATA_ROOT", unset = "")
  if (!nzchar(data_root)) {
    stop(
      "LITXR_DATA_ROOT is not set. Add `LITXR_DATA_ROOT=/absolute/path/to/shared/data/root` ",
      "to `.Renviron` and restart R.",
      call. = FALSE
    )
  }

  normalizePath(file.path(data_root, "config.yaml"), winslash = "/", mustWork = FALSE)
}

#' Initialize a litxr project
#'
#' Reads the target config path from `LITXR_DATA_ROOT`. If the file does not
#' exist, `litxr_init()` writes a starter `config.yaml` there. If the file
#' already exists, it refuses to overwrite it and instructs the user to edit
#' that file manually. After creating the file, it reminds the user to update
#' `project.data_root` and each collection `local_path`.
#'
#' @return Invisibly returns the config path.
#' @export
litxr_init <- function() {
  config_path <- litxr_config_path()
  config_dir <- dirname(config_path)

  if (file.exists(config_path)) {
    stop(
      "config.yaml already exists at ", config_path, ". ",
      "Refusing to overwrite it with the default config. ",
      "Modify that file manually following the litxr config format.",
      call. = FALSE
    )
  }

  if (!dir.exists(config_dir)) {
    dir.create(config_dir, recursive = TRUE, showWarnings = FALSE)
  }

  yaml::write_yaml(.litxr_default_config(.litxr_project_root_from_config(config_path)), config_path)
  message(
    "Wrote default config to ", config_path, ". ",
    "Edit `project.data_root` and each collection `local_path` before syncing. ",
    "`project.data_root` is the root folder for your literature data store. ",
    "Each collection `local_path` is the folder where that collection's reference JSON files will be stored."
  )
  invisible(config_path)
}

#' Read a litxr config file
#'
#' @param path Optional direct path to a config file. When omitted, `litxr`
#'   reads `LITXR_DATA_ROOT`.
#'
#' @return Parsed config list.
#' @export
litxr_read_config <- function(path = NULL) {
  config_path <- if (is.null(path)) litxr_config_path() else path

  if (!file.exists(config_path)) {
    stop("Config file not found: ", config_path, call. = FALSE)
  }

  raw_lines <- readLines(config_path, warn = FALSE)
  normalized_text <- .litxr_normalize_yaml_indentation(raw_lines)

  cfg <- tryCatch(
    yaml::yaml.load(normalized_text),
    error = function(e) {
      stop("Failed to parse config in ", config_path, ". ", conditionMessage(e), call. = FALSE)
    }
  )
  attr(cfg, "config_root") <- dirname(config_path)
  attr(cfg, "config_path") <- normalizePath(config_path, winslash = "/", mustWork = FALSE)
  .litxr_validate_config(cfg, config_path)
}

#' List collections registered in the config
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#'
#' @return `data.table` of collection registrations.
#' @export
litxr_list_collections <- function(config = NULL) {
  cfg <- if (is.character(config)) litxr_read_config(config) else config
  if (is.null(cfg)) cfg <- litxr_read_config()
  collections <- .litxr_config_collections(cfg)

  data.table::rbindlist(lapply(collections, function(x) {
    data.table::data.table(
      collection_id = x$collection_id,
      collection_type = x$collection_type,
      title = x$title,
      remote_channel = x$remote_channel,
      local_path = x$local_path
    )
  }), fill = TRUE)
}

#' List journals registered in the config
#'
#' Backward-compatible wrapper around `litxr_list_collections()`. This function
#' keeps the old journal-oriented column names while exposing the normalized
#' collection config.
#'
#' @param config Optional parsed config list or a direct config path. When
#'   omitted, `litxr` reads `LITXR_DATA_ROOT`.
#'
#' @return `data.table` of collection registrations with legacy journal column
#'   names.
#' @export
litxr_list_journals <- function(config = NULL) {
  collections <- litxr_list_collections(config)
  if (!nrow(collections)) {
    return(data.table::data.table())
  }

  journals <- data.table::copy(collections)
  data.table::setnames(journals, "collection_id", "journal_id")
  journals
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
      data_root = "."
    ),
    collections = list(
      list(
        collection_id = "journal_of_finance",
        collection_type = "journal",
        title = "Journal of Finance",
        remote_channel = "crossref",
        local_path = "ref/journal_of_finance",
        metadata = list(
          publisher = "Wiley",
          issn_print = "0022-1082",
          issn_electronic = "1540-6261"
        ),
        sync = list(
          filters = list(
            issn = "0022-1082"
          )
        )
      ),
      list(
        collection_id = "journal_of_financial_economics",
        collection_type = "journal",
        title = "Journal of Financial Economics",
        remote_channel = "crossref",
        local_path = "ref/journal_of_financial_economics",
        metadata = list(
          publisher = "Elsevier",
          issn_print = "0304-405X"
        ),
        sync = list(
          filters = list(
            issn = "0304-405X"
          )
        )
      ),
      list(
        collection_id = "arxiv_cs_ai",
        collection_type = "arxiv_category",
        title = "arXiv cs.AI",
        remote_channel = "arxiv",
        local_path = "ref/arxiv_cs_ai",
        metadata = list(
          archive = "arXiv",
          category = "cs.AI"
        ),
        sync = list(
          search_query = "cat:cs.AI"
        )
      )
    )
  )
}

.litxr_project_root_from_config <- function(config_path) {
  config_dir <- dirname(normalizePath(config_path, winslash = "/", mustWork = FALSE))
  if (basename(config_dir) == ".litxr") {
    return(dirname(config_dir))
  }
  config_dir
}

.litxr_validate_config <- function(cfg, config_path) {
  if (is.null(cfg) || !is.list(cfg)) {
    stop("Invalid config in ", config_path, ": expected a YAML object.", call. = FALSE)
  }

  cfg <- .litxr_normalize_config_schema(cfg)

  if (is.null(cfg$collections) || !length(cfg$collections)) {
    stop("Invalid config in ", config_path, ": `collections` must contain at least one collection.", call. = FALSE)
  }

  required_fields <- c("collection_id", "title", "remote_channel", "local_path")
  for (collection in cfg$collections) {
    missing <- required_fields[vapply(required_fields, function(field) {
      is.null(collection[[field]]) || !nzchar(as.character(collection[[field]]))
    }, logical(1))]

    if (length(missing)) {
      stop(
        "Invalid config in ", config_path, ": collection entry is missing ",
        paste(missing, collapse = ", "), ".",
        call. = FALSE
      )
    }
  }

  cfg
}

.litxr_normalize_yaml_indentation <- function(lines) {
  normalized <- vapply(lines, function(line) {
    prefix_match <- regmatches(line, regexpr("^[ \t]*", line))
    prefix <- if (length(prefix_match)) prefix_match else ""
    rest <- substring(line, nchar(prefix) + 1L)
    prefix <- gsub("\t", "  ", prefix, fixed = TRUE)
    paste0(prefix, rest)
  }, character(1))

  paste(normalized, collapse = "\n")
}

.litxr_write_config <- function(cfg, path = attr(cfg, "config_path", exact = TRUE)) {
  if (is.null(path) || !nzchar(path)) {
    stop("Unable to write config: config path is missing.", call. = FALSE)
  }

  out <- .litxr_normalize_config_schema(cfg)
  attr(out, "config_root") <- NULL
  attr(out, "config_path") <- NULL
  out$journals <- NULL
  yaml::write_yaml(out, path)
  attr(cfg, "config_root") <- dirname(path)
  attr(cfg, "config_path") <- normalizePath(path, winslash = "/", mustWork = FALSE)
  invisible(path)
}

.litxr_config_collections <- function(cfg) {
  if (!is.null(cfg$collections)) {
    return(cfg$collections)
  }
  cfg$journals
}

.litxr_collection_ids_by_remote_channel <- function(cfg, remote_channel) {
  remote_channel <- as.character(remote_channel)[[1L]]
  if (is.na(remote_channel) || !nzchar(remote_channel)) {
    return(character())
  }

  collections <- .litxr_config_collections(cfg)
  if (!length(collections)) {
    return(character())
  }

  ids <- vapply(collections, function(collection) {
    if (identical(as.character(collection$remote_channel), remote_channel)) {
      collection_id <- collection$collection_id
      if (is.null(collection_id) || !length(collection_id) || is.na(collection_id[[1L]]) || !nzchar(as.character(collection_id[[1L]]))) {
        collection_id <- collection$journal_id
      }
      if (is.null(collection_id) || !length(collection_id) || is.na(collection_id[[1L]]) || !nzchar(as.character(collection_id[[1L]]))) {
        NA_character_
      } else {
        as.character(collection_id[[1L]])
      }
    } else {
      NA_character_
    }
  }, character(1))
  ids <- ids[!is.na(ids) & nzchar(ids)]
  unique(ids)
}

.litxr_normalize_config_schema <- function(cfg) {
  if (!is.null(cfg$collections) && length(cfg$collections)) {
    collections <- lapply(cfg$collections, .litxr_normalize_collection_entry)
  } else if (!is.null(cfg$journals) && length(cfg$journals)) {
    collections <- lapply(cfg$journals, .litxr_collection_from_legacy_journal)
  } else {
    collections <- list()
  }

  collections <- .litxr_compact_collection_entries(collections)
  cfg$collections <- collections
  cfg$journals <- lapply(collections, .litxr_collection_to_legacy_journal)
  cfg
}

.litxr_compact_collection_entries <- function(collections) {
  if (!length(collections)) {
    return(collections)
  }

  ids <- vapply(collections, function(collection) {
    collection_id <- collection$collection_id
    if (is.null(collection_id) || !length(collection_id) || is.na(collection_id[[1L]])) {
      collection_id <- collection$journal_id
    }
    if (is.null(collection_id) || !length(collection_id) || is.na(collection_id[[1L]]) || !nzchar(as.character(collection_id[[1L]]))) {
      NA_character_
    } else {
      as.character(collection_id[[1L]])
    }
  }, character(1))

  ids[is.na(ids) | !nzchar(ids)] <- NA_character_
  keep <- rep(TRUE, length(collections))
  for (id in unique(ids[!is.na(ids)])) {
    idx <- which(ids == id)
    if (!length(idx)) {
      next
    }
    if (identical(id, "unclassified_doi")) {
      preferred <- idx[!vapply(collections[idx], .litxr_collection_is_legacy_unclassified_doi, logical(1))]
      if (length(preferred)) {
        idx <- tail(preferred, 1L)
      } else {
        idx <- tail(idx, 1L)
      }
    } else {
      idx <- tail(idx, 1L)
    }
    keep[which(ids == id)] <- FALSE
    keep[idx] <- TRUE
  }

  collections[keep]
}

.litxr_collection_from_legacy_journal <- function(journal) {
  collection <- journal
  collection$collection_id <- if (!is.null(journal$journal_id)) journal$journal_id else journal$collection_id
  collection$collection_type <- if (!is.null(journal$collection_type)) {
    journal$collection_type
  } else if (identical(journal$remote_channel, "arxiv")) {
    "arxiv_category"
  } else {
    "journal"
  }
  collection$journal_id <- NULL
  collection
}

.litxr_collection_to_legacy_journal <- function(collection) {
  journal <- collection
  journal$journal_id <- collection$collection_id
  journal
}

.litxr_normalize_collection_entry <- function(collection) {
  out <- collection
  if (is.null(out$collection_id) && !is.null(out$journal_id)) {
    out$collection_id <- out$journal_id
  }
  if (.litxr_collection_is_legacy_unclassified_doi(out)) {
    original_id <- if (!is.null(out$collection_id) && length(out$collection_id)) as.character(out$collection_id[[1L]]) else NA_character_
    out$collection_id <- "unclassified_doi"
    out$collection_type <- "doi_collection"
    out$title <- "Unclassified DOI"
    out$remote_channel <- "crossref"
    out$local_path <- file.path("ref", "unclassified_doi")
    out$metadata <- c(out$metadata, list(legacy_collection_id = original_id))
    out$sync <- list(filters = list(issn = NA_character_))
  }
  if (is.null(out$collection_type) || !nzchar(as.character(out$collection_type))) {
    out$collection_type <- if (identical(out$remote_channel, "arxiv")) "arxiv_category" else "journal"
  }
  out
}

.litxr_collection_is_legacy_unclassified_doi <- function(collection) {
  collection_id <- collection$collection_id
  if (is.null(collection_id) || !length(collection_id)) {
    collection_id <- collection$journal_id
  }
  collection_id <- if (is.null(collection_id) || !length(collection_id)) NA_character_ else as.character(collection_id[[1L]])
  title <- if (!is.null(collection$title) && length(collection$title)) as.character(collection$title[[1L]]) else NA_character_

  is_legacy_id <- !is.na(collection_id) && (
    identical(collection_id, "23") ||
      grepl("^crossref_unclassified(?:_[0-9]+)?$", collection_id, ignore.case = TRUE)
  )
  is_legacy_title <- !is.na(title) && identical(title, "Crossref Unclassified")
  is_legacy_id || is_legacy_title
}
