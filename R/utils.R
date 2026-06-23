.compact_filter_string <- function(filters) {
  if (is.null(filters) || !length(filters)) {
    return(NULL)
  }

  pieces <- vapply(names(filters), function(name) {
    value <- filters[[name]]
    if (length(value) > 1L) {
      value <- paste(value, collapse = ",")
    }
    paste0(name, ":", value)
  }, character(1))

  paste(pieces, collapse = ",")
}

.litxr_read_fst_subset <- function(path, columns = NULL, as_data_table = TRUE) {
  if (!file.exists(path)) {
    return(data.table::data.table())
  }
  if (is.null(columns) || !length(columns)) {
    return(fst::read_fst(path, as.data.table = as_data_table))
  }

  available <- .litxr_read_index_columns_safe(path)
  if (is.null(available)) {
    return(data.table::data.table())
  }
  keep <- intersect(unique(as.character(columns)), available)
  if (!length(keep)) {
    return(data.table::data.table())
  }
  fst::read_fst(path, columns = keep, as.data.table = as_data_table)
}
