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
