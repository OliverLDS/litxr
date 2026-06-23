litxr_diag_now <- function() {
  Sys.time()
}

litxr_diag_file_meta <- function(path) {
  path <- as.character(path)
  if (!length(path) || is.na(path[[1]]) || !nzchar(trimws(path[[1]]))) {
    return(list(
      path = NA_character_,
      exists = FALSE,
      size_bytes = NA_real_,
      mtime = NA_character_
    ))
  }

  path <- path.expand(path[[1]])
  exists <- file.exists(path)
  info <- if (exists) file.info(path) else NULL
  list(
    path = normalizePath(path, winslash = "/", mustWork = FALSE),
    exists = exists,
    size_bytes = if (exists) as.numeric(info$size) else NA_real_,
    mtime = if (exists) format(as.POSIXct(info$mtime, tz = "UTC"), tz = "UTC", usetz = TRUE) else NA_character_
  )
}

litxr_diag_init <- function(script, enabled = FALSE, inputs = list(), outputs = list(), args = list()) {
  list(
    script = as.character(script)[[1]],
    status = "running",
    enabled = isTRUE(enabled),
    started_at = format(litxr_diag_now(), tz = "UTC", usetz = TRUE),
    ended_at = NA_character_,
    elapsed_sec = NA_real_,
    args = args,
    inputs = inputs,
    outputs = outputs,
    steps = list()
  )
}

litxr_diag_step <- function(diag, name, started_at, inputs = list(), outputs = list(), details = list()) {
  if (!isTRUE(diag$enabled)) {
    return(diag)
  }

  elapsed_sec <- as.numeric(difftime(litxr_diag_now(), started_at, units = "secs"))
  step <- c(
    list(
      name = as.character(name)[[1]],
      elapsed_sec = elapsed_sec
    ),
    if (length(inputs)) list(inputs = inputs) else list(),
    if (length(outputs)) list(outputs = outputs) else list(),
    if (length(details)) list(details = details) else list()
  )
  diag$steps[[length(diag$steps) + 1L]] <- step
  diag
}

litxr_diag_finish <- function(diag, status = "ok", details = list(), error = NULL) {
  diag$status <- as.character(status)[[1]]
  diag$ended_at <- format(litxr_diag_now(), tz = "UTC", usetz = TRUE)
  diag$elapsed_sec <- as.numeric(difftime(litxr_diag_now(), as.POSIXct(diag$started_at, tz = "UTC"), units = "secs"))
  if (!is.null(error) && nzchar(as.character(error)[[1]])) {
    diag$error <- as.character(error)[[1]]
  }
  if (length(details)) {
    diag$details <- details
  }
  diag
}

litxr_diag_emit <- function(diag, con = stderr()) {
  if (!isTRUE(diag$enabled)) {
    return(invisible(FALSE))
  }
  cat(
    "diagnostics=",
    jsonlite::toJSON(diag, auto_unbox = TRUE, null = "null", pretty = FALSE),
    "\n",
    file = con,
    sep = ""
  )
  invisible(TRUE)
}
