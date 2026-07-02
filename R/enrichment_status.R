.litxr_build_enrichment_status_index <- function(cfg) {
  refs <- .litxr_authoritative_project_records(cfg)
  if (!nrow(refs)) {
    return(data.table::data.table(
      ref_id = character(),
      has_md = logical(),
      has_llm_digest = logical(),
      updated_at = character()
    ))
  }

  llm_index <- .litxr_read_llm_digest_index(cfg)
  llm_keys <- if (nrow(llm_index)) llm_index$ref_id else character()
  ref_keys <- vapply(as.character(refs$ref_id), .litxr_llm_digest_index_key, character(1))

  status <- data.table::data.table(
    ref_id = refs$ref_id,
    has_md = vapply(refs$ref_id, function(x) file.exists(.litxr_md_path(cfg, x)), logical(1)),
    has_llm_digest = !is.na(ref_keys) & ref_keys %in% llm_keys,
    updated_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
  )

  status
}

.litxr_write_enrichment_status_index <- function(cfg) {
  .litxr_ensure_project_index_dir(cfg)
  status <- .litxr_build_enrichment_status_index(cfg)
  fst::write_fst(as.data.frame(status), .litxr_enrichment_status_index_path(cfg))
  invisible(.litxr_enrichment_status_index_path(cfg))
}

.litxr_update_enrichment_status_ref <- function(cfg, ref_id) {
  path <- .litxr_enrichment_status_index_path(cfg)
  if (!file.exists(path)) {
    return(.litxr_write_enrichment_status_index(cfg))
  }

  status <- fst::read_fst(path, as.data.table = TRUE)
  if (!nrow(status)) {
    return(.litxr_write_enrichment_status_index(cfg))
  }

  row <- data.table::data.table(
    ref_id = as.character(ref_id),
    has_md = file.exists(.litxr_md_path(cfg, ref_id)),
    has_llm_digest = !is.null(.litxr_llm_digest_index_lookup(cfg, ref_id)),
    updated_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
  )

  hit <- match(row$ref_id[[1]], status$ref_id)
  if (is.na(hit)) {
    status <- data.table::rbindlist(list(status, row), fill = TRUE)
  } else {
    data.table::set(status, i = hit, j = "has_md", value = row$has_md[[1]])
    data.table::set(status, i = hit, j = "has_llm_digest", value = row$has_llm_digest[[1]])
    data.table::set(status, i = hit, j = "updated_at", value = row$updated_at[[1]])
  }

  fst::write_fst(as.data.frame(status), path)
  invisible(path)
}

.litxr_read_enrichment_status_index <- function(cfg) {
  path <- .litxr_enrichment_status_index_path(cfg)
  if (!file.exists(path)) {
    .litxr_write_enrichment_status_index(cfg)
  }
  fst::read_fst(path, as.data.table = TRUE)
}
