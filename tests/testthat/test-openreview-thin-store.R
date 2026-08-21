td <- tempfile("litxr-openreview-")
dir.create(td)
old_litxr_data_root <- Sys.getenv("LITXR_DATA_ROOT", unset = NA_character_)
Sys.setenv(LITXR_DATA_ROOT = td)
on.exit({
  if (is.na(old_litxr_data_root)) Sys.unsetenv("LITXR_DATA_ROOT") else Sys.setenv(LITXR_DATA_ROOT = old_litxr_data_root)
}, add = TRUE)

litxr::litxr_init()
cfg <- litxr::litxr_read_config()
registered <- litxr:::.litxr_register_manual_collection(
  cfg,
  "openreview",
  collection_title = "OpenReview",
  remote_channel = "openreview",
  collection_type = "openreview"
)
records <- litxr::litxr_add_refs(
  data.frame(
    ref_id = "openreview:s9z0HzWJJp",
    source = "openreview",
    source_id = "s9z0HzWJJp",
    entry_type = "inproceedings",
    title = "SocioDojo",
    abstract = "Fixture abstract.",
    url = "https://openreview.net/forum?id=s9z0HzWJJp",
    stringsAsFactors = FALSE
  ),
  collection_id = "openreview",
  config = registered$cfg,
  auto_register = FALSE
)
expect_identical(records$ref_id[[1L]], "openreview:s9z0HzWJJp")

sync <- litxr::litxr_sync_thin_ref_stores_from_json(registered$cfg, collection_ids = "openreview")
openreview_store <- data.table::as.data.table(fst::read_fst(litxr:::.litxr_ref_openreview_path(registered$cfg), as.data.table = TRUE))
expect_identical(names(openreview_store), c("openreview_id", "collection_index", "json_filename"))
expect_identical(openreview_store$openreview_id[[1L]], "s9z0HzWJJp")
expect_identical(sync$row_counts$ref_openreview, 1L)

locations <- litxr:::.litxr_ref_json_locations_from_thin_stores(registered$cfg, "openreview:s9z0HzWJJp")
expect_identical(locations$ref_id[[1L]], "openreview:s9z0HzWJJp")
expect_true(file.exists(locations$json_path[[1L]]))

prompt <- litxr::litxr_llm_digest_prompt("openreview:s9z0HzWJJp", config = registered$cfg)
expect_match(prompt, "https://openreview.net/pdf\\?id=s9z0HzWJJp", fixed = FALSE)

bib_path <- tempfile(fileext = ".bib")
written <- litxr::write_bibtex_entries(bib_path, "openreview:s9z0HzWJJp", config = registered$cfg)
expect_identical(written$status, "ok")
expect_match(paste(readLines(bib_path, warn = FALSE), collapse = "\n"), "SocioDojo", fixed = TRUE)
