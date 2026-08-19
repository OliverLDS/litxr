td <- tempfile("litxr-manual-isbn-")
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
  "manual_books",
  collection_title = "Manual Books",
  remote_channel = "isbn",
  collection_type = "manual_isbn"
)

records <- litxr::litxr_add_refs(
  data.frame(
    source = "manual",
    entry_type = "book",
    title = "Artificial Intelligence: A Modern Approach",
    authors = "Stuart Russell; Peter Norvig",
    year = 2021L,
    isbn = "9780134610993",
    stringsAsFactors = FALSE
  ),
  collection_id = "manual_books",
  config = registered$cfg,
  auto_register = FALSE
)
expect_identical(records$ref_id[[1L]], "isbn:9780134610993")

litxr::litxr_sync_thin_ref_stores_from_json(registered$cfg, collection_ids = "manual_books")
isbn_store <- data.table::as.data.table(fst::read_fst(litxr:::.litxr_ref_isbn_path(registered$cfg), as.data.table = TRUE))
expect_identical(names(isbn_store), c("isbn", "collection_index", "json_filename"))
expect_identical(isbn_store$isbn[[1L]], "9780134610993")
expect_true(nzchar(isbn_store$json_filename[[1L]]))
