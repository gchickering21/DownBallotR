test_that("nc snapshot loads and has expected columns", {
  df <- get_nc_snapshot()
  expect_true(nrow(df) > 0)
  expect_true("election_date" %in% names(df))
  expect_true("votes" %in% names(df))
})
