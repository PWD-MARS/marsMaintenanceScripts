df <- tibble(a = NA, b = 11:20, c = letters[1:10])

df$a[2][is.na(df$a[2])] <- 100

paste0("test #", df$a[2])


df |> dplyr::select(dtime = a, d = ncol(.))

accessdb_query <- paste0("select * from [", df$a[2], "] where [Standard Dtime] > #",df$a[2], "# ")
accessdb_query


date <- tibble(dates = c("2025-03-20 00:00:00", "2025-03-20 01:00:00", "2025-03-20 02:00:00", "2025-03-20 03:00:00"))


datev1 <- date |>
  mutate(date2 = ymd_hms(dates, tz = "America/New_York")) |>
  mutate(date3 = as.POSIXct(date2, tz = "America/New_York"))

date3 <- as.POSIXct(date2 + second(1), "tz = America/New_York")
