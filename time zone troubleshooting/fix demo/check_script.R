library(tidyverse)
library(readxl)
library(DBI)
library(fs)
library(pool)

year <- 2022

# Connect to DB and pull data with local time
conn <- dbPool(
  drv = RPostgres::Postgres(),
  host = "PWDMARSDBS1",
  port = 5434,
  dbname = "mars_testdeploy",
  user= Sys.getenv("shiny_uid"),
  password = Sys.getenv("shiny_pwd"),
  # Pulls with local time
  timezone = "Etc/GMT+5")
# Get data from MARS
# conn <- dbConnect(odbc::odbc(), dsn = "mars", uid = Sys.getenv("shiny_uid"), pwd = Sys.getenv("shiny_pwd"))
queryMars <- sprintf("SELECT * FROM data.tbl_ow_leveldata_raw WHERE date_part('year', dtime_est) = %d",
                     year)
fromMars <- dbGetQuery(conn, queryMars)
fromMars <- fromMars |>
  mutate(dtime_local = if_else(second(dtime_est) == 59, dtime_est + dseconds(1), 
                               dtime_est),
         dtime_local = with_tz(dtime_local), tzone = "America/New_York") |>
  select(dtime_local, level_ft, ow_uid)


queryTest <- sprintf("SELECT * FROM data.test_tbl_ow_leveldata_raw WHERE date_part('year', dtime_local) = %d",
                 year)
fromTest <- dbGetQuery(conn, queryTest) |> select(dtime_local, level_ft, ow_uid)
poolClose(conn)

diffs <- setdiff(fromMars, fromTest)
diffs1 <- setdiff(fromTest, fromMars)

marchTest <- fromTest |> filter(between(dtime_local, ymd("2022-03-13"), ymd("2022-03-14")) & ow_uid == 661) |> arrange()
marchMars <- fromMars |> filter(between(dtime_local, ymd("2022-03-13"), ymd("2022-03-14")) & ow_uid == 661) |> arrange()


test41923 <- fromTest |> filter(level_ft == "4.1923")
mars41923 <- fromMars |> filter(level_ft == "4.1923")
