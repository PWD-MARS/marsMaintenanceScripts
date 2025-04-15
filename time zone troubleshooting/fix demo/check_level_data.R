library(tidyverse)
library(readxl)
library(DBI)
library(fs)
library(pool)


years <- 2022
# # Check ow_leveldata_raw
# 
# # Pull data from sandbox database with up-to-date data
# conn <- dbPool(
#   drv = RPostgres::Postgres(),
#   host = "PWDMARSDBS1",
#   port = 5434,
#   dbname = "mars_testdeploy",
#   user= Sys.getenv("shiny_uid"),
#   password = Sys.getenv("shiny_pwd"),
#   timezone = NULL)
# # Get data from MARS
# # conn <- dbConnect(odbc::odbc(), dsn = "mars", uid = Sys.getenv("shiny_uid"), pwd = Sys.getenv("shiny_pwd"))
# fromSandbox <- dbGetQuery(conn, "SELECT * FROM data.tbl_ow_leveldata_raw WHERE date_part('year', dtime_est) = '2024'")
# poolClose(conn)
# 
# conn <- dbPool(
#   drv = RPostgres::Postgres(),
#   host = "PWDMARSDBS1",
#   port = 5434,
#   dbname = "mars_data",
#   user= Sys.getenv("shiny_uid"),
#   password = Sys.getenv("shiny_pwd"),
#   timezone = NULL)
# 
# fromMars <- dbGetQuery(conn, "SELECT * FROM data.tbl_ow_leveldata_raw WHERE dtime_est >= '2024-01-01'")
# poolClose(conn)

# ow_uid 663 on 5/14 has the 59 seconds


level_comp <- function(year) {
  conn <- dbPool(
    drv = RPostgres::Postgres(),
    host = "PWDMARSDBS1",
    port = 5434,
    dbname = "mars_testdeploy",
    user= Sys.getenv("shiny_uid"),
    password = Sys.getenv("shiny_pwd"),
    # Pulls with local time
    timezone = NULL)
  # Get data from MARS
  # conn <- dbConnect(odbc::odbc(), dsn = "mars", uid = Sys.getenv("shiny_uid"), pwd = Sys.getenv("shiny_pwd"))
  queryMars <- sprintf("SELECT * FROM data.tbl_ow_leveldata_raw WHERE date_part('year', dtime_est) = %d",
                   year)
  fromMars <- dbGetQuery(conn, queryMars)
  fromMars <- fromMars |>
    mutate(dtime_local = if_else(second(dtime_est) == 59, dtime_est + second(1), dtime_est )) |>
    select(dtime_local, level_ft, ow_uid)
  

  # queryTest <- sprintf("SELECT * FROM data.test_tbl_ow_leveldata_raw WHERE date_part('year', dtime_est) = %d",
  #                  year)
  # fromTest <- dbGetQuery(conn, queryTest) |> select(dtime_local, level_ft, ow_uid)
  # poolClose(conn)
  # 
  # diffs <- symdiff(fromMars, fromTest)
 
  
}

diffy <- level_comp(years)

differs <- diffy |> filter(ow_uid == 661)

queryTest <- sprintf("SELECT * FROM data.test_tbl_ow_leveldata_raw WHERE date_part('year', dtime_local) = %d",
                 years)
fromTest <- dbGetQuery(conn, queryTest) |> select(dtime_local, level_ft, ow_uid)
poolClose(conn)

tester <- fromTest |> filter(ow_uid == 661)
