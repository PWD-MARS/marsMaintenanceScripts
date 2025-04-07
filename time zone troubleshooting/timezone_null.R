library(tidyverse)
library(readxl)
library(DBI)
library(fs)
library(pool)

# Import excel data
#cs1_24q1 <- read_excel("Data/1267-2-1_CS1_24Q1_15min_QAQC_20240509_SPM.xlsx")

conn <- dbPool(
  drv = RPostgres::Postgres(),
  host = "192.168.131.120",
  port = 5432,
  dbname = "CentralDB",
  user= Sys.getenv("central_uid"),
  password = Sys.getenv("central_pwd"),
  timezone = NULL)


# Get data from centralDB
# conn <- dbConnect(odbc::odbc(), dsn = "CentralDB", uid = Sys.getenv("central_uid"), 
#                   pwd = Sys.getenv("central_pwd"))
fromCentral <- dbGetQuery(conn, "SELECT * FROM pwdrg.tblmodelrain")
poolClose(conn)

# Organize data from centralDB
names(fromCentral)
filtered_central <- fromCentral  |>
  select(GaugeNo, DateTime, Rainfall)
names(filtered_central) <- c("gage", "time", "rainfall")

conn <- dbPool(
  drv = RPostgres::Postgres(),
  host = "PWDMARSDBS1",
  port = 5434,
  dbname = "monica_seriestest",
  user= Sys.getenv("shiny_uid"),
  password = Sys.getenv("shiny_pwd"),
  timezone = NULL)
# Get data from MARS
# conn <- dbConnect(odbc::odbc(), dsn = "mars", uid = Sys.getenv("shiny_uid"), pwd = Sys.getenv("shiny_pwd"))
fromMars <- dbGetQuery(conn, "SELECT * FROM tbl_gage_rain")
poolClose(conn)


# Organize Data from MARS
filtered_mars <- fromMars |> select(gage_uid, dtime_local, rainfall_in)
names(filtered_mars) <- c("gage", "time", "rainfall")

# Check differences
differences <- symdiff(filtered_mars, filtered_central)

gage_diffs <- differences |> group_by(gage) |> summarize(n())

differences |> filter(gage <= 35) |> ggplot(aes(x = gage)) +
  geom_bar()

differences |> filter(gage == c(36, 37)) |> ggplot() +
  geom_bar(aes(x = gage)) + 
  scale_x_discrete(limits = c(36, 37))
# Differences between MARS and CentalDB
print(differences[,2])

# conn <- dbPool(
#   drv = RPostgres::Postgres(),
#   host = "PWDMARSDBS1",
#   port = 5434,
#   dbname = "Jon_sandbox",
#   user= Sys.getenv("shiny_uid"),
#   password = Sys.getenv("shiny_pwd"),
#   timezone = NULL)
# 
# dbWriteTable(conn, "tbl_test_date", filtered_mars |> select(time, rainfall), append = TRUE)
