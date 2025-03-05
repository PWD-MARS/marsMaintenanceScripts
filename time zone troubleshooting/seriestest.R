#Database Stuff
library(odbc)
library(RPostgres)
library(tidyverse)
library(lubridate)


options(stringsAsFactors=FALSE)



marsDBCon_odbc <- dbConnect(odbc::odbc(), "monica_sandbox")

hhdbcon_odbc <- dbConnect(odbc::odbc(), "CentralDB")


marsDBCon_rpostgres <- RPostgres::dbConnect(RPostgres::Postgres(), 
                                            user = Sys.getenv("admin_uid"), 
                                            password = Sys.getenv("admin_pwd"), 
                                            host = "127.0.0.1",
                                            port = 5434,
                                            dbname = "monica_sandbox")

hhdbcon_rpostgres <- dbConnect(RPostgres::Postgres(),
                               user = "pwdguest",
                               password = "readonly",
                               host = "192.168.131.120",
                               port = 5432,
                               dbname = "CentralDB")

#Reading with odbc
  hhrain_odbc <- dbGetQuery(hhdbcon_odbc, "select * from pwdrg.tblModelRain limit 1")

  testframe <- data.frame(dtime_utc_notz = hhrain_odbc$DateTime,
                        dtime_local_notz = force_tz(hhrain_odbc$DateTime, "America/New_York"),
                        dtime_utc_tz = hhrain_odbc$DateTime,
                        dtime_local_tz = force_tz(hhrain_odbc$DateTime, "America/New_York"),
                        text_without_notz = as.character(hhrain_odbc$DateTime),
                        text_with_notz = paste0(as.character(hhrain_odbc$DateTime), "-05"),
                        text_without_tz = as.character(hhrain_odbc$DateTime),
                        text_with_tz = paste0(as.character(hhrain_odbc$DateTime), "-05"))

#Reading with rpostgres
  hhrain_rpostgres <- dbGetQuery(hhdbcon_rpostgres, "select * from pwdrg.tblModelRain limit 1")

  testframe2 <- data.frame(dtime_utc_notz = hhrain_rpostgres$DateTime,
                         dtime_local_notz = force_tz(hhrain_rpostgres$DateTime, "America/New_York"),
                         dtime_utc_tz = hhrain_rpostgres$DateTime,
                         dtime_local_tz = force_tz(hhrain_rpostgres$DateTime, "America/New_York"),
                         text_without_notz = as.character(hhrain_rpostgres$DateTime),
                         text_with_notz = paste0(as.character(hhrain_rpostgres$DateTime), "-05"),
                         text_without_tz = as.character(hhrain_rpostgres$DateTime),
                         text_with_tz = paste0(as.character(hhrain_rpostgres$DateTime), "-05"))


#reading and writing with odbc
  odbcframe <- mutate(testframe, read_package = "odbc", write_package = "odbc")
  dbWriteTable(marsDBCon_odbc, "seriestest", odbcframe, append = TRUE)

#ODBC package to read, rpostgres to write
  odbc_rpframe <- mutate(testframe, read_package = "odbc", write_package = "rpostgres")
  dbWriteTable(marsDBCon_rpostgres, "seriestest", odbc_rpframe, append = TRUE)

#RPostgres package to read, odbc to write
  rp_odbcframe <- mutate(testframe2, read_package = "rpostgres", write_package = "odbc")
  dbWriteTable(marsDBCon_odbc, "seriestest", rp_odbcframe, append = TRUE)

#Rpostgres to read and write
  rpframe <- mutate(testframe2, read_package = "rpostgres", write_package = "rpostgres")
  dbWriteTable(marsDBCon_rpostgres, "seriestest", rpframe, append = TRUE)


#Reread everything
seriestest_odbc <- dbGetQuery(marsDBCon_odbc, "select * from seriestest")
seriestest_rpostgres <- dbGetQuery(marsDBCon_rpostgres, "select * from seriestest")
