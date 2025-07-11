#Database Stuff
library(RPostgres)
library(tidyverse)
library(lubridate)
library(pool)
library(digest)

mars_old <- dbPool(
  drv = RPostgres::Postgres(),
  host = "PWDMARSDBS1",
  port = 5434,
  dbname = "mars_data",
  user= Sys.getenv("admin_uid"),
  password = Sys.getenv("admin_pwd"),
  timezone = "UTC") #To pull in time series data without force-coercing it to local time, we need to set the TZ to UTC here

mars_deploy <- dbPool(
  drv = RPostgres::Postgres(),
  host = "PWDMARSDBS1",
  port = 5434,
  dbname = "mars_data",
  user= Sys.getenv("admin_uid"),
  password = Sys.getenv("admin_pwd"),
  timezone = NULL)

baro_rawfiles <- dbGetQuery(mars_old, "select distinct baro_rawfile_uid from data.tbl_baro order by baro_rawfile_uid asc")

for(i in 1:nrow(baro_rawfiles)){
  print(paste("Pulling Rawfile", baro_rawfiles$baro_rawfile_uid[i]))
  prod_barodata <- dbGetQuery(mars_old, paste0("select dtime_est, baro_rawfile_uid, baro_psi, temp_f from data.tbl_baro where baro_rawfile_uid = ", baro_rawfiles$baro_rawfile_uid[i]))
  
  prod_barodata$dtime_est <- force_tz(prod_barodata$dtime_est, tz = "EST") #It comes in as UTC, we must make it EST
  
  prod_barodata <- prod_barodata %>%
    mutate(secondbump = (second(dtime_est) == 59)) %>% #Calculate whether the seconds place needs to be bumped +1 second
    mutate(dtime_bumped = as.POSIXct(ifelse(secondbump, dtime_est + dseconds(1), dtime_est), tz = "EST")) %>%
    mutate(fast_redeploy_error = duplicated(dtime_bumped))#In certain very rare circumstances, when a sensor with a :59
  #deployment error was collected, redeployed before the next sample
  #interval would have been taken, *and* was redeployed without the :59
  #deployment error, this can result in duplicated timestamps.
  #in this case, we need to pick one to discard. We will discard the first
  #record of the subsequent deployment.
  
  if(any(prod_barodata$fast_redeploy_error)){
    print(paste("Fast Redeploy Errors: ", sum(prod_barodata$fast_redeploy_error)))
  }
  
  prod_barodata <- prod_barodata %>%
    filter(fast_redeploy_error == FALSE) 
  
  file_barodata <- prod_barodata %>%
    mutate(dtime = with_tz(dtime_bumped, tz = "America/New_York")) %>%
    transmute(dtime,
           baro_psi,
           temp_f,
           baro_rawfile_uid) 
  
  dbWriteTable(mars_deploy, RPostgres::Id(schema = "data", table = "test_tbl_baro"), file_barodata, append = TRUE, row.names=FALSE)
  
}