#Database Stuff
library(RPostgres)
library(tidyverse)
library(lubridate)
library(pool)
library(digest)

mars_test <- dbPool(
  drv = RPostgres::Postgres(),
  host = "PWDMARSDBS1",
  port = 5434,
  dbname = "mars_testdeploy",
  user= Sys.getenv("admin_uid"),
  password = Sys.getenv("admin_pwd"),
  timezone = "UTC") #To pull in time series data without force-coercing it to local time, we need to set the TZ to UTC here

mars_deploy <- dbPool(
  drv = RPostgres::Postgres(),
  host = "PWDMARSDBS1",
  port = 5434,
  dbname = "mars_testdeploy",
  user= Sys.getenv("admin_uid"),
  password = Sys.getenv("admin_pwd"),
  timezone = NULL)

#We have three goals with importing this data.
  # We need to spring-forwards the historical water level data. I won't elaborate on this because we've discussed it extensively and the methods for doing so are documented in timezonefix.R
  # The second goal is not clobbering the "entry date" metadata in tbl_ow_leveldata_raw. This historical data must be preserved because it is used in the QA and quarterly reporting process.
  #The third goal is bumping forward measurements that take place on the :59 second mark to the :00 mark of the next minute
  #This is important for daylight savings time reasons because it will fail to spring-forwards and fall-back on the time that is
  #One second before the DST boundary
  
  
#S.1 Level data import
  prod_ows <- dbGetQuery(mars_test, "select distinct ow_uid from data.tbl_ow_leveldata_raw order by ow_uid asc")
  
  for(i in 1:nrow(prod_ows)){
   print(paste("Pulling OW", prod_ows$ow_uid[i]))
    prod_leveldata <- dbGetQuery(mars_test, paste0("select dtime_est, ow_uid, level_ft, date_added from data.tbl_ow_leveldata_raw where ow_uid = ", prod_ows$ow_uid[i]))
    
    prod_leveldata$dtime_est <- force_tz(prod_leveldata$dtime_est, tz = "EST") #It comes in as UTC, we must make it EDT
    
    prod_leveldata <- prod_leveldata %>%
      mutate(secondbump = (second(dtime_est) == 59)) %>% #Calculate whether the seconds place needs to be bumped +1 second
      mutate(dtime_bumped = as.POSIXct(ifelse(secondbump, dtime_est + dseconds(1), dtime_est), tz = "EST")) %>%
      mutate(fast_redeploy_error = duplicated(dtime_bumped))#In certain very rare circumstances, when a sensor with a :59
                                                            #deployment error was collected, redeployed before the next sample
                                                            #interval would have been taken, *and* was redeployed without the :59
                                                            #deployment error, this can result in duplicated timestamps.
                                                            #in this case, we need to pick one to discard. We will discard the first
                                                            #record of the subsequent deployment.

    if(any(prod_leveldata$fast_redeploy_error)){
      print(paste("Fast Redeploy Errors: ", sum(prod_leveldata$fast_redeploy_error)))
    }
    
    prod_leveldata <- prod_leveldata %>%
      filter(fast_redeploy_error == FALSE) 
    
    file_leveldata <- prod_leveldata %>%
      mutate(dtime_local = with_tz(dtime_bumped, tz = "America/New_York")) %>%
      select(dtime_local,
             level_ft,
             ow_uid,
             date_added)
    
    dbWriteTable(mars_deploy, RPostgres::Id(schema = "data", table = "test_tbl_ow_leveldata_raw"), file_leveldata, append = TRUE, row.names=FALSE)

  }
  
#S.2 Groundwater data import
  '
  prod_gws <- dbGetQuery(mars_test, "select distinct ow_uid from data.tbl_gw_depthdata_raw order by ow_uid asc")
  
  for(i in 1:nrow(prod_gws)){
    print(paste("Pulling GW", prod_gws$ow_uid[i]))
    prod_depthdata <- dbGetQuery(mars_test, paste0("select dtime_est, ow_uid, depth_ft from data.tbl_gw_depthdata_raw where ow_uid = ", prod_gws$ow_uid[i]))
    
    prod_depthdata$dtime_est <- force_tz(prod_depthdata$dtime_est, tz = "EST") #It comes in as UTC, we must make it EDT
    
    prod_depthdata <- prod_depthdata %>%
      mutate(secondbump = (second(dtime_est) == 59)) %>% #Calculate whether the seconds place needs to be bumped +1 second
      mutate(dtime_bumped = as.POSIXct(ifelse(secondbump, dtime_est + dseconds(1), dtime_est), tz = "EST"))
    
    file_depthdata <- prod_depthdata %>%
      mutate(dtime_local = with_tz(dtime_bumped, tz = "America/New_York")) %>%
      select(dtime_local,
             depth_ft,
             ow_uid) #There is no date_added field in groundwater data
    
    dbWriteTable(mars_deploy, RPostgres::Id(schema = "data", table = "test_tbl_gw_depthdata_raw"), file_depthdata, append = TRUE, row.names=FALSE)
    
  }
  '