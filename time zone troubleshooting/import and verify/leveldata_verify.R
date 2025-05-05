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
  timezone = "EST") #To pull in time series data without force-coercing it to local time, we need to set the TZ to EST here

mars_deploy <- dbPool(
  drv = RPostgres::Postgres(),
  host = "PWDMARSDBS1",
  port = 5434,
  dbname = "mars_testdeploy",
  user= Sys.getenv("admin_uid"),
  password = Sys.getenv("admin_pwd"),
  timezone = NULL)

#Pulling the old data, without coercion
old_leveldata <- dbGetQuery(mars_test, "select * from data.tbl_ow_leveldata_raw")

#Pulling the new data
new_leveldata <- dbGetQuery(mars_deploy, "select * from data.test_tbl_ow_leveldata_raw")

#We must...
  #Coerce the new data from America/New_York to EST, undoing the spring-forwards
  #Bump the *old* data up one second when the sensor measured on the :59
  #Strip the fast-redeploy errors from the old data, since they have been stripped in the new data

#And then we can compare the data

#Coerce time zone
new_leveldata <- mutate(new_leveldata, dtime_est = with_tz(dtime_local, tzone = "EST"))

#Bump the 59th second
old_leveldata <- old_leveldata %>%
  mutate(secondbump = (second(dtime_est) == 59)) %>% #Calculate whether the seconds place needs to be bumped +1 second
  mutate(dtime_bumped = as.POSIXct(ifelse(secondbump, dtime_est + dseconds(1), dtime_est), tz = "EST"))

#Fast redeploy errors are calculated on a per-ow_uid basis, so we need to do a group operation
old_leveldata <- old_leveldata %>% group_by(ow_uid) %>%
  mutate(fast_redeploy_error = duplicated(dtime_bumped)) %>%
  ungroup()


#Validation:

#Are the only extra rows in old_leveldata due to unstripped fast redeploy errors?
nrow(new_leveldata) == (nrow(old_leveldata) - sum(old_leveldata$fast_redeploy_error))

#Strip the FREs
old_leveldata <- filter(old_leveldata, fast_redeploy_error == FALSE)

#Prepare the time series for comparison
  #The primary keys will not match, so we will remove that field from both frames
  #The fields used to compose the FRE check in the old data will also be removed
  #The dtime_local field will be removed from the new data, since we are only concerned with the pre-spring-forwards data
  #Finally, the dtime_bumped in the old data will be renamed dtime_est so it can be compared to the new data

  #We will also sort the data, ordering them by ow_uid, and dtime_est within them

old_leveldata <- old_leveldata %>%
  select(ow_uid, dtime_est = dtime_bumped, level_ft, date_added) %>%
  arrange(ow_uid, dtime_est)

new_leveldata <- new_leveldata %>%
  select(ow_uid, dtime_est, level_ft, date_added) %>%
  arrange(ow_uid, dtime_est)

comp <- symdiff(old_leveldata, new_leveldata)

#S.2 Verifying groundwater data
#Pulling the old data, without coercion
old_depthdata <- dbGetQuery(mars_test, "select * from data.tbl_gw_depthdata_raw")

#Pulling the new data
new_depthdata <- dbGetQuery(mars_deploy, "select * from data.test_tbl_gw_depthdata_raw")

#We must...
#Coerce the new data from America/New_York to EST, undoing the spring-forwards
#Bump the *old* data up one second when the sensor measured on the :59
#Strip the fast-redeploy errors from the old data, since they have been stripped in the new data

#And then we can compare the data

#Coerce time zone
new_depthdata <- mutate(new_depthdata, dtime_est = with_tz(dtime_local, tzone = "EST"))

#Bump the 59th second
old_depthdata <- old_depthdata %>%
  mutate(secondbump = (second(dtime_est) == 59)) %>% #Calculate whether the seconds place needs to be bumped +1 second
  mutate(dtime_bumped = as.POSIXct(ifelse(secondbump, dtime_est + dseconds(1), dtime_est), tz = "EST"))

#Fast redeploy errors are calculated on a per-ow_uid basis, so we need to do a group operation
old_depthdata <- old_depthdata %>% group_by(ow_uid) %>%
  mutate(fast_redeploy_error = duplicated(dtime_bumped)) %>%
  ungroup()


#Validation:

#Are the only extra rows in old_depthdata due to unstripped fast redeploy errors?
nrow(new_depthdata) == (nrow(old_depthdata) - sum(old_depthdata$fast_redeploy_error))

#Strip the FREs
old_depthdata <- filter(old_depthdata, fast_redeploy_error == FALSE)

#Prepare the time series for comparison
#The primary keys will not match, so we will remove that field from both frames
#The fields used to compose the FRE check in the old data will also be removed
#The dtime_local field will be removed from the new data, since we are only concerned with the pre-spring-forwards data
#Finally, the dtime_bumped in the old data will be renamed dtime_est so it can be compared to the new data

#We will also sort the data, ordering them by ow_uid, and dtime_est within them

old_depthdata <- old_depthdata %>%
  select(ow_uid, dtime_est = dtime_bumped, depth_ft) %>%
  arrange(ow_uid, dtime_est)

new_depthdata <- new_depthdata %>%
  select(ow_uid, dtime_est, depth_ft) %>%
  arrange(ow_uid, dtime_est)

comp <- symdiff(old_depthdata, new_depthdata)


