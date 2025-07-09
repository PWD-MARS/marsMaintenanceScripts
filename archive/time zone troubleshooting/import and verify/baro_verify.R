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
  dbname = "mars_data",
  user= Sys.getenv("admin_uid"),
  password = Sys.getenv("admin_pwd"),
  timezone = "EST") #To pull in time series data without force-coercing it to local time, we need to set the TZ to EST here

mars_deploy <- dbPool(
  drv = RPostgres::Postgres(),
  host = "PWDMARSDBS1",
  port = 5434,
  dbname = "demo_deployment",
  user= Sys.getenv("admin_uid"),
  password = Sys.getenv("admin_pwd"),
  timezone = NULL)

#Pulling the old data, without coercion
old_barodata <- dbGetQuery(mars_test, "select * from data.tbl_baro")

#Pulling the new data
new_barodata <- dbGetQuery(mars_deploy, "select * from data.test_tbl_baro")

#We must...
#Coerce the new data from America/New_York to EST, undoing the spring-forwards
#Bump the *old* data up one second when the sensor measured on the :59
#Strip the fast-redeploy errors from the old data, since they have been stripped in the new data

#And then we can compare the data

#Coerce time zone
new_barodata <- mutate(new_barodata, dtime_est = with_tz(dtime, tzone = "EST"))

#Bump the 59th second
old_barodata <- old_barodata %>%
  mutate(secondbump = (second(dtime_est) == 59)) %>% #Calculate whether the seconds place needs to be bumped +1 second
  mutate(dtime_bumped = as.POSIXct(ifelse(secondbump, dtime_est + dseconds(1), dtime_est), tz = "EST"))

#Fast redeploy errors are calculated on a per-baro_rawfile_uid basis, so we need to do a group operation
old_barodata <- old_barodata %>% group_by(baro_rawfile_uid) %>%
  mutate(fast_redeploy_error = duplicated(dtime_bumped)) %>%
  ungroup()


#Validation:

#Are the only extra rows in old_leveldata due to unstripped fast redeploy errors?
nrow(new_barodata) == (nrow(old_barodata) - sum(old_barodata$fast_redeploy_error))

#Strip the FREs
old_barodata <- filter(old_barodata, fast_redeploy_error == FALSE)

#Prepare the time series for comparison
#The primary keys will not match, so we will remove that field from both frames
#The fields used to compose the FRE check in the old data will also be removed
#The dtime field will be removed from the new data, since we are only concerned with the pre-spring-forwards data
#Finally, the dtime_bumped in the old data will be renamed dtime_est so it can be compared to the new data

#We will also sort the data, ordering them by ow_uid, and dtime_est within them

old_barodata <- old_barodata %>%
  select(baro_rawfile_uid, dtime_est = dtime_bumped, baro_psi, temp_f) %>%
  arrange(baro_rawfile_uid, dtime_est)

new_barodata <- new_barodata %>%
  select(baro_rawfile_uid, dtime_est, baro_psi, temp_f) %>%
  arrange(baro_rawfile_uid, dtime_est)

comp <- symdiff(old_barodata, new_barodata)

