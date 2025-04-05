#Database Stuff
library(RPostgres)
library(tidyverse)
library(lubridate)
library(pool)
library(digest)

mars <- dbPool(
  drv = RPostgres::Postgres(),
  host = "PWDMARSDBS1",
  port = 5434,
  dbname = "mars_testdeploy",
  user= Sys.getenv("admin_uid"),
  password = Sys.getenv("admin_pwd"),
  timezone = NULL)

centraldb <- dbPool(
  drv = RPostgres::Postgres(),
  host = "192.168.131.120",
  port = 5432,
  dbname = "CentralDB",
  user= Sys.getenv("central_uid"),
  password = Sys.getenv("central_pwd"),
  timezone = NULL)

#S.1 Import rain gage data

#Read the rain gage data from the H&H database
hhrainfall_gage <- dbGetQuery(centraldb, "select * from pwdrg.tblModelRain") %>% 
  transmute(gage_uid = GaugeNo, 
            dtime_local = DateTime, #Comes in as America/New York time zone
            rainfall_in = round(Rainfall, 4)) %>%
  filter(!(gage_uid %in% c(36, 37))) #We don't track these ones! We need to change that someday.

#Create temporary escrow table to host our prospective written data
dbWriteTable(mars, "tbl_gage_escrow", hhrainfall_gage, temporary = TRUE, overwrite = TRUE)

#S.2 Verify rain gage data
  gage_verify <- dbGetQuery(mars, "select * from tbl_gage_escrow")
  
  #Count the rows
  rows_equal <- nrow(gage_verify) == nrow(hhrainfall_gage) #True!
  
  #Sum the rainfall across every gage since 1990
  mars_sums <- group_by(gage_verify, gage_uid) %>%
    summarize(mars_total_in = sum(rainfall_in))
  
  hh_sums <- group_by(hhrainfall_gage, gage_uid) %>%
    summarize(hh_total_in = sum(rainfall_in))
  
  sum_check <- left_join(mars_sums, hh_sums) %>%
    mutate(sum_check = mars_total_in == hh_total_in)
  
  sums_equal <- all(sum_check$sum_check) #All true!
  
  #Dplyr set comparison operation
  #Hashes are failing suddenly despite everything else being the same,
  #For reasons unfathomable to me
  sets_same <- setequal(hhrainfall_gage, gage_verify)
  
  #If every check succeeds, append the escrow data into the main table
  if(all(rows_equal, sums_equal, sets_same)){
    dbExecute(mars, "insert into data.test_tbl_gage_rain (gage_uid, dtime_local, rainfall_in)
              select gage_uid, dtime_local, rainfall_in from tbl_gage_escrow")
  }

#S.3 Calculate Event Metadata
library(pwdgsi)
  
  #Pull rain information
  mars_rain <- dbGetQuery(mars, "select * from data.test_tbl_gage_rain")
  
  #Tag rainfall time series with event markers
  mars_rain_tagged <- mars_rain %>% 
    group_by(gage_uid) %>%
    arrange(dtime_local) %>% 
    mutate(event_id = marsDetectEvents(dtime_local, rainfall_in)) %>%
    #Drop the last "complete" event in case it straddles the month boundary
    #It will get processed the when the next batch of data comes in
    filter(!is.na(event_id), event_id != max(event_id, na.rm = TRUE)) %>%
    ungroup
  
  rain_newevents <- mars_rain_tagged %>%
    group_by(gage_uid, event_id) %>%
    summarize(records = n(),
              eventdatastart_local = first(dtime_local),
              eventdataend_local = last(dtime_local),
              eventduration_hr = marsStormDuration_hr(dtime_local),
              eventpeakintensity_inhr = marsStormPeakIntensity_inhr(dtime_local, rainfall_in),
              eventavgintensity_inhr = marsStormAverageIntensity_inhr(dtime_local, rainfall_in),
              eventdepth_in = marsStormDepth_in(rainfall_in)) %>%
    select(-event_id) %>%
    ungroup %>%
    rowwise %>%
    mutate(md5hash = digest(paste(gage_uid, records, eventdatastart_local, eventdataend_local, eventduration_hr, eventpeakintensity_inhr, eventavgintensity_inhr, eventdepth_in), algo = "md5"))

  #To verify that we have correctly calculated all of the rain, we will sum the 
    #total rain contained in marked events and the total rain outside of marked events
    #this "outside" rain either doesn't meet the minimum depth threshold for event detection
    #or comes at the end of the time series, when we can't yet verify that the rain event won't continue
    #into the next batch of data
  mars_rain_verify <- mars_rain %>% 
    group_by(gage_uid) %>%
    arrange(dtime_local) %>% 
    mutate(event_id = marsDetectEvents(dtime_local, rainfall_in)) %>%
    #For verification, do not purge non-event rain
    #And instead of dropping the last event, set its ID to NA
    mutate(event_id = ifelse(event_id == max(event_id, na.rm = TRUE), NA, event_id)) %>%
    ungroup
  
  mars_stats_verify <- mars_rain_verify %>% 
    mutate(orphan = is.na(event_id)) %>% #orphans are outside of marked rain events
    group_by(orphan) %>%
    summarize(depth_in = sum(rainfall_in))
  
  #Round to the nearest 100th of an inch, because that's what pwdgsi does
  round(mars_stats_verify$depth_in[1], 2) == round(sum(rain_newevents$eventdepth_in), 2) #True!
  
  #Events are good, send the events to the DB in an escrow table
  dbWriteTable(mars, "tbl_event_escrow", rain_newevents, temporary = TRUE, overwrite = TRUE)

  #Reread and verify a lack of drift
  mars_events_verify <- dbGetQuery(mars, "select * from tbl_event_escrow")
  
  #Compute hashes
  mars_events_verify <- mars_events_verify %>%
    rowwise %>%
    mutate(md5hash_verify = digest(paste(gage_uid, records, eventdatastart_local, eventdataend_local, eventduration_hr, eventpeakintensity_inhr, eventavgintensity_inhr, eventdepth_in), algo = "md5")) %>%
    mutate(hashes_match = md5hash_verify == md5hash)
  
  table(mars_events_verify$hashes_match) #All true!

  if(all(mars_events_verify$hashes_match)){
    dbExecute(mars, "insert into data.test_tbl_gage_event (gage_uid, eventdatastart_local, eventdataend_local, eventduration_hr, eventpeakintensity_inhr, eventavgintensity_inhr, eventdepth_in)
              select gage_uid, eventdatastart_local, eventdataend_local, eventduration_hr, eventpeakintensity_inhr, eventavgintensity_inhr, eventdepth_in from tbl_event_escrow")
  }

  
    
  