#Database Stuff
library(RPostgres)
library(tidyverse)
library(lubridate)
library(pool)
library(digest)
library(padr)

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

  
#S.4 Radar rainfall import
  #Assess raw files in our DB and H&H
  db_rawfiles <- dbGetQuery(mars, "select * from admin.test_tbl_radar_rawfile")
  hh_rawfiles <- dbGetQuery(centraldb, "select yearmon, path as filepath from radar2sheds.garr_inventory") %>%
    mutate(yearmon = str_replace(yearmon, "-", "")) %>%
    filter(yearmon > "201401") %>%
    mutate(filepath = str_replace(filepath, "M:\\\\", "//pwdoows/OOWS/Modeling/")) %>% #Replace the drive letter
    mutate(filepath = str_replace(filepath, "M:/", "//pwdoows/OOWS/Modeling/")) %>% #Newer file paths are formatted differently
    mutate(filepath = str_replace(filepath, "\\\\", "/")) %>% #Replace the generic \ with / for path separation
    mutate(filepath = str_replace(filepath, "bases\\\\", "bases/")) %>% #This one doesn't match in the above regex, somehow
    mutate(filepath = str_replace(filepath, "fall\\\\", "fall/")) #Neither does this one

  #Which files don't we have
  new_rawfiles <- filter(hh_rawfiles, !(filepath %in% db_rawfiles$filepath)) 
  
  #only cells in Philly
  phillycells <- dbGetQuery(mars, "select radar_uid from admin.tbl_radar")
  
  #Unzip the files
  dir.create("~/radarrain", showWarnings = FALSE)
  for(i in 1:nrow(new_rawfiles)){
    if(nrow(new_rawfiles) == 0){break}
    file.copy(from = new_rawfiles$filepath[i], to = "~/radarrain", overwrite = FALSE)
    unzip(paste0("~/radarrain/", basename(new_rawfiles$filepath[i])), exdir = "~/radarrain", files = paste0(new_rawfiles$yearmon[i], ".csv"))
    file.remove(paste0("~/radarrain/", basename(new_rawfiles$filepath[i])))
  }
  
  #read the files
  newradarfiles <- list.files(path = "~/radarrain", full.names = TRUE, pattern = "\\.csv$")
  newradardata <- data.frame(dtime_edt = NULL, radar_uid = NULL, rainfall_in = NULL)
  for(i in 57:length(newradarfiles)){
    if(length(newradarfiles) == 0){break}
    print(paste0("reading ", newradarfiles[i]))
    radarfile <- read.csv(newradarfiles[i], header = FALSE)
    colnames(radarfile) <- c("dtime_raw", "tz", "radar_uid", "rainfall_in")
    radarfile <- radarfile %>% 
      filter(radar_uid %in% phillycells$radar_uid) %>%
      filter(rainfall_in > 0) %>%
      mutate(dtime_local = ymd_hm(dtime_raw, tz = "America/New_York")) %>%
      select(radar_uid, rainfall_in, dtime_local)
    
    radarinterval <- get_interval(radarfile$dtime_local)
    print(paste("File", basename(newradarfiles[i]), "has datetime interval", radarinterval))
    
    if(radarinterval != "15 min"){
      radarfile <- thicken(radarfile, "15 min", rounding = "up", ties_to_earlier = TRUE) %>%
        group_by(dtime_local_15_min, radar_uid) %>%
        summarize(rainfall_in = sum(rainfall_in)) %>%
        select(radar_uid, rainfall_in, dtime_local = dtime_local_15_min)
    }
    
    tryCatch(expr = {
      success <<- FALSE
      print(paste("Writing", newradarfiles[i]))
      dbWriteTable(mars, RPostgres::Id(schema = "data", table = "test_tbl_radar_rain"), radarfile, append= TRUE, row.names = FALSE)
      success <<- TRUE
    },
    error = function(e) {
      print(e)
    })
    
    if(success){
      successful_file <- new_rawfiles[i, ]
      dbWriteTable(mars, RPostgres::Id(schema = "admin", table = "test_tbl_radar_rawfile"), successful_file, append= TRUE, row.names = FALSE)
    }
        
        file.remove(newradarfiles[i])
  }

#S.5 Calculate Radar Event Metadata

  #Pull rain information
  mars_rain <- dbGetQuery(mars, "select * from data.test_tbl_radar_rain")
  
  #Tag rainfall time series with event markers
  mars_rain_tagged <- mars_rain %>% 
    group_by(radar_uid) %>%
    arrange(dtime_local) %>% 
    mutate(event_id = marsDetectEvents(dtime_local, rainfall_in)) %>%
    #Drop the last "complete" event in case it straddles the month boundary
    #It will get processed the when the next batch of data comes in
    filter(!is.na(event_id), event_id != max(event_id, na.rm = TRUE)) %>%
    ungroup
  
  rain_newevents <- mars_rain_tagged %>%
    group_by(radar_uid, event_id) %>%
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
    mutate(md5hash = digest(paste(radar_uid, records, eventdatastart_local, eventdataend_local, eventduration_hr, eventpeakintensity_inhr, eventavgintensity_inhr, eventdepth_in), algo = "md5"))
  
  #To verify that we have correctly calculated all of the rain, we will sum the 
  #total rain contained in marked events and the total rain outside of marked events
  #this "outside" rain either doesn't meet the minimum depth threshold for event detection
  #or comes at the end of the time series, when we can't yet verify that the rain event won't continue
  #into the next batch of data
  mars_rain_verify <- mars_rain %>% 
    group_by(radar_uid) %>%
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
  dbWriteTable(mars, "tbl_radar_event_escrow", rain_newevents, temporary = TRUE, overwrite = TRUE)
  
  #Reread and verify a lack of drift
  mars_events_verify <- dbGetQuery(mars, "select * from tbl_radar_event_escrow")
  
  #Compute hashes
  mars_events_verify <- mars_events_verify %>%
    rowwise %>%
    mutate(md5hash_verify = digest(paste(radar_uid, records, eventdatastart_local, eventdataend_local, eventduration_hr, eventpeakintensity_inhr, eventavgintensity_inhr, eventdepth_in), algo = "md5")) %>%
    mutate(hashes_match = md5hash_verify == md5hash)
  
  table(mars_events_verify$hashes_match) #All true!
  
  if(all(mars_events_verify$hashes_match)){
    dbExecute(mars, "insert into data.test_tbl_radar_event (radar_uid, eventdatastart_local, eventdataend_local, eventduration_hr, eventpeakintensity_inhr, eventavgintensity_inhr, eventdepth_in)
              select radar_uid, eventdatastart_local, eventdataend_local, eventduration_hr, eventpeakintensity_inhr, eventavgintensity_inhr, eventdepth_in from tbl_radar_event_escrow")
}
