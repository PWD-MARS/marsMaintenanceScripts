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
  dbname = "monica_seriestest",
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
            rainfall_in = round(Rainfall, 4))

hh_md5hash <- digest(hhrainfall_gage, algo= "md5") #calculating hash for the entire dataset

#Create temporary escrow table to host our prospective written data
dbWriteTable(mars, "tbl_gage_escrow", hhrainfall_gage, temporary = TRUE)

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
  
  #Compare hashes
  mars_md5hash = digest(gage_verify, algo = "md5")
  
  hash_equal <- mars_md5hash == hh_md5hash #True! Data frames are identical
                             #Every record was properly retrieved
  
  #If every check succeeds, append the escrow data into the main table
  if(all(rows_equal, sums_equal, hashes_equal)){
    dbExecute(mars, "insert into tbl_gage_rain (gage_uid, dtime_local, rainfall_in)
              select gage_uid, dtime_local, rainfall_in from tbl_gage_escrow")
  }

#S.3 Calculate Event Metadata
library(pwdgsi)
  
  #Pull rain information
  mars_rain <- dbGetQuery(mars, "select * from tbl_gage_rain")
  
  rain_newevents <- mars_rain %>% 
    group_by(gage_uid) %>%
    arrange(dtime_local) %>% 
    mutate(event_id = marsDetectEvents(dtime_local, rainfall_in)) %>%
    #Drop the last "complete" event in case it straddles the month boundary
    #It will get processed the when the next batch of data comes in
    filter(!is.na(event_id), event_id != max(event_id, na.rm = TRUE)) %>%
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

  #Create temporary escrow table to host our prospective written data
  dbWriteTable(mars, "tbl_event_escrow", rain_newevents, overwrite = TRUE, temporary = TRUE)

  mars_events <- dbGetQuery(mars, tbl_event_escro)
  