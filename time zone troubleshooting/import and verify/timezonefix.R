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
  dbname = "demo_deployment",
  user= Sys.getenv("admin_uid"),
  password = Sys.getenv("admin_pwd"),
  timezone = NULL)

### S.1 Retroactively springing forwards and falling back
  # Hartranft 1-1-1 OW1 as a test case
  
  library(RODBC) #Required for Access reads
  
  hart <-"//pwdoows/oows/Watershed Sciences/GSI Monitoring/02 GSI Monitoring Sites/Hartranft_1/1-1-1_OW1_Hartranft_GSI_Monitoring_Database_20130403_sw.mdb"
  accessdbCon <- RODBC::odbcConnectAccess2007(hart)
  
  accessdb_query <- paste0("select * from [1-1-1_OW1_CWL_Monitoring] where [Standard Dtime] > #2/20/2001# ")
  hartdata_raw <- sqlQuery(accessdbCon, accessdb_query, as.is = TRUE)
  odbcClose(accessdbCon)
  
  hartdata_forwards <- hartdata_raw %>%
    select(dtime_text = 1, level_ft = ncol(.)) %>% #dtime is the first column, level is the last
    transmute(dtime_text, dtime_raw = ymd_hms(dtime_text, tz = "EST"), level_ft = as.numeric(level_ft)) %>% #TS is in no-DST local time, TZ is EST
    mutate(dtime_raw = dtime_raw + seconds(1)) %>% #Some sensors launched and recorded on the :59 second, not the :00 second. Fixing this
    filter(dtime_raw >= ymd_hms("2013-03-10 00:00:00", tz = "EST")) %>%
    filter(dtime_raw <= ymd_hms("2013-03-10 04:00:00", tz = "EST")) %>%
    transmute(dtime_text, dtime_local = with_tz(dtime_raw, "America/New_York")) %>% #With_tz will spring the no-DST EST time forwards appropriately
    rowwise() %>% #Calculate hash row by row
    mutate(md5hash = digest(paste(dtime_text, dtime_local), algo = "md5")) #Hash to ensure data is written correctly
  
  hartdata_backwards <- hartdata_raw %>%
    select(dtime_text = 1, level_ft = ncol(.)) %>% #dtime is the first column, level is the last
    transmute(dtime_text, dtime_raw = ymd_hms(dtime_text, tz = "EST"), level_ft = as.numeric(level_ft)) %>% #TS is in no-DST local time, TZ is EST
    mutate(dtime_raw = dtime_raw + seconds(1)) %>% #Some sensors launched and recorded on the :59 second, not the :00 second. Fixing this
    filter(dtime_raw >= ymd_hms("2013-11-03 00:00:00", tz = "EST")) %>%
    filter(dtime_raw <= ymd_hms("2013-11-03 04:00:00", tz = "EST")) %>%
    transmute(dtime_text, dtime_local = with_tz(dtime_raw, "America/New_York")) %>% #With_tz will fall the no-DST EST time back appropriately
    rowwise() %>% #Calculate hash row by row
    mutate(md5hash = digest(paste(dtime_text, dtime_local), algo = "md5")) #Hash to ensure data is written correctly
  
  hartdata_corrected <- rbind(hartdata_forwards, hartdata_backwards)
  
  dbWriteTable(mars, "dst_corrections", hartdata_corrected, append = TRUE)
  
  #Check to see if the data wrote and read correctly by checking the hashes
  hartdata_mars <- dbGetQuery(mars, "select * from dst_corrections")
  
  #Is the time zone preserved?
  tz(hartdata_mars$dtime_local) == "America/New_York" #True!
  
  #Do the hashes compute the same?
  hartdata_mars <- rowwise(hartdata_mars) %>% 
    mutate(md5hash_check = digest(paste(dtime_text, dtime_local), algo = "md5")) %>%
    mutate(hashes_equal = md5hash == md5hash_check)
  
  table(hartdata_mars$hashes_equal) #All true! Time series wrote and read without drift
  
# S.2 Complete pulls of rainfall data
  #Ensure no rainfall data fails to read or write from H&H
  #Accomplish this by hashing the full dataset
  
  centraldb <- dbPool(
    drv = RPostgres::Postgres(),
    host = "192.168.131.120",
    port = 5432,
    dbname = "CentralDB",
    user= Sys.getenv("central_uid"),
    password = Sys.getenv("central_pwd"),
    timezone = NULL)

  #Read the rain gage data from the H&H database
  hhrainfall_gage <- dbGetQuery(centraldb, "select * from pwdrg.tblModelRain") %>% 
    transmute(gage_uid = GaugeNo, 
              dtime_local = DateTime, #Comes in as America/New York time zone
              rainfall_in = round(Rainfall, 4))
  
  hh_md5hash <- digest(hhrainfall_gage, algo= "md5") #calculating hash for the entire dataset

  dbWriteTable(mars, "rainfall_completeness", hhrainfall_gage, overwrite = TRUE) #append = FALSE  
  
  #Read the data back and hash it again
  mars_gage <- dbGetQuery(mars, 
      "select gage_uid, dtime_local, rainfall_in from rainfall_completeness") #Do not bring down the primary key.
                                                                              # since the H&H data doesn't have it it will pollute the hash
  
  #Count the rows
  nrow(mars_gage) == nrow(hhrainfall_gage) #True!
  
  #Sum the rainfall across every gage since 1990
  mars_sums <- group_by(mars_gage, gage_uid) %>%
    summarize(mars_total_in = sum(rainfall_in))
  
  hh_sums <- group_by(hhrainfall_gage, gage_uid) %>%
    summarize(hh_total_in = sum(rainfall_in))

  sum_check <- left_join(mars_sums, hh_sums) %>%
    mutate(sum_check = mars_total_in == hh_total_in)

  table(sum_check$sum_check) #All true!
  
  #Compare hashes
  mars_md5hash = digest(mars_gage, algo = "md5")
  
  mars_md5hash == hh_md5hash #True! Data frames are identical
                             #Every record was properly retrieved
  
  