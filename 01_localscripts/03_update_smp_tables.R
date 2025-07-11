#Load from local libs
setwd("C:/marsMaintenanceScripts/01_localscripts")
.libPaths("./lib")
readRenviron("./.Renviron")

# Section 0: Preamble ----
#GIS stuff
library(sf)
library(s2)

#Dplyr stuff
library(magrittr)
library(tidyverse)

#Database Stuff
library(odbc)
library(pool)

#Other stuff
library(knitr)
library(digest)
options(stringsAsFactors=FALSE)

###Section 0.1: Check parameter validity

#ODBC String for GIS DB
dsn_infra_pub <- paste0("MSSQL:server=PWDGISSQL;",
                        "database=GIS_APPS;",
                        "UID=", Sys.getenv("gis_uid"), ";",
                        "PWD=", Sys.getenv("gis_pwd"), ";")

log_code <- digest(now()) #Unique ID for the log batches

###Section 0.2: Connect to the database.
marsDBCon <- dbPool(
  drv = RPostgres::Postgres(),
  host = "PWDMARSDBS1",
  port = 5434,
  dbname = "mars_prod",
  user= Sys.getenv("admin_uid"),
  password = Sys.getenv("admin_pwd"),
  timezone = NULL)

errorCodes <- data.frame(code = 0:4,
                         message = c("Execution successful.",
                                     "Could not connect to Postgres. Is the database down?",
                                     NA, #Write error from TryCatch will be used
                                     NA, #Write error from TryCatch will be used
                                     NA #Write error from TryCatch will be used
                         ), stringsAsFactors=FALSE)

kill = FALSE
success = FALSE
errorCode = 0

###Log: Start
logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                         milestone = 1,
                         exit_code = NA,
                         note = "Testing DB Connection")

dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)

####Error check - Did we connect?
if(typeof(marsDBCon) == "list")
{
  kill = TRUE
  errorCode = 1
}

#Refresh materialized view so it has the newest cache of SMP IDs
#If we don't do this, new IDs won't be found, and we will get an insertion error.
if(!kill){
  dbGetQuery(marsDBCon, "REFRESH MATERIALIZED VIEW external.mat_assets WITH DATA;")
}

## Break Point 1: Connection Error ----
if(kill == TRUE){
  print("# Script Results: Error\n")
  print(paste("## Error Code:", errorCode, "\n"))
  print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
  
  stop(message = errorCodes$message[errorCode+1])
}

###Log: Start
logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                         milestone = 2,
                         exit_code = NA,
                         note = "Gathering location data")

dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)

# Section 1: Update smp_loc table ----
  ###Check GIS data for any new SMPs, and insert them into smp_loc
  #Fetch current version of smp_loc
  smp_loc <- dbGetQuery(marsDBCon, "SELECT * FROM admin.tbl_smp_loc")
  smp_loc$lon_wgs84 %>% as.numeric %>% round(4) -> smp_loc$lon_wgs84
  smp_loc$lat_wgs84 %>% as.numeric %>% round(4) -> smp_loc$lat_wgs84
  
  #Grab centroids of new SMPs
  basin <- suppressWarnings((st_read(dsn_infra_pub, "gisad.GSWIBASIN", quiet = TRUE))) %>% 
    st_set_crs(2272) %>% st_transform(4326) %>% #Convert from PA State Plane to WGS 1984
    st_centroid %>% filter(!is.na(SMP_ID)) %>% transmute(smp_id = gsub("\\s", "", SMP_ID)) %>%
    filter(!(grepl("[A-z]", smp_id))) %>% #No A-Z characters permitted. Upper and lower case.
    filter(!(smp_id %in% smp_loc$smp_id)) %>%
    {data.frame(st_coordinates(.), .$smp_id)} %>%
    select(smp_id = 3, lon_wgs84 = 1, lat_wgs84 = 2)
  
  blueroof <- suppressWarnings(st_read(dsn_infra_pub, "gisad.GSWIBLUEROOF", quiet = TRUE)) %>% 
    st_set_crs(2272) %>% st_transform(4326) %>% #Convert from PA State Plane to WGS 1984
    st_centroid %>% filter(!is.na(SMP_ID)) %>% transmute(smp_id = gsub("\\s", "", SMP_ID)) %>%
    filter(!(grepl("[A-z]", smp_id))) %>% #No A-Z characters permitted. Upper and lower case.
    filter(!(smp_id %in% smp_loc$smp_id)) %>%
    {data.frame(st_coordinates(.), .$smp_id)} %>%
    select(smp_id = 3, lon_wgs84 = 1, lat_wgs84 = 2)
  
  bumpout <- suppressWarnings(st_read(dsn_infra_pub, "gisad.GSWIBUMPOUT", quiet = TRUE)) %>%
    st_set_crs(2272) %>% st_transform(4326) %>% #Convert from PA State Plane to WGS 1984
    st_centroid %>% filter(!is.na(SMP_ID)) %>% transmute(smp_id = gsub("\\s", "", SMP_ID)) %>%
    filter(!(grepl("[A-z]", smp_id))) %>% #No A-Z characters permitted. Upper and lower case.
    filter(!(smp_id %in% smp_loc$smp_id)) %>%
    {data.frame(st_coordinates(.), .$smp_id)} %>%
    select(smp_id = 3, lon_wgs84 = 1, lat_wgs84 = 2)
  
  cistern <- suppressWarnings(st_read(dsn_infra_pub, "gisad.GSWICISTERN", quiet = TRUE)) %>% 
    st_set_crs(2272) %>% st_transform(4326) %>% #Convert from PA State Plane to WGS 1984
    st_centroid %>% filter(!is.na(SMP_ID)) %>% transmute(smp_id = gsub("\\s", "", SMP_ID)) %>%
    filter(!(grepl("[A-z]", smp_id))) %>% #No A-Z characters permitted. Upper and lower case.
    filter(!(smp_id %in% smp_loc$smp_id)) %>%
    {data.frame(st_coordinates(.), .$smp_id)} %>%
    select(smp_id = 3, lon_wgs84 = 1, lat_wgs84 = 2)
  
  drainagewell <- suppressWarnings(st_read(dsn_infra_pub, "gisad.GSWIDRAINAGEWELL", quiet = TRUE)) %>% 
    st_set_crs(2272) %>% st_transform(4326) %>% #Convert from PA State Plane to WGS 1984
    st_centroid %>% filter(!is.na(SMP_ID)) %>% transmute(smp_id = gsub("\\s", "", SMP_ID)) %>%
    filter(!(grepl("[A-z]", smp_id))) %>% #No A-Z characters permitted. Upper and lower case.
    filter(!(smp_id %in% smp_loc$smp_id)) %>%
    {data.frame(st_coordinates(.), .$smp_id)} %>%
    select(smp_id = 3, lon_wgs84 = 1, lat_wgs84 = 2)
  
  greenroof <- suppressWarnings(st_read(dsn_infra_pub, "gisad.GSWIGREENROOF", quiet = TRUE)) %>% 
    st_set_crs(2272) %>% st_transform(4326) %>% #Convert from PA State Plane to WGS 1984
    st_make_valid %>% st_centroid %>% filter(!is.na(SMP_ID)) %>% transmute(smp_id = gsub("\\s", "", SMP_ID)) %>%
    filter(!(grepl("[A-z]", smp_id))) %>% #No A-Z characters permitted. Upper and lower case.
    filter(!(smp_id %in% smp_loc$smp_id)) %>%
    {data.frame(st_coordinates(.), .$smp_id)} %>%
    select(smp_id = 3, lon_wgs84 = 1, lat_wgs84 = 2)
  
  permeablepavement <- suppressWarnings(st_read(dsn_infra_pub, "gisad.GSWIPERMEABLEPAVEMENT", quiet = TRUE)) %>% 
    st_set_crs(2272) %>% st_transform(4326) %>% #Convert from PA State Plane to WGS 1984
    st_make_valid %>% st_centroid %>% filter(!is.na(SMP_ID)) %>% transmute(smp_id = gsub("\\s", "", SMP_ID)) %>%
    filter(!(grepl("[A-z]", smp_id))) %>% #No A-Z characters permitted. Upper and lower case.
    filter(!(smp_id %in% smp_loc$smp_id)) %>%
    {data.frame(st_coordinates(.), .$smp_id)} %>%
    select(smp_id = 3, lon_wgs84 = 1, lat_wgs84 = 2)
  
  planter <- suppressWarnings(st_read(dsn_infra_pub, "gisad.GSWIPLANTER", quiet = TRUE)) %>% 
    st_set_crs(2272) %>% st_transform(4326) %>% #Convert from PA State Plane to WGS 1984
    st_centroid %>% filter(!is.na(SMP_ID)) %>% transmute(smp_id = gsub("\\s", "", SMP_ID)) %>%
    filter(!(grepl("[A-z]", smp_id))) %>% #No A-Z characters permitted. Upper and lower case.
    filter(!(smp_id %in% smp_loc$smp_id)) %>%
    {data.frame(st_coordinates(.), .$smp_id)} %>%
    select(smp_id = 3, lon_wgs84 = 1, lat_wgs84 = 2)
  
  raingarden <- suppressWarnings(st_read(dsn_infra_pub, "gisad.GSWIRAINGARDEN", quiet = TRUE)) %>% 
    st_set_crs(2272) %>% st_transform(4326) %>% #Convert from PA State Plane to WGS 1984
    st_centroid %>% filter(!is.na(SMP_ID)) %>% transmute(smp_id = gsub("\\s", "", SMP_ID)) %>%
    filter(!(grepl("[A-z]", smp_id))) %>% #No A-Z characters permitted. Upper and lower case.
    filter(!(smp_id %in% smp_loc$smp_id)) %>%
    {data.frame(st_coordinates(.), .$smp_id)} %>%
    select(smp_id = 3, lon_wgs84 = 1, lat_wgs84 = 2)
  
  swale <- suppressWarnings(st_read(dsn_infra_pub, "gisad.GSWISWALE", quiet = TRUE)) %>% 
    st_set_crs(2272) %>% st_transform(4326) %>% #Convert from PA State Plane to WGS 1984
    st_centroid %>% filter(!is.na(SMP_ID)) %>% transmute(smp_id = gsub("\\s", "", SMP_ID)) %>%
    filter(!(grepl("[A-z]", smp_id))) %>% #No A-Z characters permitted. Upper and lower case.
    filter(!(smp_id %in% smp_loc$smp_id)) %>%
    {data.frame(st_coordinates(.), .$smp_id)} %>%
    select(smp_id = 3, lon_wgs84 = 1, lat_wgs84 = 2)
  
  treetrench <- suppressWarnings(st_read(dsn_infra_pub, "gisad.GSWITREETRENCH", quiet = TRUE)) %>% 
    st_set_crs(2272) %>% st_transform(4326) %>% #Convert from PA State Plane to WGS 1984
    st_centroid %>% filter(!is.na(SMP_ID)) %>% transmute(smp_id = gsub("\\s", "", SMP_ID)) %>%
    filter(!(grepl("[A-z]", smp_id))) %>% #No A-Z characters permitted. Upper and lower case.
    filter(!(smp_id %in% smp_loc$smp_id)) %>%
    {data.frame(st_coordinates(.), .$smp_id)} %>%
    select(smp_id = 3, lon_wgs84 = 1, lat_wgs84 = 2)
  
  trench <- suppressWarnings(st_read(dsn_infra_pub, "gisad.GSWITRENCH", quiet = TRUE)) %>% 
    st_set_crs(2272) %>% st_transform(4326) %>% #Convert from PA State Plane to WGS 1984
    st_centroid %>% filter(!is.na(SMP_ID)) %>% transmute(smp_id = gsub("\\s", "", SMP_ID)) %>%
    filter(!(grepl("[A-z]", smp_id))) %>% #No A-Z characters permitted. Upper and lower case.
    filter(!(smp_id %in% smp_loc$smp_id)) %>%
    {data.frame(st_coordinates(.), .$smp_id)} %>%
    select(smp_id = 3, lon_wgs84 = 1, lat_wgs84 = 2)
  
  wetland <- suppressWarnings(st_read(dsn_infra_pub, "gisad.GSWIWETLAND", quiet = TRUE)) %>% 
    st_set_crs(2272) %>% st_transform(4326) %>% #Convert from PA State Plane to WGS 1984
    st_centroid %>% filter(!is.na(SMP_ID)) %>% transmute(smp_id = gsub("\\s", "", SMP_ID)) %>%
    filter(!(grepl("[A-z]", smp_id))) %>% #No A-Z characters permitted. Upper and lower case.
    filter(!(smp_id %in% smp_loc$smp_id)) %>%
    {data.frame(st_coordinates(.), .$smp_id)} %>%
    select(smp_id = 3, lon_wgs84 = 1, lat_wgs84 = 2)
  
  #Stick them all together
  smp_locNewData <- bind_rows(basin, blueroof, bumpout, cistern, drainagewell, greenroof, permeablepavement, planter, raingarden, swale, treetrench, trench, wetland)
  row.names(smp_locNewData) <- NULL
  
  if(nrow(smp_locNewData) == 0){
    kill = TRUE
    success = TRUE
  }

## Break Point 2: Early Completion ----
  if(kill == TRUE){
    print("# Script Results: Success!\n")
    print(paste("## Exit Code:", errorCode, "\n"))
    print(paste("## Exit Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }
  
# Section 2: Write centroids ----
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 3,
                           exit_code = NA,
                           note = "Writing centroids")
  
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)
  
  tryCatch(
    
    expr = {
      dbWriteTable(marsDBCon, 
                   RPostgres::Id(schema = "admin", table = "tbl_smp_loc"), 
                   smp_locNewData, 
                   append= TRUE, 
                   row.names = FALSE)
      success <<- TRUE
    },
    error = function(e) {
      kill <<- TRUE
      errorCode <<- 2
      errorCodes$message[errorCode+1] <<- e$message #Error object is a list
    }
  )
  
  if(!kill){
    #Writing file counts
    logMessage <- data.frame(date = as.Date(today()),
                             records = nrow(smp_locNewData),
                             type = "Centroids",
                             hash = log_code)
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_writes_smp"), logMessage, append = TRUE, row.names=FALSE)
  }

## Break Point 3: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }
  
# Section 3: Update smp_gage table ----
  ###If there are any SMPs with locations that do not have rain gage associations, apply those now
  #Read table of SMP locations (in case it got updated in step 1)
  smp_loc <- dbGetQuery(marsDBCon, "SELECT * FROM admin.tbl_smp_loc") %>%
    dplyr::filter(is.nan(lon_wgs84) == FALSE & is.nan(lat_wgs84) == FALSE) %>%
    dplyr::filter(is.na(lon_wgs84) == FALSE & is.na(lat_wgs84) == FALSE)
  
  #Read table of SMP/rain gage associations
  smp_gage <- dbGetQuery(marsDBCon, "SELECT * FROM admin.tbl_smp_gage")
  
  #Every SMP with a location should also have a rain gage. Find the ones that don't
  smp_loc_nogage <- anti_join(smp_loc, smp_gage, by = "smp_id")
  
  #If any SMPs don't have a gage
  if(nrow(smp_loc_nogage) > 0){
    #Fetch the rain gage locations from the database
    gage_loc <- dbGetQuery(marsDBCon, "SELECT * FROM admin.tbl_gage_loc")
    
    #Create spatial data frames for SMPs and gages
    gage_loc_spatial <- st_as_sf(gage_loc, coords = c(3, 4))
    nogage_spatial <- st_as_sf(smp_loc_nogage, coords = c(3, 4))
    
    #Find nearest gage for new SMPs
    smp_loc_nogage <- data.frame(smp_id = nogage_spatial$smp_id, 
                                 gageindex = st_nearest_feature(nogage_spatial, gage_loc_spatial)) %>%
      mutate(gage_uid = gage_loc_spatial$gage_uid[gageindex]) %>%
      filter(!duplicated(smp_id)) %>%
      select(smp_id, gage_uid)
  }
  
  if(nrow(smp_loc_nogage) == 0){
    kill = TRUE
    success = TRUE
  }

## Break Point 3: Early Completion ----
  if(kill == TRUE){
    print("# Script Results: Success!\n")
    print(paste("## Exit Code:", errorCode, "\n"))
    print(paste("## Exit Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }

# Section 4: Write gages ----
  ###Log: Start
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 5,
                           exit_code = NA,
                           note = "Writing gages")
  
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)
  
  tryCatch(
    
    expr = {
      dbWriteTable(marsDBCon, 
                   RPostgres::Id(schema = "admin", table = "tbl_smp_gage"), 
                   smp_loc_nogage, 
                   append= TRUE, 
                   row.names = FALSE)
      success <<- TRUE
    },
    error = function(e) {
      kill <<- TRUE
      errorCode <<- 3
      errorCodes$message[errorCode+1] <<- e$message #Error object is a list
    }
  )
  
  if(!kill){
    #Writing file counts
    logMessage <- data.frame(date = as.Date(today()),
                             records = nrow(smp_loc_nogage),
                             type = "Gages",
                             hash = log_code)
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_writes_smp"), logMessage, append = TRUE, row.names=FALSE)
  }

  ## Break Point 4: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }

# Section 3: Update smp_gage table ----
  ###If there are any SMPs with locations that do not have rain gage associations, apply those now
  #Read table of SMP locations (in case it got updated in step 1)
  smp_loc <- dbGetQuery(marsDBCon, "SELECT * FROM admin.tbl_smp_loc") %>%
    dplyr::filter(is.nan(lon_wgs84) == FALSE & is.nan(lat_wgs84) == FALSE) %>%
    dplyr::filter(is.na(lon_wgs84) == FALSE & is.na(lat_wgs84) == FALSE)
  
  #Read table of SMP/rain gage associations
  smp_gage <- dbGetQuery(marsDBCon, "SELECT * FROM admin.tbl_smp_gage")
  
  #Every SMP with a location should also have a rain gage. Find the ones that don't
  smp_loc_nogage <- anti_join(smp_loc, smp_gage, by = "smp_id")
  
  #If any SMPs don't have a gage
  if(nrow(smp_loc_nogage) > 0){
    #Fetch the rain gage locations from the database
    gage_loc <- dbGetQuery(marsDBCon, "SELECT * FROM admin.tbl_gage_loc")
    
    #Create spatial data frames for SMPs and gages
    gage_loc_spatial <- st_as_sf(gage_loc, coords = c(3, 4))
    nogage_spatial <- st_as_sf(smp_loc_nogage, coords = c(3, 4))
    
    #Find nearest gage for new SMPs
    smp_loc_nogage <- data.frame(smp_id = nogage_spatial$smp_id, 
                                 gageindex = st_nearest_feature(nogage_spatial, gage_loc_spatial)) %>%
      mutate(gage_uid = gage_loc_spatial$gage_uid[gageindex]) %>%
      filter(!duplicated(smp_id)) %>%
      select(smp_id, gage_uid)
  }
  
  if(nrow(smp_loc_nogage) == 0){
    kill = TRUE
    success = TRUE
  }

## Break Point 3: Early Completion ----
  if(kill == TRUE){
    print("# Script Results: Success!\n")
    print(paste("## Exit Code:", errorCode, "\n"))
    print(paste("## Exit Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }

# Section 4: Write gages ----
  ###Log: Start
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 5,
                           exit_code = NA,
                           note = "Writing gages")
  
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)
  
  tryCatch(
    
    expr = {
      dbWriteTable(marsDBCon, 
                   RPostgres::Id(schema = "admin", table = "tbl_smp_gage"), 
                   smp_loc_nogage, 
                   append= TRUE, 
                   row.names = FALSE)
      success <<- TRUE
    },
    error = function(e) {
      kill <<- TRUE
      errorCode <<- 3
      errorCodes$message[errorCode+1] <<- e$message #Error object is a list
    }
  )
  
  if(!kill){
    #Writing file counts
    logMessage <- data.frame(date = as.Date(today()),
                             records = nrow(smp_loc_nogage),
                             type = "Gages",
                             hash = log_code)
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_writes_smp"), logMessage, append = TRUE, row.names=FALSE)
  }

  ## Break Point 5: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }

  # Section 5: Update smp_radar table ----
  ###If there are any SMPs with locations that do not have grid cell associations, apply those now
  #Read table of SMP locations (in case it got updated in step 1)
  smp_loc <- dbGetQuery(marsDBCon, "SELECT * FROM admin.tbl_smp_loc") %>%
    dplyr::filter(is.nan(lon_wgs84) == FALSE & is.nan(lat_wgs84) == FALSE) %>%
    dplyr::filter(is.na(lon_wgs84) == FALSE & is.na(lat_wgs84) == FALSE)
  
  #Read table of SMP/grid cell associations
  smp_radar <- dbGetQuery(marsDBCon, "SELECT * FROM admin.tbl_smp_radar")
  
  #Every SMP with a location should also have a grid cell. Find the ones that don't
  smp_loc_noradar <- anti_join(smp_loc, smp_radar, by = "smp_id")
  
  #If any SMPs don't have a radar
  if(nrow(smp_loc_noradar) > 0){
    #Fetch the grid cell locations from the database
    radar_loc <- dbGetQuery(marsDBCon, "SELECT * FROM admin.tbl_radar_loc")
    
    #Create spatial data frames for SMPs and Grid Cells
    radar_loc_spatial <- st_as_sf(radar_loc, coords = c(3, 4))
    noradar_spatial <- st_as_sf(smp_loc_noradar, coords = c(3, 4))
    
    #Find nearest radar for new SMPs
    smp_loc_noradar <- data.frame(smp_id = noradar_spatial$smp_id, 
                                  radarindex = st_nearest_feature(noradar_spatial, radar_loc_spatial)) %>%
      mutate(radar_uid = radar_loc_spatial$radar_uid[radarindex]) %>%
      filter(!duplicated(smp_id)) %>%
      select(smp_id, radar_uid)
  }
  
  if(nrow(smp_loc_noradar) == 0){
    kill = TRUE
    success = TRUE
  }
  
  ## Break Point 6: Early Completion ----
  if(kill == TRUE){
    print("# Script Results: Success!\n")
    print(paste("## Exit Code:", errorCode, "\n"))
    print(paste("## Exit Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }
  
  # Section 6: Write grid cells ----
  ###Log: Start
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 5,
                           exit_code = NA,
                           note = "Writing Grid Cells")
  
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)
  
  tryCatch(
    
    expr = {
      dbWriteTable(marsDBCon, 
                   RPostgres::Id(schema = "admin", table = "tbl_smp_radar"), 
                   smp_loc_noradar, 
                   append= TRUE, 
                   row.names = FALSE)
      success <<- TRUE
    },
    error = function(e) {
      kill <<- TRUE
      errorCode <<- 3
      errorCodes$message[errorCode+1] <<- e$message #Error object is a list
    }
  )
  
  if(!kill){
    #Writing file counts
    logMessage <- data.frame(date = as.Date(today()),
                             records = nrow(smp_loc_noradar),
                             type = "Grid Cells",
                             hash = log_code)
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_writes_smp"), logMessage, append = TRUE, row.names=FALSE)
  }
  
  ## Break Point 7: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }
  
  ###Log: End
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = NA,
                           exit_code = errorCode,
                           note = errorCodes$message[errorCode+1])
  
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_smp"), logMessage, append = TRUE, row.names=FALSE)