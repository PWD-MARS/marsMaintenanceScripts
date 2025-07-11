# Section 1: Setting runtime parameters ----

  #Load from local libs
  .libPaths("./lib")
  readRenviron("./.Renviron")

  #Dplyr stuff
  library(magrittr)
  library(tidyverse)
  library(lubridate)
  library(data.table)
  
  #Database Stuff
  library(RODBC)
  library(pool)
  library(RPostgres)
  
  #Other stuff
  library(knitr)
  library(digest)
  options(stringsAsFactors=FALSE)
  
  errorCodes <- data.frame(code = 0:6,
                           message = c("Execution successful.",
                                       "Could not connect to Postgres DB. Is Postgres down?",
                                       NA, #Error from TryCatch will be used
                                       NA, #Error from TryCatch will be used
                                       NA,  #Error from TryCatch will be used
                                       NA,  #Error from TryCatch will be used
                                       NA
                           ), stringsAsFactors=FALSE)
  
  kill = FALSE
  success = FALSE
  errorCode = 0
  
  log_code <- digest(now()) #Unique ID for the log batches

  #Connect to MARS database using ODBC channel
  marsDBCon <- tryCatch({
    dbPool(
      drv = RPostgres::Postgres(),
      host = "PWDMARSDBS1",
      port = 5434,
      dbname = "mars_prod",
      user= Sys.getenv("admin_uid"),
      password = Sys.getenv("admin_pwd"),
      timezone = NULL)},
    error = function(e){e})
  
  #Error check - Did we connect?
  if(typeof(marsDBCon) == "list")
  {
    kill = TRUE
    errorCode = 1
  }

## Break Point 1: Failed DB connection ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    stop(message = errorCodes$message[errorCode+1])
  }

# Section 2: Gathering OW Data ----
  ###Log: Start
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 2,
                           exit_code = NA,
                           note = "Gathering OW Data")
  
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_ow"), logMessage, append = TRUE, row.names=FALSE)
  
  
  #Read the latest date from each observation well from the mars database
  ow_latestdates <- dbGetQuery(marsDBCon, "SELECT * FROM data.viw_owdata_latestdates")
  
  #Read the accessdb table from the mars database and attach it to the date data
  accessdb <- dbGetQuery(marsDBCon, "SELECT filepath, ow_uid, datatable, sumptable FROM admin.tbl_accessdb")
  accessdb_latestdates <- left_join(ow_latestdates, accessdb, by = "ow_uid") %>% 
    filter(!is.na(filepath), !is.na(datatable))
  
  #Read the data from each Access DB's data table, 
  newdata <- data.frame(dtime = NULL, ow_uid = NULL, level_ft = NULL) #data frame to incrementally append to
  
  for(i in 1:nrow(accessdb_latestdates)){
    #Debug statement. Uncomment if running interactively.
    #print(paste("Accessing", accessdb_latestdates$filepath[i]))
    
    tryCatch(
      expr = {
        accessdbCon <- odbcConnectAccess2007(accessdb_latestdates$filepath[i])
        accessdb_latestdates$dtime[i][is.na(accessdb_latestdates$dtime[i])] <- mdy('2/20/2001', tz = "America/New_York")
        accessdb_query <- paste0("select * from [", accessdb_latestdates$datatable[i], "] where [Standard Dtime] > #",accessdb_latestdates$dtime[i], "# ")
        print(accessdb_query)
        
        accessdb_newdata <- sqlQuery(accessdbCon, accessdb_query, as.is = TRUE) %>%
          select(dtime = 1, level_ft = ncol(.)) %>% #dtime is the first column, level is the last
          mutate(dtime = ymd_hms(dtime, tz = "America/New_York"), level_ft = as.numeric(level_ft)) %>% #Data comes in as plain text from RODBC
          filter(dtime > accessdb_latestdates$dtime[i]) %>% #We still need to filter by > the latest date because Access will treat values with fractional seconds as > values without fractional seconds. When R recieves them, though, we get them without the fractional seconds, so from our perspective, we have a value that is = the latest date. This is very silly.
          mutate(secondbump = (second(dtime) == 59)) %>% #Calculate whether the seconds place needs to be bumped +1 second
          mutate(dtime = as.POSIXct(ifelse(secondbump, dtime + dseconds(1), dtime), tz = "America/New_York")) %>%
          filter(dtime > accessdb_latestdates$dtime[i]) %>% #We still need to filter by > the latest date because Access will treat values with fractional seconds as > values without fractional seconds. When R recieves them, though, we get them without the fractional seconds, so from our perspective, we have a value that is = the latest date. This is very silly.
          arrange(dtime) %>% #Order by ascending datetime in case it's out of order in the DB
          mutate(ow_uid = accessdb_latestdates$ow_uid[i]) %>% #Attach OW UID to the data
          mutate(key = paste(ow_uid, dtime, sep = "_"), 
                 dupe = duplicated(key)) %>% #Sometimes there are duplicates in the Access DBs
          filter(dupe == FALSE) %>% #Remove the dupe rows
          select(-key, -dupe, -secondbump) #Remove the key columns
        
        newdata <- bind_rows(newdata, accessdb_newdata)
        odbcClose(accessdbCon)
      },
      error = function(e){
        kill <<- TRUE
        errorCode <<- 2
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
      }
    )
  }
  

## Break Point 2: OW Data Read Error ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    stop(message = errorCodes$message[errorCode+1])
  }
  

  ###Log: Start
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 3,
                           exit_code = NA,
                           note = "Processing OW Data")
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_ow"), logMessage, append = TRUE, row.names=FALSE)
  
  
  #Some nulls may exist so we purge them
  newdata %<>% filter(complete.cases(newdata))
  newdata$dtime <- newdata$dtime %>% 
    round_date(unit = "minute") %>%
    as.character  
  
  
  #Pull OW table for use in the report table
  ow <- dbGetQuery(marsDBCon, "select * from fieldwork.tbl_ow")
  
  ####Error check - Any new data?
  if(nrow(newdata) == 0)
  {
    # This is possible if all the new files are empty
    display_newdata <- data.frame(NULL)
  } else{
    display_newdata <- newdata %>% 
      group_by(ow_uid) %>% 
      summarize(data_points = n(), first_est = first(dtime), last_est = last(dtime)) %>%
      left_join(ow, by = "ow_uid") %>%
      select(smp_id, ow_suffix, ow_uid, first_est, last_est, data_points)
    #count unique wells with new data
  }

# Section 3: Writing New OW Data ----
  ###Log: Start
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 4,
                           exit_code = NA,
                           note = "Writing New OW Data")
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_ow"), logMessage, append = TRUE, row.names=FALSE)
  
  #Create a data frame of file-by-file writing results
  owdata_results <- newdata %>% 
    group_by(ow_uid) %>% 
    summarize(outcome = NA, data_points = n()) %>% 
    left_join(ow, by = "ow_uid") %>% 
    transmute(smp_id, ow_suffix, ow_uid, 
              data_points, 
              outcome)
  
  #Write each well's worth of data to the database one at a time
  for(i in 1:nrow(owdata_results)){
    if(nrow(owdata_results) == 0){break} #If there are no new data sources, don't do anything
    
    newdata_currentfile <- filter(newdata, ow_uid == owdata_results$ow_uid[i])
    
    if(nrow(newdata_currentfile) > 0){
      tryCatch({owdata_results$outcome[i] <- dbWriteTable(marsDBCon, 
                                                          RPostgres::Id(schema = "data", table = "tbl_ow_leveldata_raw"), 
                                                          newdata_currentfile, 
                                                          append= TRUE, 
                                                          row.names = FALSE)
      }, # append the data
      error = function(e){
        kill <<- TRUE
        errorCode <<- 3
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
        success <<- TRUE
      }
      )
    }
  }
  
  #Site successes
  success_wells <- sum(owdata_results$outcome)
  success_points <- filter(owdata_results, outcome == TRUE) %>%
    pull(data_points) %>%
    sum
  
  #Writing file counts
  if(success_wells > 0){ #If the write succeeded
    logMessage <- data.frame(date = as.Date(today()),
                             records = success_wells,
                             type = "OWs",
                             hash = log_code)
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_writes_ow"), logMessage, append = TRUE, row.names=FALSE)
    
    logMessage <- data.frame(date = as.Date(today()),
                             records = success_points,
                             type = "OW Records",
                             hash = log_code)
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_writes_ow"), logMessage, append = TRUE, row.names=FALSE)
  }
  
  #Refresh matview for QA Shiny app
  dbGetQuery(marsDBCon, "refresh materialized view data.mat_level_data_day;")

## Break Point 3: OW Data Write Error ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    stop(message = errorCodes$message[errorCode+1])
  }

# Section 4: Gathering GW Data ----
  ###Log: Start
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 5,
                           exit_code = NA,
                           note = "Gathering GW Data")
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_ow"), logMessage, append = TRUE, row.names=FALSE)
  
  
  #Read the latest date from each observation well from the mars database
  gw_latestdates <- dbGetQuery(marsDBCon, "SELECT * FROM data.viw_gwdata_latestdates")
  
  #Read the accessdb table from the mars database and attach it to the date data
  accessdb <- dbGetQuery(marsDBCon, "SELECT filepath, ow_uid, datatable, sumptable FROM admin.tbl_accessdb")
  accessdb_latestdates <- left_join(gw_latestdates, accessdb, by = "ow_uid") %>% filter(!is.na(filepath), !is.na(datatable))
  
  #Read the data from each Access DB's data table, 
  newdata <- data.frame(dtime = NULL, ow_uid = NULL, depth_ft = NULL) #data frame to incrementally append to
  
  for(i in 1:nrow(accessdb_latestdates)){
    #Debug statement. Uncomment if running interactively.
    print(paste("Accessing", basename(accessdb_latestdates$filepath[i])))
    
    #We need RODBC to connect to the DBs because odbc::odbc throws a "DSN too long" error. I would like to fix this sometime
    tryCatch(
      expr = {
        accessdbCon <- RODBC::odbcConnectAccess2007(accessdb_latestdates$filepath[i])
        accessdb_latestdates$dtime[i][is.na(accessdb_latestdates$dtime[i])] <- mdy('2/20/2001', tz = "America/New_York") #replace NAs with an early date
        accessdb_query <- paste0("select * from [", accessdb_latestdates$datatable[i], "] where [Standard Dtime] > #",accessdb_latestdates$dtime[i], "# ") #query new rows
        #print(accessdb_query)
        
        
        accessdb_newdata <- sqlQuery(accessdbCon, accessdb_query, as.is = TRUE) %>% 
          select(dtime = 1, depth_ft = ncol(.)) %>% #dtime is the first column, level is the last
          mutate(dtime = ymd_hms(dtime, tz = "EST"), depth_ft = as.numeric(depth_ft)) %>% #Data comes in as plain text from RODBC
          mutate(secondbump = (second(dtime) == 59)) %>% #Calculate whether the seconds place needs to be bumped +1 second
          mutate(dtime = as.POSIXct(ifelse(secondbump, dtime + dseconds(1), dtime), tz = "EST")) %>%
          mutate(dtime = with_tz(dtime, tz = "America/New_York")) %>% #Convert to EDT
          filter(dtime > accessdb_latestdates$dtime[i]) %>% #Only take the new data
          arrange(dtime) %>% #Order by ascending datetime in case it's out of order in the DB
          mutate(ow_uid = accessdb_latestdates$ow_uid[i]) %>% #Attach OW UID to the data
          mutate(key = paste(ow_uid, dtime, sep = "_"), 
                 dupe = duplicated(key)) %>% #Sometimes there are duplicates in the Access DBs
          filter(dupe == FALSE) %>% #Remove the dupe rows
          select(-key, -dupe, -secondbump) #Remove the key columns
        
        newdata <- bind_rows(newdata, accessdb_newdata)
        odbcClose(accessdbCon)
      },
      error = function(e){
        kill <<- TRUE
        errorCode <<- 4
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
      }
    )
  }
  
## Break Point 4: GW Data Read Error ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    stop(message = errorCodes$message[errorCode+1])
  }

  ###Log: Start
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 6,
                           exit_code = NA,
                           note = "Processing GW Data")
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_ow"), logMessage, append = TRUE, row.names=FALSE)
  
  #Some nulls may exist so we purge them
  newdata %<>% filter(complete.cases(newdata))
  newdata$dtime <- newdata$dtime %>% 
    round_date(unit = "minute") %>%
    as.character
  
  ####Error check - Any new data?
  if(nrow(newdata) == 0)
  {
    # This is possible if all the new files are empty
    display_newdata <- data.frame(NULL)
    
  } else{
    display_newdata <- newdata %>% 
      group_by(ow_uid) %>% 
      summarize(data_points = n(), first_est = first(dtime), last_est = last(dtime)) %>%
      left_join(ow, by = "ow_uid") %>%
      select(smp_id, ow_suffix, ow_uid, first_est, last_est, data_points)
    #count unique wells with new data
  }

# Section 5: Writing GW Data ----
  ###Log: Start
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 7,
                           exit_code = NA,
                           note = "Writing GW Data")
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_ow"), logMessage, append = TRUE, row.names=FALSE)
  
  
  #Create a data frame of file-by-file writing results
  if(nrow(newdata) > 0){
    gwdata_results <- newdata %>% 
      group_by(ow_uid) %>% 
      summarize(outcome = NA, data_points = n()) %>% 
      left_join(ow, by = "ow_uid") %>% 
      transmute(smp_id, ow_suffix, ow_uid, 
                data_points, 
                outcome)
  } else {
    gwdata_results <- data.frame(outcome = NULL)
  }
  
  
  #Write each well's worth of data to the database one at a time
  for(i in 1:nrow(gwdata_results)){
    if(nrow(gwdata_results) == 0){break} #If there are no new data sources, don't do anything
    
    newdata_currentfile <- filter(newdata, ow_uid == gwdata_results$ow_uid[i])
    
    if(nrow(newdata_currentfile) > 0){
      tryCatch({gwdata_results$outcome[i] <- dbWriteTable(marsDBCon, 
                                                          RPostgres::Id(schema = "data", table = "tbl_gw_depthdata_raw"), 
                                                          newdata_currentfile, 
                                                          append= TRUE, 
                                                          row.names = FALSE)
      }, # append the data
      error = function(e){
        keepRunning <<- FALSE
        errorCode <<- 6
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
        success <<- TRUE
      }
      )
    }
    
  }
  
  #Site successes
  success_gwells <- sum(gwdata_results$outcome)
  
  if(nrow(gwdata_results) > 0){
    success_gpoints <- filter(gwdata_results, outcome == TRUE) %>%
      pull(data_points) %>%
      sum
  }

  #Writing file counts
  if(success_gwells > 0){ #If the write succeeded
    logMessage <- data.frame(date = as.Date(today()),
                             records = success_gwells,
                             type = "GWs",
                             hash = log_code)
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_writes_ow"), logMessage, append = TRUE, row.names=FALSE)
    
    logMessage <- data.frame(date = as.Date(today()),
                             records = success_gpoints,
                             type = "GW Records",
                             hash = log_code)
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_writes_ow"), logMessage, append = TRUE, row.names=FALSE)
  }
  
  #Refresh matview for QA Shiny app
  dbGetQuery(marsDBCon, "refresh materialized view data.mat_gw_data_day;")

## Break Point 5: GW Data Write Error ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    stop(message = errorCodes$message[errorCode+1])
  }

# Section 6: Data Gaps ----
  ###Log: Start
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 8,
                           exit_code = NA,
                           note = "Checking for data gaps")
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_ow"), logMessage, append = TRUE, row.names=FALSE)
  
  data_gaps_current <- odbc::dbGetQuery(marsDBCon, "SELECT * FROM data.tbl_datagaps")
  
  #query the collection calendar and arrange by deployment_uid
  collect_query <- "select *, admin.fun_smp_to_system(smp_id) as system_id ,data.fun_date_to_fiscal_quarter(cast(date_100percent AS DATE)) as expected_fiscal_quarter, data.fun_date_to_fiscal_quarter(cast(collection_dtime AS DATE)) as collected_fiscal_quarter from fieldwork.viw_qaqc_deployments"
  deployments_df <- odbc::dbGetQuery(marsDBCon, collect_query) %>%
    select(smp_id, deployment_uid ,ow_uid, ow_suffix, deployment_dtime, collection_dtime, date_100percent, type, term) %>%
    filter(type == "LEVEL" & (term == "Short" | term == "Long")) %>%
    na.omit() %>%
    mutate(dif = as.numeric(collection_dtime - date_100percent)) %>%
    mutate(reference_date = fifelse(dif > 0, as.POSIXct(date_100percent), as.POSIXct(collection_dtime)))
  
  # Data day for level
  level_data_day <- odbc::dbGetQuery(marsDBCon, "SELECT * FROM data.mat_level_data_day") %>%
    mutate(level_data_exist = "Yes")
  # Data day for gw
  gw_data_day <- odbc::dbGetQuery(marsDBCon,"SELECT * FROM data.mat_gw_data_day") %>%
    mutate(gw_data_exist = "Yes")
  
  deployments_df <- deployments_df %>%
    select(deployment_uid ,ow_uid, ow_suffix, deployment_dtime, collection_dtime, reference_date, type, term) %>%
    filter(type == "LEVEL" & (term == "Short" | term == "Long")) %>%
    na.omit() %>%
    mutate(datagap_days = NA)
  
  # checking the data contunity for collected sensors. long/short level data sensors
  deployments_df <- deployments_df %>%
    select(deployment_uid ,ow_uid, ow_suffix, deployment_dtime, collection_dtime, reference_date, type, term) %>%
    filter(type == "LEVEL" & (term == "Short" | term == "Long")) %>%
    na.omit() %>%
    mutate(datagap_days = NA)
  
  # loop through deployments and assign a boolean datagap status using left-join tool. To do this, a squence of days are created from deployment to collection and is
  # left-joined by the data-day time-series to expose gaps
  for (i in 1:nrow(deployments_df)) {
    
    dates <- seq(from=as.Date(deployments_df$deployment_dtime[i]), to=as.Date(deployments_df$reference_date[i]), by = "days")
    dates <- as.data.frame(dates)
    
    if (deployments_df$ow_suffix[i] == "GW1" | deployments_df$ow_suffix[i] == "GW2" | deployments_df$ow_suffix[i] == "GW3" | deployments_df$ow_suffix[i] == "GW4" | deployments_df$ow_suffix[i] == "GW5" | deployments_df$ow_suffix[i] == "CW1"){
      
      data_df <- gw_data_day %>%
        filter(ow_uid == deployments_df$ow_uid[i]) 
      data_check <- dates %>%
        left_join(data_df, by = c("dates" = "gw_data_day")) 
      deployments_df[i,"datagap_days"] <- sum(is.na(data_check$gw_data_exist))
      
    } else {
      
      data_df <- level_data_day %>%
        filter(ow_uid == deployments_df$ow_uid[i]) 
      data_check <- dates %>%
        left_join(data_df, by = c("dates" = "level_data_day")) 
      # only flag of there is more than 2 days of data gaps (excluding deployment and collection dates)
      deployments_df[i,"datagap_days"] <- sum(is.na(data_check$level_data_exist))
      
    }
  }
  
  # prep and write to db, look for new deployments and those with updated gapdays
  datagaps <- deployments_df %>%
    select(deployment_uid, datagap_days) %>%
    anti_join(data_gaps_current, by = c("deployment_uid","datagap_days"))

# Section 7: Writing Data Gaps ----
  ###Log: Start
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 9,
                           exit_code = NA,
                           note = "Writing data gaps")
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_ow"), logMessage, append = TRUE, row.names=FALSE)
  
  if(nrow(datagaps) > 0){
    
    tryCatch(
      
      expr = {
        # delete values that have changed 
        sql_string <- paste("delete from data.tbl_datagaps WHERE deployment_uid in (", paste(datagaps$deployment_uid, collapse = ","), ");")
        dbExecute(marsDBCon, sql_string)
        
        # write the new data and data with updated values to DB
        dbWriteTable(marsDBCon, Id(schema = "data", table = "tbl_datagaps"), datagaps, append= TRUE, row.names = FALSE )
        success <<- TRUE
      },
      error = function(e) {
        kill <<- TRUE
        errorCode <<- 7
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
      }
    )
    
    if(!kill)
    {
      #Writing file counts
      logMessage <- data.frame(date = as.Date(today()),
                               records = nrow(datagaps),
                               type = "Data gaps",
                               hash = log_code)
      
      dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_writes_ow"), logMessage, append = TRUE, row.names=FALSE) 
    }
  }

## Break Point 6: Data Gap Write Error ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    stop(message = errorCodes$message[errorCode+1])
  }

## Script End
  if(kill == FALSE){
    print("# Script Results: Success\n")
    print(paste("## Exit Code:", errorCode, "\n"))
    print(paste("## Exit Message: ", errorCodes$message[errorCode+1]))
    
    ###Log: End
    logMessage <- data.frame(date = as.Date(today()), 
                             milestone = NA,
                             exit_code = errorCode,
                             hash = log_code,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_accessdb"), logMessage, append = TRUE, row.names=FALSE)
    
  }  
