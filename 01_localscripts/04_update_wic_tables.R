# Section 0: Preamble ----
  # Install required package
  library(DBI)
  library(odbc)
  library(tidyverse)
  library(sf)
  library(lubridate)
  library(knitr)
  library(digest)
  library(RPostgres)
  #Not in logical
  `%!in%` <- Negate(`%in%`)
  
  # replace special characters with friendlier characters
  special_char_replace <- function(note){
    
    note_fix <- note %>%
      str_replace_all(c("•" = "-", "ï‚§" = "-", "“" = '"', '”' = '"'))
    
    return(note_fix)
    
  }
  
  # colur in-text numbers
  colorize <- function(x, color) {
    if (knitr::is_latex_output()) {
      sprintf("\\textcolor{%s}{%s}", color, x)
    } else if (knitr::is_html_output()) {
      sprintf("<span style='color: %s;'>%s</span>", color,
              x)
    } else x
  }
  
  # DB connections
  mars_con <- dbConnect(RPostgres::Postgres(),
                        host = "PWDMARSDBS1.pwd.phila.local",
                        port = 5434,
                        dbname = "mars_prod",
                        user = Sys.getenv("admin_uid"),
                        password = Sys.getenv("admin_pwd"))
  
  cityworks_con <- dbConnect(odbc(),
                             Driver = "SQL Server",
                             Server = "PWDCWSQLP",
                             Database = "PWD_Cityworks",
                             uid = Sys.getenv("cw_uid"),
                             pwd= Sys.getenv("cw_pwd"))
  
  gisdata_con <- dbConnect(odbc(),
                           Driver = "SQL Server",
                           Server = "PWDGISSQL",
                           Database = "GISDATA",
                           uid = Sys.getenv("gis_uid"),
                           pwd= Sys.getenv("gis_pwd"))
  
  gisapps_con <- paste0("MSSQL:server=PWDGISSQL;",
                        "database=GIS_APPS;",
                        "UID=", Sys.getenv("gis_uid"), ";",
                        "PWD=", Sys.getenv("gis_pwd"), ";")
  
  errorCodes <- data.frame(code = 0:13,
                           message = c("Execution successful.",
                                       "Could not connect to Postgres DB. Is Postgres down?",
                                       NA, #Write error from TryCatch will be used
                                       NA, #Write error from TryCatch will be used
                                       NA, #Write error from TryCatch will be used
                                       NA, #Write error from TryCatch will be used
                                       NA, #Write error from TryCatch will be used
                                       NA, #Write error from TryCatch will be used
                                       NA, #Write error from TryCatch will be used
                                       NA, #Write error from TryCatch will be used
                                       NA, #Write error from TryCatch will be used
                                       NA, #Write error from TryCatch will be used
                                       NA, #Write error from TryCatch will be used
                                       NA #Write error from TryCatch will be used
                           ), stringsAsFactors=FALSE)
  
  kill = FALSE
  errorCode = 0
  
  log_code <- digest(now()) #Unique ID for the log batches
  
  ####Error check - Did we connect?
  if(!RPostgres::dbIsValid(mars_con))
  {
    kill = TRUE
    errorCode = 1
  } else{
    ###Log: Start
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = 1,
                             exit_code = NA,
                             note = "DB Connection Successful")
    
    dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
  }
  
  

## Break Point 0 - Bad Connection -----
  if(kill){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    stop(message = errorCodes$message[errorCode+1])
  }
  
# Section 1 - Prepping the Raw Cityworks Data ----
  ###Log: Gathering Cityworks Data
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 2,
                           exit_code = NA,
                           note = "Searching Cityworks for WIC records")
  
  dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
  
  # cityworks wic orders and comments tables
  cw_workorders <- dbGetQuery(cityworks_con, "SELECT wo.WORKORDERID, wo.INITIATEDATE AS date,     wo.LOCATION, wo.WOXCOORDINATE, wo.WOYCOORDINATE, woe.ENTITYUID AS FACILITYID 
   FROM Azteca.WORKORDER wo INNER JOIN Azteca.ACTIVITYLINK rwo ON wo.WORKORDERID = rwo.DESTACTIVITYSTRINGID and SOURCEACTIVITYTYPE = 'servicerequest' and DESTACTIVITYTYPE = 'workorder'
   LEFT JOIN Azteca.REQUEST r ON rwo.SOURCEACTIVITYID = r.REQUESTID 
   LEFT JOIN Azteca.WORKORDERENTITY woe ON wo.WORKORDERID = woe.WORKORDERID 
   WHERE ((wo.DESCRIPTION = 'A - PROPERTY INVESTIGATION' AND r.DESCRIPTION = 'WATER IN CELLAR') OR
   (wo.DESCRIPTION = 'A - LEAK INVESTIGATION' AND r.DESCRIPTION = 'WATER IN CELLAR'))
      ")
  cw_comments <- dbGetQuery(cityworks_con, "SELECT COMMENTID, WORKORDERID, COMMENTS from Azteca.WORKORDERCOMMENT")
  
  # Make names & data format consistent 
  names(cw_workorders) <- c("workorder_id","date","address","xcoordinate","ycoordinate","facility_id")
  names(cw_comments) <- c("comment_id","workorder_id","comment")
  
  # cast types    
  cw_workorders$date <- as.Date(cw_workorders$date)
  cw_workorders$workorder_id <- as.numeric(cw_workorders$workorder_id)
  cw_comments$workorder_id <- as.numeric(cw_comments$workorder_id)
  
  # de-duplicate workorders
  # get duplicated work orders
  duped_ids <- cw_workorders %>%
    mutate(dupe = duplicated(workorder_id)) %>%
    filter(dupe == TRUE) 
  
  # filter cw_workorders df to only duplicated workorder ids
  duped_workorders <- cw_workorders %>%
    filter(workorder_id %in% duped_ids$workorder_id)
  
  # remove the duplicated work orders from the main df and bind it after de-duplication
  nonduped_workorders <- cw_workorders %>%
    filter(workorder_id %!in% duped_ids$workorder_id)
  
  # for duped workorders, retain only 1 entry per address, prioritize the entry with non-zero facility id (xy coordinates are the same across dupes). Exclude empty addresses. 
  de_duped_waddress <- duped_workorders %>%
    filter(facility_id != "0" & address !="") %>%
    group_by(address) %>%
    summarise(facility_id = facility_id[1], workorder_id = workorder_id[1], xcoordinate = xcoordinate[1], ycoordinate = xcoordinate[1], date = date[1])
  
  # for empty address, prioritize non-zero facility id (pick the first row if more than 1 non-zero)
  de_duped_noaddress <- duped_workorders %>%
    filter(address == "") %>%
    group_by(facility_id) %>%
    summarise(workorder_id = workorder_id[1], address = address[1], xcoordinate = xcoordinate[1], ycoordinate = xcoordinate[1], date = date[1]) 
  
  # bind dfs
  cw_workorders <- bind_rows(nonduped_workorders, de_duped_waddress, de_duped_noaddress)
  
  # filter comments to WIC related ones
  cw_comments <- cw_comments %>%
    filter(workorder_id %in% cw_workorders$workorder_id)

# Section 2.1 - Processing New/Changed Work Orders and Comments ----
  ###Log: Identifying New/Changed WICs & Comments
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 3,
                           exit_code = NA,
                           note = "Processing Raw WIC Data")
  
  dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
  
  # MARS wic and comments
  mars_workorders <- dbGetQuery(mars_con, "SELECT * FROM fieldwork.tbl_wic_workorder")
  mars_comments <- dbGetQuery(mars_con, "SELECT * FROM fieldwork.tbl_wic_comment")
  
  # Remove \r and replace \n with <br> for reactable. skip any remaining backslash. Also replace special chars   
  cw_comments$comment <- gsub("[\r]", "", cw_comments$comment)
  cw_comments$comment <- gsub("\\n", "<br>", cw_comments$comment)
  cw_comments$comment <- gsub('\'', '\'\'',  cw_comments$comment)
  cw_comments$comment <- special_char_replace(cw_comments$comment)
  
  # New WICs
  new_wic <- cw_workorders %>%
    filter(workorder_id %!in% mars_workorders$workorder_id)
  
  # Changed WICs
  changed_wic <- cw_workorders %>%
    filter(workorder_id %!in% new_wic$workorder_id) %>%
    anti_join(mars_workorders, by = c("workorder_id", "date","address","xcoordinate","ycoordinate","facility_id"))
  
  # New Comments
  new_comment <- cw_comments %>%
    filter(comment_id %!in% mars_comments$comment_id)
  
  # Changed Comments
  changed_comment <- cw_comments %>%
    filter(comment_id %!in% new_comment$comment_id) %>%
    anti_join(mars_comments, by = c("workorder_id", "comment", "comment_id"))


# Section 2.2 - Writing new WIC data ----
  ###Log: Writing new WIC data
  if(nrow(new_wic) > 0 & !kill){
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = 4,
                             exit_code = NA,
                             note = "Writing new WICs to MARS")
    
    dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    tryCatch(
      
      expr = {
        success_wo <- dbWriteTable(mars_con, 
                                   Id(schema = "fieldwork", table = "tbl_wic_workorder"), 
                                   new_wic, 
                                   append= TRUE, 
                                   row.names = FALSE)
      },
      error = function(e) {
        kill <<- TRUE
        errorCode <<- 2
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
      }
    )
    
    #Writing file counts
    if(!kill){ #If the write succeeded
      logMessage <- data.frame(date = as.Date(today()),
                               records = nrow(new_wic),
                               type = "New WICs",
                               hash = log_code)
      
      dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_writes_wic"), logMessage, append = TRUE, row.names=FALSE)
    }
  }

  ## Break Point 2.2: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }
  
# Section 2.3 - Update Changed Work Orders in MARS DB ----
  ###Log: Update Changed Work Orders in MARS DB
  if(nrow(changed_wic) > 0 & !kill){
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = 5,
                             exit_code = NA,
                             note = "Writing updated WICs to MARS")
    
    dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    successful_updates <- 0
    
    # update changed work orders
    for (i in 1:nrow(changed_wic)) {
      
      wo_sql_string <- sprintf(
        "UPDATE fieldwork.tbl_wic_workorder SET date = '%s', address = '%s',  xcoordinate = %s, ycoordinate= %s,facility_id = '%s' WHERE workorder_id = %s;",
        changed_wic$date[i],
        changed_wic$address[i],
        changed_wic$xcoordinate[i],
        changed_wic$ycoordinate[i],
        changed_wic$facility_id[i],
        changed_wic$workorder_id[i]
      )
      
      # Execute the update statement
      success_upwo <- dbSendStatement(mars_con, wo_sql_string)
      
      tryCatch(
        
        expr = {
          success_upwo <- dbSendStatement(mars_con, wo_sql_string)
          successful_updates <- successful_updates + 1
        },
        error = function(e) {
          kill <<- TRUE
          errorCode <<- 3
          errorCodes$message[errorCode+1] <<- e$message #Error object is a list
        })
    }
    
    #Writing file counts
    if(successful_updates > 0){ #If the write succeeded
      logMessage <- data.frame(date = as.Date(today()),
                               records = successful_updates,
                               type = "Updated WICs",
                               hash = log_code)
      
      dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_writes_wic"), logMessage, append = TRUE, row.names=FALSE)
    }
  }

## Break Point 2.3: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }
  
# Section 2.4 - Write New Comments to MARS DB ----
  if(nrow(new_comment) > 0 & !kill){
    ###Log: Write New Comments to MARS DB
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = 6,
                             exit_code = NA,
                             note = "Writing new comments to MARS")
    
    dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    tryCatch(
      
      expr = {
        success_wocomment <- dbWriteTable(mars_con, 
                                          Id(schema = "fieldwork", table = "tbl_wic_comment"), 
                                          new_comment, 
                                          append= TRUE, 
                                          row.names = FALSE)
      },
      error = function(e) {
        kill <<- TRUE
        errorCode <<- 4
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
      }
    )
    
    #Writing file counts
    if(!kill){ #If the write succeeded
      logMessage <- data.frame(date = as.Date(today()),
                               records = nrow(new_comment),
                               type = "New Comments",
                               hash = log_code)
      
      dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_writes_wic"), logMessage, append = TRUE, row.names=FALSE)
    }
  }
  ## Break Point 2.4: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }
# Section 2.5 - Updating changed comments to MARS DB ----
  if(nrow(changed_comment) > 0 & !kill){
    ###Log: Updating changed comments to MARS DB 
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = 7,
                             exit_code = NA,
                             note = "Writing updated comments to MARS")
    
    dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    successful_upcomms <- 0
    
    # update changed comments
    for (i in 1:nrow(changed_comment)) {
      if(nrow(changed_comment) == 0) {break} #SKip this in case we are running chunks that wouldn't usually execute in debug mode
      
      upcomm_sql_string <- sprintf(
        "UPDATE fieldwork.tbl_wic_comment set comment = '%s' WHERE workorder_id = %s AND comment_id = %s;",
        changed_comment$comment[i],
        changed_comment$workorder_id[i],
        changed_comment$comment_id[i]
      )
      
      tryCatch(
        
        expr = {
          success_upcomments <- dbSendStatement(mars_con, upcomm_sql_string)
          successful_upcomms <- successful_upcomms + 1
        },
        error = function(e) {
          kill <<- TRUE
          errorCode <<- 5
          errorCodes$message[errorCode+1] <<- e$message #Error object is a list
        })
    }
    
    #Writing file counts
    if(successful_upcomms > 0){ #If the write succeeded
      logMessage <- data.frame(date = as.Date(today()),
                               records = successful_upcomms,
                               type = "Updated Comments",
                               hash = log_code)
      
      dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_writes_wic"), logMessage, append = TRUE, row.names=FALSE)
    }
  }
  ## Break Point 2.5: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }
# Section 3 - Attach WICs to Parcels ----
  ###Log: Reading parcel data
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 8,
                           exit_code = NA,
                           note = "Reading parcel data")
  
  dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
  
  # pull pwd parcels (a large shapefile on the server which st_read fails to scrape from gis db)
  pwd_parcels_sf <- st_read(dsn = "\\\\pwdoows\\oows\\Watershed Sciences\\GSI Monitoring\\09 GIS Data\\PWD_PARCELS ", layer = "PWD_PARCELS")%>%
    select(gis_address = ADDRESS, gis_facility_id = FACILITYID)
  # non-spatial version
  pwd_parcels_df <- pwd_parcels_sf %>%
    st_set_geometry (NULL)
  
  if(nrow(cw_workorders) > 0){
    # Join by address (primary attribute for matching)
    wicparcel_join_address <- cw_workorders %>%
      inner_join(pwd_parcels_df, by = c("address" = "gis_address")) %>%
      select(workorder_id, address, facility_id = gis_facility_id)
    
    # Join by address (secondary attribute for matching)
    wicparcel_join_facilityid <- cw_workorders %>%
      filter(workorder_id %!in% wicparcel_join_address$workorder_id) %>%
      inner_join(pwd_parcels_df, by = c("facility_id" = "gis_facility_id")) %>%
      select(workorder_id, address = gis_address, facility_id)
    
    # get remaining unverified work orders
    xy_wics <- cw_workorders %>%
      filter(workorder_id %!in% wicparcel_join_address$workorder_id & workorder_id %!in% wicparcel_join_facilityid$workorder_id) %>%
      filter(!is.na(xcoordinate) & !is.na(ycoordinate))
    
    # Build the spatial object from the XY coordinates 
    xy_vectors <-  c(xy_wics[,"xcoordinate"], xy_wics[,"ycoordinate"])
    
    xy_matrix <- matrix(data = xy_vectors, ncol = 2) %>% 
      na.omit
    
    xy_wic_sf <- xy_matrix %>%  
      as.matrix() %>%  
      st_multipoint() %>%  
      st_sfc() %>%  
      st_cast('POINT')
    
    st_crs(xy_wic_sf) <- 2272
    
    # Intersect the Parcel polygons and the point
    xy_wic_intersect <- st_intersects(xy_wic_sf, pwd_parcels_sf)
    
    # extract the pwd_parcels_df & xy_wics row numbers from xy_wic_intersect 
    wo_index <- NULL
    parcel_index <- NULL
    
    for(i in 1:length(xy_wic_intersect)) {
      parcel_index_temp <- xy_wic_intersect[[i]]
      if (length(parcel_index_temp) > 0) {
        wo_index_temp <- rep(i, length(parcel_index_temp))
        wo_index <- c(wo_index, wo_index_temp)
        parcel_index <- c(parcel_index, parcel_index_temp) 
      }
    }
    # Use work order id index (wo_index vector) and pwd_parcels_df index (parcel_index vector) to grab the right rows and bind the resulting columns 
    wicparcel_xy <- bind_cols(xy_wics[wo_index, ], pwd_parcels_df[parcel_index, ]) %>%
      select(workorder_id, address = gis_address, facility_id = gis_facility_id)
  }
  
  #If any parcels have been identified, check for new ones
  if(nrow(wicparcel_xy) > 0 | nrow(wicparcel_join_facilityid) > 0 | nrow(wicparcel_join_address) > 0){
    # Bind the WIC parcels
    wic_parcels <- bind_rows(wicparcel_join_address, wicparcel_join_facilityid, wicparcel_xy)
    
    # pull wic parcels from MARS DB
    mars_wic_parcels <- dbGetQuery(mars_con, "SELECT * FROM fieldwork.tbl_wic_parcel")
    
    # new wic parcel
    new_wic_parcel <- wic_parcels %>%
      filter(workorder_id %!in% mars_wic_parcels$workorder_id)
    
    # changed wic parcel
    changed_wic_parcel <- wic_parcels %>%
      filter(workorder_id %!in% new_wic_parcel$workorder_id) %>%
      anti_join(mars_wic_parcels, by = c("workorder_id", "address", "facility_id"))
  }
  
# Section 3.1 - Write WIC parcel data to MARS ----
  if(nrow(new_wic_parcel) > 0 & !kill){
    ###Log: Writing new WIC Parcels to MARS
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = 8,
                             exit_code = NA,
                             note = "Writing new WIC Parcels to MARS")
    
    dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    tryCatch(
      
      expr = {
        success_wicparcel <- dbWriteTable(mars_con, 
                                          Id(schema = "fieldwork", table = "tbl_wic_parcel"), 
                                          new_wic_parcel, 
                                          append= TRUE, 
                                          row.names = FALSE)
      },
      error = function(e) {
        kill <<- TRUE
        errorCode <<- 6
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
      }
    )
    
    #Writing file counts
    if(!kill){ #If the write succeeded
      logMessage <- data.frame(date = as.Date(today()),
                               records = nrow(new_wic_parcel),
                               type = "New WIC Parcels",
                               hash = log_code)
      
      dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_writes_wic"), logMessage, append = TRUE, row.names=FALSE)
    }
  }
  ## Break Point 3.1: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }
# Section 3.2 - Write Updated WIC parcel data to MARS ----
  if(nrow(changed_wic_parcel) > 0 & !kill){
    ###Log: Writing updated WIC Parcels to MARS
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = 9,
                             exit_code = NA,
                             note = "Writing updated WIC Parcels to MARS")
    
    dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    
    # update changed wic parcels
    for (i in 1:nrow(changed_wic_parcel)) {
      
      upwicparcel_sql_string <- sprintf(
        "UPDATE fieldwork.tbl_wic_parcel set address = '%s', facility_id = '%s' WHERE workorder_id = %s;",
        changed_wic_parcel$address[i],
        changed_wic_parcel$facility_id[i],
        changed_wic_parcel$workorder_id[i]
      )
      
      tryCatch(
        
        expr = {
          # Execute the update statement
          success_upwicparcel <- dbSendStatement(mars_con, upwicparcel_sql_string)
          successful_upparcels <- successful_upparcels + 1
        },
        error = function(e) {
          kill <<- TRUE
          errorCode <<- 7
          errorCodes$message[errorCode+1] <<- e$message #Error object is a list
        })
    }
    
    #Writing file counts
    if(successful_upparcels > 0){ #If the write succeeded
      logMessage <- data.frame(date = as.Date(today()),
                               records = successful_upparcels,
                               type = "Updated WIC Parcels",
                               hash = log_code)
      
      dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_writes_wic"), logMessage, append = TRUE, row.names=FALSE)
    }
  }
  ## Break Point 3.2: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }
# Section 4 - Identifying new WIC polygons ----
  ###Log: Identifying new WIC polygons
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 10,
                           exit_code = NA,
                           note = "Identifying new WIC polygons")
  
  # pull footprint parcels    
  footprint_sf <- st_read(dsn = "\\\\pwdoows\\oows\\Watershed Sciences\\GSI Monitoring\\09 GIS Data\\Building_Footprint_Shapefile", layer = "Building_Footprint") 
  footprint_sf <- footprint_sf %>%
    select(address = ADDRESS)
  
  # pull MARS's spatial polygons
  mars_footprint_sf  <- st_read(dsn = mars_con, Id(schema="fieldwork", table = "tbl_wic_footprint_geom"))
  mars_propertyline_sf  <- st_read(dsn = mars_con, Id(schema="fieldwork", table = "tbl_wic_propertyline_geom"))
  mars_smp_sf  <- st_read(dsn = mars_con, Id(schema="fieldwork", table = "tbl_wic_smp_geom"))
  
  # find wic property lines
  wic_propertyline_sf <- pwd_parcels_sf %>%
    filter(gis_address %in% wic_parcels$address) %>%
    select(address = gis_address, propertyline_geom = geometry)
  
  # Combine geometries per address
  wic_propertyline_combined_sf <- wic_propertyline_sf %>%
    group_by(address) %>%
    summarize(propertyline_geom = st_combine(propertyline_geom)) %>%
    st_as_sf() 
  
  new_wic_propertyline_sf <- wic_propertyline_combined_sf %>%
    filter(address %!in% mars_propertyline_sf$address)
  
  # find wic footprints
  wic_footprint_sf <- footprint_sf %>%
    filter(address %in% wic_parcels$address) %>%
    select(address, footprint_geom = geometry)
  
  # Combine geometries per address
  wic_footprint_combined_sf <- wic_footprint_sf %>%
    group_by(address) %>%
    summarize(footprint_geom = st_combine(footprint_geom)) %>%
    st_as_sf() 
  
  new_wic_footprint_sf <- wic_footprint_combined_sf %>%
    filter(address %!in% mars_footprint_sf$address) 
  
# Section 4.1 - Writing WIC Property Lines ----
  if(nrow(new_wic_propertyline_sf) > 0 & !kill){
    ###Log: Writing WIC Property Lines to MARS
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = 11,
                             exit_code = NA,
                             note = "Writing WIC Property Lines to MARS")
    tryCatch(
      
      expr = {
        success_woprop <- st_write(obj = new_wic_propertyline_sf,
                                   dsn = mars_con,
                                   Id(schema = "fieldwork", table = "tbl_wic_propertyline_geom"), 
                                   append= TRUE)
      },
      error = function(e) {
        kill <<- TRUE
        errorCode <<- 8
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
      }
    )
    
    #Writing file counts
    if(!kill){ #If the write succeeded
      logMessage <- data.frame(date = as.Date(today()),
                               records = nrow(new_wic_propertyline_sf),
                               type = "New WIC Property Lines",
                               hash = log_code)
      
      dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_writes_wic"), logMessage, append = TRUE, row.names=FALSE)
    }
  }

  ## Break Point 4.1: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }

#Section 4.2 - Writing WIC footprints ----
  if(nrow(new_wic_footprint_sf) > 0 & !kill){
    ###Log: Writing WIC Footprints to MARS
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = 12,
                             exit_code = NA,
                             note = "Writing WIC Footprints to MARS")
    tryCatch(
      
      expr = {
        success_wo <- st_write(obj = new_wic_footprint_sf,
                               dsn = mars_con,
                               Id(schema = "fieldwork", table = "tbl_wic_footprint_geom"), 
                               append= TRUE)
      },
      error = function(e) {
        kill <<- TRUE
        errorCode <<- 9
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
      }
    )
    
    #Writing file counts
    if(!kill){ #If the write succeeded
      logMessage <- data.frame(date = as.Date(today()),
                               records = nrow(new_wic_footprint_sf),
                               type = "New WIC Footprints",
                               hash = log_code)
      
      dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_writes_wic"), logMessage, append = TRUE, row.names=FALSE)
    }
  }
  ## Break Point 4.2: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }

# Section 4.3 - Identify new SMP Polygons ----
  ###Log: Identifying new SMP polygons
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 13,
                           exit_code = NA,
                           note = "Identifying new SMP polygons")
  
  # pull mars polygons
  mars_smp_sf <- dbGetQuery(mars_con, "SELECT * FROM fieldwork.tbl_wic_smp_geom")
  
  # SMP Polygons from GIS. all CRS set at 2272 coordinate system to be consistent with pwd_parcels and footprint CRS
  basin <- suppressWarnings(st_read(gisapps_con, "gisad.GSWIBASIN", quiet = TRUE)) %>%
    st_set_crs(2272) %>%
    select(smp_id = SMP_ID)
  blueroof <- suppressWarnings(st_read(gisapps_con, "gisad.GSWIBLUEROOF", quiet = TRUE)) %>%
    st_set_crs(2272) %>%
    select(smp_id = SMP_ID)
  bumpout <- suppressWarnings(st_read(gisapps_con, "gisad.GSWIBUMPOUT", quiet = TRUE)) %>%
    st_set_crs(2272) %>%
    select(smp_id = SMP_ID)
  cistern <- suppressWarnings(st_read(gisapps_con, "gisad.GSWICISTERN", quiet = TRUE)) %>%
    st_set_crs(2272) %>%
    select(smp_id = SMP_ID)
  greenroof <- suppressWarnings(st_read(gisapps_con, "gisad.GSWIGREENROOF", quiet = TRUE)) %>%
    st_set_crs(2272) %>%
    select(smp_id = SMP_ID)
  permeablepavement <- suppressWarnings(st_read(gisapps_con, "gisad.GSWIPERMEABLEPAVEMENT", quiet = TRUE)) %>%
    st_set_crs(2272) %>%
    select(smp_id = SMP_ID)
  planter <- suppressWarnings(st_read(gisapps_con, "gisad.GSWIPLANTER", quiet = TRUE)) %>%
    st_set_crs(2272) %>%
    select(smp_id = SMP_ID)
  raingarden <- suppressWarnings(st_read(gisapps_con, "gisad.GSWIRAINGARDEN", quiet = TRUE)) %>%
    st_set_crs(2272) %>%
    select(smp_id = SMP_ID)
  swale <- suppressWarnings(st_read(gisapps_con, "gisad.GSWISWALE", quiet = TRUE)) %>%
    st_set_crs(2272) %>%
    select(smp_id = SMP_ID)
  treetrench <- suppressWarnings(st_read(gisapps_con, "gisad.GSWITREETRENCH", quiet = TRUE)) %>%
    st_set_crs(2272) %>%
    select(smp_id = SMP_ID)
  trench <- suppressWarnings(st_read(gisapps_con, "gisad.GSWITRENCH", quiet = TRUE)) %>%
    st_set_crs(2272) %>%
    select(smp_id = SMP_ID)
  wetland <- suppressWarnings(st_read(gisapps_con, "gisad.GSWIWETLAND", quiet = TRUE)) %>%
    st_set_crs(2272) %>%
    select(smp_id = SMP_ID)
  
  # single sf with no NA
  smp_sf <- bind_rows(basin, blueroof, bumpout, cistern, greenroof, permeablepavement, planter, raingarden, swale, treetrench, trench, wetland) %>%
    distinct() %>%
    na.omit() %>%
    group_by(smp_id) %>%
    summarize(SHAPE = st_combine(SHAPE)) %>%
    st_as_sf()
  
  # filter SMPs to public ones
  smp_sf["public"] <- grepl ("\\d+-\\d+-\\d+", smp_sf[["smp_id"]])
  smp_sf <- smp_sf %>%
    filter(public == TRUE) %>%
    select(-public)
  
  # system polygons
  system_sf <- smp_sf
  system_sf['system_id'] <- gsub('-\\d+$','', system_sf$smp_id)
  system_sf <- system_sf %>%
    group_by(system_id) %>%
    summarize(SHAPE = st_combine(SHAPE)) %>%
    st_as_sf()
  
  # New Polygons
  new_smp_sf <- smp_sf %>%
    filter(smp_id %!in% mars_smp_sf$smp_id) %>%
    select(smp_id, smp_geom = SHAPE)
  
# Section 4.4 - New SMP Polygons WKTs in MARS DB ----
  if(nrow(new_smp_sf) > 0 & !kill){
    ###Log: Writing SMP Polygons to MARS
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = 14,
                             exit_code = NA,
                             note = "Writing SMP Polygons to MARS")
    tryCatch(
      
      expr = {
        success_wo <- st_write(obj = new_smp_sf,
                               dsn = mars_con,
                               Id(schema = "fieldwork", table = "tbl_wic_smp_geom"), 
                               append= TRUE)
      },
      error = function(e) {
        kill <<- TRUE
        errorCode <<- 10
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
      }
    )
    
    #Writing file counts
    if(!kill){ #If the write succeeded
      logMessage <- data.frame(date = as.Date(today()),
                               records = nrow(new_smp_sf),
                               type = "New SMP Polygons",
                               hash = log_code)
      
      dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_writes_wic"), logMessage, append = TRUE, row.names=FALSE)
    }
  }
  ## Break Point 4.4: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }

# Section 5.1 - Intersect WIC Parcels with System Polygons and Calculate Distance ----
  ###Log: Intersecting WIC Parcels with System Polygons
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = 15,
                           exit_code = NA,
                           note = "Intersecting WIC Parcels with System Polygons")
  
  # Create buffer 100 ft buffer around SMPs and intersect them with the WIC Parcels
  system_sf_100ft <- st_buffer(system_sf, 100)
  system_wic_intersect <- st_intersects(system_sf_100ft, wic_propertyline_combined_sf)
  
  intersecting_addresses <- wic_propertyline_combined_sf %>%
    st_set_geometry(NULL)
  intersecting_systems <- system_sf_100ft %>%
    st_set_geometry(NULL)
  
  # placeholder
  system_wic_df <- data.frame(system_id = as.character(),
                              wic_address = as.character(),
                              property_dist_ft = as.numeric(),
                              footprint_dist_ft = as.numeric())
  
  # Develop a dataframe that associate smp_ids with wic property addresses- calculate smp-wic distances too 
  for (i in 1:length(system_wic_intersect)) {
    if (length(system_wic_intersect[[i]]) > 0) {
      for (j in 1:length(system_wic_intersect[[i]])) {
        wic_sf_index <- as.vector(system_wic_intersect[[i]])
        # property distance
        temp_sys_wic_prop_dist <- st_distance(system_sf[i, ], wic_propertyline_combined_sf[wic_sf_index[j], ])
        # footprint distance
        temp_sys_wic_footprint_dist <- st_distance(system_sf[i, ], filter(wic_footprint_combined_sf, address == wic_propertyline_combined_sf[wic_sf_index[j], ]$address))
        # put the data in a dataframe- using minimum function for footprint in case there are several footprint polygons per address
        temp_system_wic_df <- data.frame(system_id = intersecting_systems[i, ], 
                                         wic_address = pull(intersecting_addresses[system_wic_intersect[[i]][j],]),
                                         property_dist_ft = ifelse(length(temp_sys_wic_prop_dist) > 0, as.numeric(temp_sys_wic_prop_dist), NA),
                                         footprint_dist_ft = ifelse(length(temp_sys_wic_footprint_dist) > 0, min(as.numeric(temp_sys_wic_footprint_dist)), NA)
        )
        # bind rows
        system_wic_df <- bind_rows(system_wic_df, temp_system_wic_df)
      }
    }
  }

# Section 5.2 - Identify the WIC Construction Status ----
  # Getting the construction milestones dates
  cipit <- dbGetQuery(mars_con, "SELECT * FROM external.tbl_cipit_project")
  smpbdv <- dbGetQuery(mars_con, "SELECT * FROM  external.tbl_smpbdv")
  smp_milestones <- smpbdv %>%
    inner_join(cipit, by = "worknumber") %>%
    inner_join(system_wic_df, by = "system_id") %>%
    inner_join(wic_parcels, by = c("wic_address" = "address")) %>%
    inner_join(cw_workorders, by = "workorder_id") %>%
    select (system_id, workorder_id, wic_address, date, property_dist_ft, footprint_dist_ft, construction_start_date, pc_ntp_date, construction_complete_date, contract_closed_date) %>%
    distinct()
  
  #setting the lookup_id's default in smp milestone to 4
  smp_milestones['con_phase_lookup_uid'] <- 4
  for(i in 1:nrow(smp_milestones)) {
    if (!is.na(smp_milestones[i, "construction_start_date"]) & !is.na(smp_milestones[i, "construction_complete_date"]) ) {
      if (smp_milestones[i, "date"] >= smp_milestones[i, "construction_start_date"] & smp_milestones[i, "date"] <= smp_milestones[i, "construction_complete_date"]  ) {
        smp_milestones[i, "con_phase_lookup_uid"] <- 1
      } else if (smp_milestones[i, "date"] < smp_milestones[i, "construction_start_date"]) {
        smp_milestones[i, "con_phase_lookup_uid"] <- 3
      } else {
        smp_milestones[i, "con_phase_lookup_uid"] <- 2
      }
    } else if (!is.na(smp_milestones[i, "pc_ntp_date"]) & !is.na(smp_milestones[i, "construction_complete_date"] )) {
      if (smp_milestones[i, "date"] >= smp_milestones[i, "pc_ntp_date"] & smp_milestones[i, "date"] <= smp_milestones[i, "construction_complete_date"]  ) {
        smp_milestones[i, "con_phase_lookup_uid"] <- 1
      } else if (smp_milestones[i, "date"] < smp_milestones[i, "pc_ntp_date"]) {
        smp_milestones[i, "con_phase_lookup_uid"] <- 3
      } else {
        smp_milestones[i, "con_phase_lookup_uid"] <- 2
      }
      
    } else if (!is.na(smp_milestones[i, "construction_start_date"]) & !is.na(smp_milestones[i, "contract_closed_date"])) {
      if (smp_milestones[i, "date"] >= smp_milestones[i, "construction_start_date"] & smp_milestones[i, "date"] <= smp_milestones[i, "contract_closed_date"]  ) {
        smp_milestones[i, "con_phase_lookup_uid"] <- 1
      } else if (smp_milestones[i, "date"] < smp_milestones[i, "construction_start_date"]) {
        smp_milestones[i, "con_phase_lookup_uid"] <- 3
      } else {
        smp_milestones[i, "con_phase_lookup_uid"] <- 2
      }
    } else if (!is.na(smp_milestones[i, "pc_ntp_date"]) & !is.na(smp_milestones[i, "contract_closed_date"])) {
      if (smp_milestones[i, "date"] >= smp_milestones[i, "pc_ntp_date"] & smp_milestones[i, "date"] <= smp_milestones[i, "contract_closed_date"]  ) {
        smp_milestones[i, "con_phase_lookup_uid"] <- 1
      } else if (smp_milestones[i, "date"] < smp_milestones[i, "pc_ntp_date"]) {
        smp_milestones[i, "con_phase_lookup_uid"] <- 3
      } else {
        smp_milestones[i, "con_phase_lookup_uid"] <- 2
      }
      
    } else { 
      smp_milestones[i, "con_phase_lookup_uid"] <- 4
    }
  }
  
  # prep the final table for writing in DB
  wic_system <- smp_milestones %>%
    select(system_id, workorder_id, wic_address, con_phase_lookup_uid, footprint_dist_ft, property_dist_ft) %>%
    distinct()
  
  wic_system[,"footprint_dist_ft"] <- round(wic_system[,"footprint_dist_ft"])
  wic_system[,"property_dist_ft"] <- round(wic_system[,"property_dist_ft"])
  
# Section 5.3 - Identify New/Changed WIC near Systems ----
  mars_wic_system <- dbGetQuery(mars_con, "SELECT * FROM fieldwork.tbl_wic_system")
  # updating tables with the system ids and workorder ids for status record keeping
  mars_wic_system_status <- dbGetQuery(mars_con, "SELECT * FROM fieldwork.tbl_wic_system_status")
  mars_wic_wo_status <- dbGetQuery(mars_con, "SELECT * FROM fieldwork.tbl_wic_wo_status")
  
  # New wic_system
  new_wic_system <- wic_system %>%
    anti_join(mars_wic_system, by = c("system_id","workorder_id"))
  
  # Changed wic_system
  changed_wic_system <- wic_system %>%
    anti_join(new_wic_system, by = c("system_id","workorder_id")) %>%
    anti_join(mars_wic_system, by = c("system_id", "workorder_id", "wic_address", "con_phase_lookup_uid", "footprint_dist_ft", "property_dist_ft")) 
  
  # New system 
  new_wic_system_status <- wic_system %>%
    anti_join(mars_wic_system_status, by = "system_id") %>%
    select(system_id) %>%
    distinct()
  
  # New workorder id
  new_wic_wo_status <- wic_system %>%
    anti_join(mars_wic_wo_status, by = "workorder_id") %>%
    select(workorder_id) %>%
    distinct()

# Section 5.4 - Add New System-WIC to MARS DB ----
  if(nrow(new_wic_system) > 0 & !kill){
    ###Log: Writing new WIC Parcels to MARS
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = 16,
                             exit_code = NA,
                             note = "Writing new WIC-System Intersections to MARS")
    
    dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    tryCatch(
      
      expr = {
        success_wo <- dbWriteTable(mars_con, 
                                   Id(schema = "fieldwork", table = "tbl_wic_system"), 
                                   new_wic_system, 
                                   append= TRUE, 
                                   row.names = FALSE)
      },
      error = function(e) {
        kill <<- TRUE
        errorCode <<- 11
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
      }
    )
    
    #Writing file counts
    if(!kill){ #If the write succeeded
      logMessage <- data.frame(date = as.Date(today()),
                               records = nrow(new_wic_system),
                               type = "New WIC-System Intersections",
                               hash = log_code)
      
      dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_writes_wic"), logMessage, append = TRUE, row.names=FALSE)
    }
    
  }
  
  ## Break Point 5.4: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }
  
# Section 5.5 - Updating WIC-System Intersections ----
  if(nrow(changed_wic_system) > 0 & !kill){
    ###Log: Update Changed SMP-WIC in MARS DB
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = 17,
                             exit_code = NA,
                             note = "Writing updated WIC-System Intersections to MARS")
    
    dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    successful_upwicsmps <- 0
    
    # update changed wic-smp
    for (i in 1:nrow(changed_wic_system)) {
      if(nrow(changed_wic_system) == 0) {break} #SKip this in case we are running chunks that wouldn't usually execute in debug mode
      
      upwicsmp_sql_string <- sprintf(
        "UPDATE fieldwork.tbl_wic_system
         SET con_phase_lookup_uid = %s,
             footprint_dist_ft = %s,
             property_dist_ft = %s,
             wic_address = '%s'
         WHERE workorder_id = %s AND system_id = '%s';",
        changed_wic_system$con_phase_lookup_uid[i],
        ifelse(is.na(changed_wic_system$footprint_dist_ft[i]), 'NULL', changed_wic_system$footprint_dist_ft[i]),
        ifelse(is.na(changed_wic_system$property_dist_ft[i]), 'NULL', changed_wic_system$property_dist_ft[i]),
        changed_wic_system$wic_address[i],
        changed_wic_system$workorder_id[i],
        changed_wic_system$system_id[i]
      )
      
      
      tryCatch(
        
        expr = {
          success_upwicsmp <- dbSendStatement(mars_con, upwicsmp_sql_string)
          successful_upwicsmps <- successful_upwicsmps + 1
        },
        error = function(e) {
          kill <<- TRUE
          errorCode <<- 12
          errorCodes$message[errorCode+1] <<- e$message #Error object is a list
          
        })
    }
    
    #Writing file counts
    if(successful_upwicsmps > 0){ #If the write succeeded
      logMessage <- data.frame(date = as.Date(today()),
                               records = successful_upwicsmps,
                               type = "Updated WIC-System Intersections",
                               hash = log_code)
      
      dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_writes_wic"), logMessage, append = TRUE, row.names=FALSE)
    }
  }
  ## Break Point 5.5: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }
  
# Section 5.6 - Writing new WIC-System Intersections ----
  if(nrow(new_wic_system_status) > 0 & !kill){
    ###Log: Writing new WIC Parcels to MARS
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = 18,
                             exit_code = NA,
                             note = "Writing new WIC-System Intersections to MARS")
    
    dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    tryCatch(
      
      expr = {
        success_wic_system_status <- dbWriteTable(mars_con, 
                                                  Id(schema = "fieldwork", table = "tbl_wic_system_status"), 
                                                  new_wic_system_status, 
                                                  append= TRUE, 
                                                  row.names = FALSE)
      },
      error = function(e) {
        kill <<- TRUE
        errorCode <<- 13
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
      }
    )
    
    #Writing file counts
    if(!kill){ #If the write succeeded
      logMessage <- data.frame(date = as.Date(today()),
                               records = nrow(new_wic_system_status),
                               type = "New Unique WIC Systems",
                               hash = log_code)
      
      dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_writes_wic"), logMessage, append = TRUE, row.names=FALSE)
    }
  }

  ## Break Point 5.6: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }
  
# Section 5.7 - Writing new unique Workorder IDs ----
  if(nrow(new_wic_wo_status) > 0 & !kill){
    ###Log: Writing new unique Workorder IDs to MARS
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = 19,
                             exit_code = NA,
                             note = "Writing new unique Workorder IDs to MARS")
    
    dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    tryCatch(
      
      expr = {
        success_wic_wo_status <- dbWriteTable(mars_con, 
                                              Id(schema = "fieldwork", table = "tbl_wic_wo_status"), 
                                              new_wic_wo_status, 
                                              append= TRUE, 
                                              row.names = FALSE)
      },
      error = function(e) {
        kill <<- TRUE
        errorCode <<- 14
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
      }
    )
    
    #Writing file counts
    if(!kill){ #If the write succeeded
      logMessage <- data.frame(date = as.Date(today()),
                               records = nrow(new_wic_system_status),
                               type = "New Unique Workorder IDs",
                               hash = log_code)
      
      dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_writes_wic"), logMessage, append = TRUE, row.names=FALSE)
    }
  }
  
  ## Break Point 5.7: Bad Write ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                             milestone = NA,
                             exit_code = errorCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }
  
# Section 6 - Summary Statistics ----
  if(nrow(changed_wic_system)>0){
    # Prep the table
    new_wic_kable <- new_wic_system %>%
      inner_join(cw_workorders, by = "workorder_id") %>%
      select(`System ID` = system_id, `Work Order ID` = workorder_id , Date = date,`WIC Address` = wic_address, `Dist. Footprint (ft)` = footprint_dist_ft, `Dist. Property (ft)` = property_dist_ft) %>%
      arrange(desc(Date)) %>%
      distinct()
    
    kable(new_wic_kable)
    
    # Prep the table
    changed_wic_kable <- changed_wic_system %>%
      inner_join(cw_workorders, by = "workorder_id") %>%
      select(`System ID` = system_id, `Work Order ID` = workorder_id , Date = date,`WIC Address` = wic_address, `Dist. Footprint (ft)` = footprint_dist_ft, `Dist. Property (ft)` = property_dist_ft) %>%
      arrange(desc(Date)) %>%
      distinct()
    
    kable(changed_wic_kable)
  }
 
# Section 7 - End Script ----
  ###Log: End
  logMessage <- data.frame(date = as.Date(today()), hash = log_code,
                           milestone = NA,
                           exit_code = errorCode,
                           note = errorCodes$message[errorCode+1])
  
  dbWriteTable(mars_con, RPostgres::Id(schema = "log", table = "tbl_script_wic"), logMessage, append = TRUE, row.names=FALSE)
  
  dbDisconnect(cityworks_con)
  dbDisconnect(mars_con)
  dbDisconnect(gisdata_con)
  