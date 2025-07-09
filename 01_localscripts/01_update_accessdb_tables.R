# Section 1: Setting runtime parameters ----
  
  #Load from local libs
  .libPaths("./lib")
  readRenviron("./.Renviron")
  
  #Dplyr stuff
  library(magrittr)
  library(tidyverse)
  
  #Database Stuff
  library(pool)
  library(RODBC)
  library(RPostgres)
  
  #Other stuff
  library(knitr)
  library(digest)
  
  options(stringsAsFactors=FALSE)
  
  errorCodes <- data.frame(code = 0:9,
                           message = c("Execution successful.",
                                       "Could not connect to Postgres DB. Is Postgres down?",
                                       "No Access DBs found when crawling public/private site folders. Are we connected to Active Directory?",
                                       NA, #Orphaned Access DBs enumerated in block
                                       NA, #Write error from TryCatch will be used
                                       NA, #Write error from TryCatch will be used
                                       "Access DBs found without canonical tables",
                                       NA, #Write error from TryCatch will be used
                                       NA, #Write error from TryCatch will be used
                                       NA #Write error from TryCatch will be used
                           ), stringsAsFactors=FALSE)
  
  kill = FALSE
  errorCode = 0
  logCode <- digest(now()) #Unique ID for the log batches
  
  ## Parser functions ----
  #Public SMP parser function
  #Extract public SMP IDs (X-Y-Z) from strings (like a file path)
  #Returns X-Y-Z if it finds it, NA if it doesn't. If it finds multiple X-Y-Zs in one string, it returns the first one.
  parsePublicSMPs <- function(strings){
    finalvector <- rep(NA, length(strings))
    matchindex <- which(grepl("\\d+-\\d+-\\d+", strings))
    finalvector[matchindex] <- regexpr("\\d+-\\d+-\\d+", strings) %>% {regmatches(strings, .)}
    return(finalvector)
  }
  
  #Public OW parser function
  #Extract monitoring device IDs (OW1, GW4, etc) at public SMPs from strings. String must be in the form X-Y-Z_ABC.
  #Returns AAA extracted from X-Y-Z_AAA if it finds it. Returns NA if it doesn't.
  #If it finds X-Y-Z_ABC followed by X-Y-Z_DEF within the same string, it returns ABC.
  #If it finds X-Y-Z_ABCD in the string, it returns ABC.
  parsePublicOWs <- function(strings){
    finalvector <- rep(NA, length(strings))
    matchindex <- which(grepl("\\d+-\\d+-\\d+.+?([A-Za-z]{2}\\d{1})", strings, perl=TRUE))
    finalvector[matchindex] <- gsub("^.*\\d+-\\d+-\\d+.+?([A-Za-z]{2}\\d{1}).*$", "\\1", strings, perl=TRUE)[matchindex]
    finalvector %<>% toupper
    return(finalvector)
  }
  
  #Private SMP parser function
  #Extract private SMP IDs (XXXXX) from strings (like a file path)
  #Returns XXXXX if it finds it, NA if it doesn't. If it finds multiple XXXXXs in one string, it returns the first one.
  parsePrivateSMPs <- function(strings){
    finalvector <- rep(NA, length(strings))
    matchindex <- which(grepl("\\d{5}", strings))
    finalvector[matchindex] <- regexpr("\\d{5}", strings) %>% {regmatches(strings, .)}
    return(finalvector)
  }
  
  #Private OW parser function
  #Extract monitoring device IDs (OW1, GW4, etc) at private SMPs from strings. String must be in the form XXXXX_ABC.
  #Returns AAA extracted from XXXXX_AAA if it finds it. Returns NA if it doesn't.
  #If it finds XXXXX_ABC followed by XXXXX_DEF within the same string, it returns ABC.
  #If it finds XXXXX_ABCD in the string, it returns ABC.
  parsePrivateOWs <- function(strings){
    finalvector <- rep(NA, length(strings))
    matchindex <- which(grepl("\\d{5}.+?([A-Za-z]{2}\\d{1})", strings, perl=TRUE))
    finalvector[matchindex] <- gsub("^.*\\d{5}.+?([A-Za-z]{2}\\d{1}).*$", "\\1", strings, perl=TRUE)[matchindex]
    finalvector %<>% toupper
    return(finalvector)
  }
  
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
  
  ## Other basic parameters ----
  publicsitefolder <- "//pwdoows/oows/Watershed Sciences/GSI Monitoring/02 GSI Monitoring Sites"
  privatesitefolder <- "//pwdoows/oows/Watershed Sciences/GSI Monitoring/02 GSI Monitoring Sites/z_Private Monitoring Sites"

## Break Point 1: Failed DB connection ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    stop(message = errorCodes$message[errorCode+1])
  }

# Section 2: Crawling DB Folders ----
  ###Log: Finding AccessDB files
  logMessage <- data.frame(date = as.Date(today()),
                           milestone = 2,
                           exit_code = NA,
                           hash = logCode,
                           note = "Searching for AccessDB Files")
  
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_accessdb"), logMessage, append = TRUE, row.names=FALSE)
  
  # ###Test truncation. Remove this before you ship it.
  # dbGetQuery(marsDBCon, "truncate table admin.tbl_accessdb;")
  
  
  ###Section 1.1 Scan site folders
  #Find public site folders within 02 GSI Monitoring Sites
  #Site folders will end with an underscore and a number (eg _123)
  publicsitefolders <- grep("_\\d+$", list.dirs(publicsitefolder, recursive = FALSE), value = TRUE)
  
  #Find private site folders within 02 GSI Monitoring Sites/z_Private Monitoring Sites
  #Site folders will end with an underscore, three 4-character blocks, and a 2-character block separated by hyphens
  #(eg _FY16-WAKE-4282-01)
  privatesitefolders <- grep("_\\w{4}-\\w{4}-\\w{4}-\\w{2}$", list.dirs(privatesitefolder, recursive = FALSE), value = TRUE)
  
  #Look in each folder for an Access DB
  publicaccessdbs <- list.files(publicsitefolders, "\\.accdb$|\\.mdb$", recursive=FALSE, full.names=TRUE)
  privateaccessdbs <- list.files(privatesitefolders, "\\.accdb$|\\.mdb$", recursive=FALSE, full.names=TRUE)
  
  #Fetch current version of the AccessDB table
  accessdbtable_server <- dbGetQuery(marsDBCon, "SELECT * FROM admin.tbl_accessdb")
  
  #Refresh materialized view so it has the newest cache of SMP IDs
  #If we don't do this, new IDs won't be found, and we will get an insertion error.
  dbGetQuery(marsDBCon, "REFRESH MATERIALIZED VIEW external.mat_assets WITH DATA;")
  
  ####Error check - did we find access DB files?
  if(length(publicaccessdbs) == 0 | length(privateaccessdbs) == 0)
  {
    kill = TRUE
    errorCode = 2
  }

## Break Point 2: No Filesystem Access ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    ###Log: End
    logMessage <- data.frame(date = as.Date(today()), 
                             milestone = NA,
                             exit_code = errorCode,
                             hash = logCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_accessdb"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }  

#Section 3: Parsing new Databases ----
  ###Log: Filtering for New Databases
  logMessage <- data.frame(date = as.Date(today()),
                           milestone = 3,
                           exit_code = NA,
                           hash = logCode,
                           note = "Filtering for New Databases")
  
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_accessdb"), logMessage, append = TRUE, row.names=FALSE)
  
  #Exclude DBs already in the table
  publicaccessdbs_new <- publicaccessdbs[!(publicaccessdbs %in% accessdbtable_server$filepath)]
  privateaccessdbs_new <- privateaccessdbs[!(privateaccessdbs %in% accessdbtable_server$filepath)]
  
  #Parse the filepaths of the new DBs for SMP IDs and OW suffixes
  publicaccessdbs_df <- data.frame(filepath_server = publicaccessdbs_new) %>% mutate(smp_id = parsePublicSMPs(filepath_server), ow_suffix = parsePublicOWs(filepath_server))
  privateaccessdbs_df <- data.frame(filepath_server = privateaccessdbs_new) %>% mutate(smp_id = parsePrivateSMPs(filepath_server), ow_suffix = parsePrivateOWs(filepath_server))
  
  #Connect the public and private sites
  allnewaccessdbs_df <- bind_rows(publicaccessdbs_df, privateaccessdbs_df)
  
  #Fetch current version of the OW table
  ow <- dbGetQuery(marsDBCon, "SELECT * FROM fieldwork.tbl_ow")
  
  #Join our new AccessDB file paths to the current OW table (to attach ow_uids)
  allnewaccessdbs_ow <- left_join(allnewaccessdbs_df, ow, by = c("smp_id", "ow_suffix"))
  
  #New Access DBs that have matching defined OWs need can be added to the `admin.tbl_accessdb` table
  output_newAccessDBs <- filter(allnewaccessdbs_ow, !is.na(ow_uid)) %>%
    transmute(ow_uid, filepath = filepath_server)
  
  #New Access DBs that don't have defined OWs need OWs defined for them
  #All this script can do is report on these
  ####Author's note: We could guess pretty well at creating new OWs in the database for these, but we aren't solving that problem now
  error_accessdbswithoutow <- filter(allnewaccessdbs_ow, is.na(ow_uid)) %>% mutate(filepath = filepath_server, status = "orphaned") %>%
    select(smp_id, ow_suffix, ow_uid, filepath, status)
  
  #Which known DBs weren't found? (Maybe deleted or moved)
  error_existingdbs_notfound <- filter(accessdbtable_server, !(filepath %in% c(publicaccessdbs, privateaccessdbs))) %>%
    left_join(ow) %>%
    transmute(smp_id, ow_suffix, ow_uid, filepath, status = "missing")
  
  ####Error check - any dbs in improper states?
  if(any(nrow(error_accessdbswithoutow) > 0, nrow(error_existingdbs_notfound) > 0)){
    kill = TRUE
    errorCode = 3
    errorTable = bind_rows(error_accessdbswithoutow, error_existingdbs_notfound)
    
    orphans <- errorTable %>%
      mutate(orphans = paste(smp_id, ow_suffix)) %>%
      pull(orphans) %>%
      paste(sep = ", ")
    
    message = paste("\nOrphaned:", orphans)
  }

## Break Point 3: Orphaned AccessDBs ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    ###Log: End
    logMessage <- data.frame(date = as.Date(today()), 
                             milestone = NA,
                             exit_code = errorCode,
                             hash = logCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_accessdb"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }  

# Section 4: Writing new AccessDB Paths ----
  ###Log: Writing New Databases
  logMessage <- data.frame(date = as.Date(today()),
                           milestone = 4,
                           exit_code = NA,
                           hash = logCode,
                           note = "Writing New AccessDB Databases")
  
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_accessdb"), logMessage, append = TRUE, row.names=FALSE)
  
  
  print(paste("### New Access DBs to add to `admin.tbl_accessdb`: ",   nrow(output_newAccessDBs)))
  
  filter(allnewaccessdbs_ow, !is.na(ow_uid)) %>%
    transmute(smp_id, ow = ow_suffix, uid = ow_uid,
              filepath = paste(basename(dirname(filepath_server)), basename(filepath_server), sep = "/")) %>%
    kable()
  
  tryCatch(
    
    expr = {
      dbWriteTable(marsDBCon, RPostgres::Id(schema = "admin", table = "tbl_accessdb"), output_newAccessDBs, append= TRUE, row.names = FALSE)
      success <<- TRUE
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
                             records = nrow(output_newAccessDBs),
                             type = "New DBs",
                             hash = logCode)
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_writes_accessdb"), logMessage, append = TRUE, row.names=FALSE)
  }

  ## Break Point 4: AccessDB Path Write Error ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    ###Log: End
    logMessage <- data.frame(date = as.Date(today()), 
                             milestone = NA,
                             exit_code = errorCode,
                             hash = logCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_accessdb"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }  

# Section 5: Checking AccessDBs for Canonical Tables ----
  ###Log: Start
  logMessage <- data.frame(date = as.Date(today()),
                           milestone = 5,
                           exit_code = NA,
                           hash = logCode,
                           note = "Checking AccessDBs for Canonical Table Names")
  
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_accessdb"), logMessage, append = TRUE, row.names=FALSE)
  
  ###Pull Access table again
  accessdbtable_server <- dbGetQuery(marsDBCon, "SELECT * FROM admin.tbl_accessdb")
  
  #Compose a data frame to use to check for tables in the Access databases
  #We need to verify that the tables we think are there still exist, and check for new ones
  #If there's a value in accessdb.datatable or accessdb.sumptable, we will use those
  #If there isn't, we will compose a guess as to what they might be based on ow.smp_id and ow.ow_suffix
  #We will also create a variable for the result of our guesses
  accessdb_tableguesses <- left_join(accessdbtable_server, ow, by = "ow_uid") %>%
    mutate(datatable_guess = ifelse(is.na(datatable), paste(smp_id, ow_suffix, "CWL_Monitoring", sep = "_"), datatable),
           datatable_guessresult = NA)
  
  for(i in 1:nrow(accessdb_tableguesses)){
    #Debug statement. Uncomment if running interactively.
    print(paste("Accessing", basename(accessdb_tableguesses$filepath[i])))
    
    #We need RODBC to connect to the DBs because odbc::odbc throws a "DSN too long" error. I would like to fix this sometime
    tryCatch(
      expr = {
        accessdbCon <- RODBC::odbcConnectAccess2007(accessdb_tableguesses$filepath[i])
        
        #List the tables in each database and check to see if we can find the guessed table name in there
        dbtables <- RODBC::sqlTables(accessdbCon)
        accessdb_tableguesses$datatable_guessresult[i] <- dbtables %>% {accessdb_tableguesses$datatable_guess[i] %in% .$TABLE_NAME}
        
        RODBC::odbcClose(accessdbCon)
      },
      error = function(e){
        kill <<- TRUE
        errorCode <<- 5
        errorCodes$message[errorCode+1] <<- e$message #Error object is a list
      }
    )
  }
  
  ###Log: Preparing Script Outputs
  logMessage <- data.frame(date = as.Date(today()),
                           milestone = 6,
                           exit_code = NA,
                           hash = logCode,
                           note = "Preparing Script Outputs")
  
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_accessdb"), logMessage, append = TRUE, row.names=FALSE)
  
  #Create the data frames of results to print/summarize
  #Access DBs where our current stored table names are both correct
  #Either we found what we expected to find, or we didn't find anything and expected to find nothing
  #IE: Our guess equaled what we thought was there, or expected our guess to be wrong because we didn't think there was a table
  #We do it this way so we can more easily detect if a DB we thought had no table had one created since we last checked
  #If we just checked for what we thought was there, we'd still have to check again to see if any new tables got created
  accessdb_nochange <- filter(accessdb_tableguesses,
                              (datatable_guessresult == TRUE & datatable_guess == datatable))
  
  #Access DBs where we we found tables where we thought there were none
  #IE: We had no table name stored, and we found one with our guesses
  #Write these to the DB
  accessdb_foundnewtables <- filter(accessdb_tableguesses,
                                    (datatable_guessresult == TRUE & is.na(datatable)))
  
  #Trim columns that don't go in the table
  accessdb_newtablestowrite <- transmute(accessdb_foundnewtables, accessdb_uid,
                                         datatable = ifelse(datatable_guessresult, datatable_guess, datatable))
  
  #Access DBs where we didn't find tables we thought were there
  ####Error on this
  error_missedsomething <- filter(accessdb_tableguesses,
                                  (datatable_guessresult == FALSE & !is.na(datatable))) %>%
    transmute(smp_id, ow_suffix, filepath, status = "Existing Table Gone")
  
  #Access DBs with no canonical tables
  ####Error on this
  #Only checking for data table, because without a data table, a sump table is meaningless
  error_nocanonical <- filter(accessdb_tableguesses, datatable_guessresult == FALSE, is.na(datatable)) %>%
    transmute(smp_id, ow_suffix, filepath, status = "No Tables Anywhere")

  ####Error check - any dbs in improper states?
  if(any(nrow(error_missedsomething) > 0, nrow(error_nocanonical) > 0)){
    kill = TRUE
    errorCode = 6
    errorTable = bind_rows(error_missedsomething, error_nocanonical)
    
    tablesmissing <- errorTable %>%
      mutate(orphans = paste(smp_id, ow_suffix)) %>%
      pull(orphans) %>%
      paste(sep = ", ")
    
    message = paste("Missing Tables:", tablesmissing)
  }

## Break Point 5: Malformed Databases ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    ###Log: End
    logMessage <- data.frame(date = as.Date(today()), 
                             milestone = NA,
                             exit_code = errorCode,
                             hash = logCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_accessdb"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  }    

# Section 6: Writing Canonical Tables ----
  
  ###Log: Writing New Canonical Table Names
  logMessage <- data.frame(date = as.Date(today()),
                           milestone = 7,
                           exit_code = NA,
                           hash = logCode,
                           note = "Writing New Canonical Table Names")
  
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_accessdb"), logMessage, append = TRUE, row.names=FALSE)
  
  tryCatch(
    
    expr = {
      dbWriteTable(marsDBCon, RPostgres::Id(table = "tbl_accessdb_temp"), accessdb_newtablestowrite, temporary = TRUE,row.names = FALSE, overwrite = TRUE)
      
      #Use this temp table to update the existing accessdb table
      #Temporary table means R will copy the string "NA" into the table when a value is NA.
      #We must explicitly exclude these or else they'll end up in the final table and fuck everything up.
      
      dbGetQuery(marsDBCon, "update admin.tbl_accessdb a set datatable = t.datatable from tbl_accessdb_temp t where a.accessdb_uid = t.accessdb_uid")
      
      success <<- TRUE
    },
    
    error = function(e) {
      kill <<- TRUE
      errorCode <<- 7
      errorCodes$message[errorCode+1] <<- e$message #Error object is a list
    }
  )

## Break Point 6: Canonical Table Write Error ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    ###Log: End
    logMessage <- data.frame(date = as.Date(today()), 
                             milestone = NA,
                             exit_code = errorCode,
                             hash = logCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_accessdb"), logMessage, append = TRUE, row.names=FALSE)
    
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
                             hash = logCode,
                             note = errorCodes$message[errorCode+1])
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_accessdb"), logMessage, append = TRUE, row.names=FALSE)
    
  }  
