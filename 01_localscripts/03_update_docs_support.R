#Load from local libs
setwd("C:/marsMaintenanceScripts/01_localscripts")
.libPaths("./lib")
readRenviron("./.Renviron")

#Dplyr stuff
library(tidyverse)

#Database Stuff
library(pool)
library(RPostgres)

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

  #Exit codes
  errorCodes <- data.frame(code = 0:3,
                           message = c("Execution successful.",
                                       "Could not connect to Postgres DB. Is Postgres down?",
                                       NA, #Write error from TryCatch will be used
                                       "Warning: unknown MARS environment variable(s): "
                           ), stringsAsFactors=FALSE)
  
  kill = FALSE
  errorCode = 0
  logCode <- digest(now()) #Unique ID for the log batches

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
  
#Section 1: Environment variables
  ###Log: Finding known variables
  logMessage <- data.frame(date = as.Date(today()),
                           milestone = 2,
                           exit_code = NA,
                           hash = logCode,
                           note = "Checking known Environment Variables")
  
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_docs"), logMessage, append = TRUE, row.names=FALSE)
  
  
  # 1.1 PATH
  # List environment variables with / paths instead of \
  paths <- Sys.getenv("PATH") %>% 
    str_replace_all(pattern = "\\\\", replacement = "/") %>%
    strsplit(split = ";") %>%
    unlist
  
  #Only keep paths that we have added to support server operations
  #Things we don't need:
   #R prepends rtools44 items to compile R code
   #Windows has many internal search directories always in the system path
   #MARS policy requires admin-relevant software to be installed outside a user's home directory
   #When running in RStudio, it appends some Quarto stuff to the path
  keep <- str_detect(paths, 
                     pattern = "rtools44|Windows|Users|RStudio", 
                     negate = TRUE)
  
  #Compose the string for the database:
  dbpaths <- paste(paths[keep], collapse = ";")
  
  # 1.2 PGPASSFILE
  pgpassfile <- Sys.getenv("PGPASSFILE") %>% 
    str_replace_all(pattern = "\\\\", replacement = "/")
  
  #1.3 MARSBACKUPDIR
  marsbackupdir <- Sys.getenv("MARSBACKUPDIR") %>% 
    str_replace_all(pattern = "\\\\", replacement = "/")

  #1.4 Update the known vars
  tryCatch(
    
    expr = {
      dbExecute(marsDBCon, paste0("update admin.tbl_vars_windows set varvalue = '", dbpaths, "' where varname = 'PATH'"))
      dbExecute(marsDBCon, paste0("update admin.tbl_vars_windows set varvalue = '", pgpassfile, "' where varname = 'PGPASSFILE'"))
      dbExecute(marsDBCon, paste0("update admin.tbl_vars_windows set varvalue = '", marsbackupdir, "' where varname = 'MARSBACKUPDIR'"))
      success <<- TRUE
    },
    error = function(e) {
      kill <<- TRUE
      errorCode <<- 2
      errorCodes$message[errorCode+1] <<- e$message #Error object is a list
    }
  )
  
  ## Break Point 2: Known vars not updated correctly ----
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
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_docs"), logMessage, append = TRUE, row.names=FALSE)
    
    stop(message = errorCodes$message[errorCode+1])
  } 
# Section 2: Search for other environment variables prepended with MARS
  ###Log: Finding unknown variables
  logMessage <- data.frame(date = as.Date(today()),
                           milestone = 3,
                           exit_code = NA,
                           hash = logCode,
                           note = "Checking for Unknown Environment Variables")
  
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_docs"), logMessage, append = TRUE, row.names=FALSE)
  
  vars <- names(Sys.getenv())
  marsvars <- vars[grep("^MARS", vars)]
  
  #What variables are present in the table?
  dbvars <- dbGetQuery(marsDBCon, "select varname from admin.tbl_vars_windows")
  
  #Are there any MARS variables not present in the DB?
  newvars <- setdiff(marsvars, dbvars$varname)
  
  #If new vars found, issue a warning in the log table
    #Monica note: It would be nice to just harvest them and save them
    #but this will do for now.
  if(length(newvars) > 0){
    kill <- TRUE
    errorCode <- 3
  }
    
  ## Break Point 3: Unknown var detected ----
  if(kill == TRUE){
    print("# Script Results: Error\n")
    print(paste("## Error Code:", errorCode, "\n"))
    print(paste("## Error Message: ", errorCodes$message[errorCode+1]))
    
    ###Log: End
    logMessage <- data.frame(date = as.Date(today()), 
                             milestone = NA,
                             exit_code = errorCode,
                             hash = logCode,
                             note = paste0(errorCodes$message[errorCode+1], paste(newvars, collapse = ", ")))
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_docs"), logMessage, append = TRUE, row.names=FALSE)
    
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
    
    dbWriteTable(marsDBCon, RPostgres::Id(schema = "log", table = "tbl_script_docs"), logMessage, append = TRUE, row.names=FALSE)
    
  }  