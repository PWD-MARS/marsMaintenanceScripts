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
    dbname = "sandbox_dtime",
    user= Sys.getenv("admin_uid"),
    password = Sys.getenv("admin_pwd"),
    timezone = NULL)},
  error = function(e){e})

#Section 1: Environment variables
  #1.1 PATH
  #List environment variables with / paths instead of \
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
  
  dbExecute(marsDBCon, paste0("update admin.tbl_vars_windows set varvalue = '", dbpaths, "' where varname = 'PATH'"))
  
  #1.2 PGPASSFILE
  pgpassfile <- Sys.getenv("PGPASSFILE") %>% 
    str_replace_all(pattern = "\\\\", replacement = "/")
  
  dbExecute(marsDBCon, paste0("update admin.tbl_vars_windows set varvalue = '", pgpassfile, "' where varname = 'PGPASSFILE'"))
  
  #MARSBACKUPDIR
  marsbackupdir <- Sys.getenv("MARSBACKUPDIR") %>% 
    str_replace_all(pattern = "\\\\", replacement = "/")
  
  dbExecute(marsDBCon, paste0("update admin.tbl_vars_windows set varvalue = '", marsbackupdir, "' where varname = 'MARSBACKUPDIR'"))
  
  
 