### PG14 Sequence Restarts
## This is a script used to ensure the next value in every uid sequence
## within pg14 is the next largest available uid value. This is to  avoid issues with 
## duplicate uid's during writing to databases within scripts and applications.

## Written by: Brian Cruice
## Written on: 2/1/2023

### 0.1 packages
library(tidyverse)
library(odbc)
library(DBI)
library(pwdgsi)
library(lubridate)


# Failsafe to see if sequences should be altered
WRITE <- TRUE

# Connect to database
mars_con <- odbc::dbConnect(odbc::odbc(), "mars14_data")


# Get list of sequences
mars_sequences <- dbGetQuery(mars_con,"SELECT * FROM information_schema.sequences ")


# limit to those with start value == 1

sequences_to_update <- mars_sequences %>% dplyr::filter(start_value == 1)

# do not include "external" schema

sequences_to_update <- sequences_to_update %>% dplyr::filter(sequence_schema != "external")


#see if entry date is the only one that doesn't work

# loop to get last value of each of these sequences

for(i in 1:nrow(sequences_to_update)){
  
  # get table name
  tbl_name <- paste0(sequences_to_update$sequence_schema[i],".",
                     gsub(sequences_to_update$sequence_name[i],pattern = "_uid_seq", replacement = "")
  )
  
  # get list of column names in the table
  tbl_cols <- dbGetQuery(mars_con,
                         paste0("SELECT column_name
                                FROM information_schema.columns
                                WHERE table_schema = '",sequences_to_update$sequence_schema[i],"'
                                AND table_name   = '",
                                gsub(sequences_to_update$sequence_name[i],pattern = "_uid_seq", replacement = ""),"'")) %>%
    unlist() %>% as.vector()
  
  
  # First column containing "uid" in the table; stops from choosing foreign keys.
  # Used in lieu of gsub due to some discrepency in  uid column naming conventions (see ) 
  # This is not bulletproof, but works for our database construction.
  uid_col <- tbl_cols[grep(x = tbl_cols, pattern = "uid")[1]]
  
  
  # use uid name and table name to get max uid value
  last_val <- dbGetQuery(mars_con,
                         paste0("SELECT max(",uid_col,") FROM ",tbl_name)) %>%
    unlist()
  
  # store max uid value to alter sequence
  sequences_to_update$last_value[i] <- last_val
  
  # new value to restart sequence
  restart_value <- ifelse(!is.na(sequences_to_update$last_value[i]),
                          sequences_to_update$last_value[i] + 1,
                          1)
  
  # Check if we are rewriting sequences should be written
  if(WRITE == TRUE){
    # send query to alter sequence
    dbGetQuery(mars_con,
               paste0("ALTER SEQUENCE ",sequences_to_update$sequence_schema[i],".",
                      sequences_to_update$sequence_name[i]," RESTART WITH ",restart_value,";"))
    
  }
  
}