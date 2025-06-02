#Database Stuff
library(pool)
library(RPostgres)
library(tidyverse)
library(lubridate)
library(pwdgsi)
library(padr)

#Other stuff
library(knitr)
library(digest)
options(stringsAsFactors=FALSE)

#Relevant folder paths
radarFolder <- "//pwdoows/oows/Modeling/Data/H&H Databases/RadarRainfall/Data"
unzipFolder <- "//pwdoows/oows/Watershed Sciences/GSI Monitoring/12 GSI Data Management and Analysis/01 Admin/03 MARS Maintenance Scripts/unzip"

#Database connection
marsDBCon <- tryCatch({
  dbPool(
    drv = RPostgres::Postgres(),
    host = "PWDMARSDBS1",
    port = 5434,
    dbname = "demo_deployment",
    user= Sys.getenv("admin_uid"),
    password = Sys.getenv("admin_pwd"),
    timezone = NULL)},
  error = function(e){e})

#Purge the data table so we can continually retest
dbExecute(marsDBCon, "truncate table data.test_tbl_radar_rain restart identity;")

#H&H radar rainfall data files
hh_rawfiles <- list.files(radarFolder, pattern = "*\\.zip$") #List all the zip files in that folder

#Prepare to extract the files
new_rawfiles <- data.frame(filepath = paste(radarFolder, 
                                            hh_rawfiles, 
                                            sep = "/"), #Compose the full path to extract the files
                           stringsAsFactors = FALSE) %>%
  mutate(yearmon = str_replace(filepath, ".*(\\d{4})-(\\d{2}).*", "\\1\\2"), #Extract the YYYYMM yearmonth from the filename
         #Regex notes:
         #() creates a capture group, referred to as \\1 and \\2 (capture group 1 and 2) in the replacement string
            #\\d means "any digit 0-9", {4} and {2} means "4 (or 2) of the preceeding character"
            #- is a literal hyphen, and is outside the capture groups
            #so the capture groups means DDDD and DD, where D is any digit 0-9
         #The . means "any character", and the * means "any number of the preceeding character"
            #So the .* before the first capture group means everything before the YYYY
            #And the .* after the second capture group means everything after the MM
            #The replacement string is just the reference to both capture groups
            #So everything not in the capture group is replaced with nothing
         #In practice, this means /path/to/foo_2025-04_bar.zip is replaced by 202504
            #We want this because the CSV containing the data in 2025-04.zip is named 202504.csv
            #For more information, see https://regexr.com
         datafile = paste0(yearmon, ".csv")) #Compose that CSV's file name


#Only import grid cells in Philadelphia county
phillycells <- dbGetQuery(marsDBCon, "select radar_uid from admin.tbl_radar")

#Unzip the files
dir.create(unzipFolder, showWarnings = FALSE)
for(i in 1:nrow(new_rawfiles)){

  #i <- 10 #DEBUG: only process the tenth file (has a dst fallback, these introduce parsing corner cases to test)

  #Unzip the file
  file.copy(from = new_rawfiles$filepath[i], to = unzipFolder, overwrite = TRUE)
  unzip(new_rawfiles$filepath[i], exdir = unzipFolder, files = new_rawfiles$datafile[i]) #extract only the CSV we want
  
  #Read the file
  currentfile <- paste(unzipFolder, new_rawfiles$datafile[i], sep = "/")
  currentdata <- read_csv(currentfile, 
                          col_names = c("dtime_raw", "tzone", "radar_uid", "rainfall_in"), #CSV file has no column headers
                          col_types = c("c", "c", "i", "d")) #character, character, integer, double (see ?read_csv col_types argument)
  #We need to read the datetime in as a string to prevent R from mishandling time zones
  
  #The tzone member of this data frame contains one of two values - "EST" or "EDT"
  #The dtime member is the raw local clock time and corrects for daylight savings 
    #ie, it skips 2:00 AM at the spring-forwards and repeats 1:00 AM at the fall-back
  
  #When we parse dtime_raw with ymd_hm, we will run into a limitation of lubridate's datetime parser
  #The parser can only apply a single time zone value to the entire vector, 
    #and will always return the same UTC offset when given the same input string
    #eg 1:00 AM on the fall-back day will always be given in a UTC offset of -04
  
  #This is a problem when daylight savings time falls back. 
    #As stated above, the clock values in the 1:00 AM hour will be repeated when this happens.
  #In order to properly keep the time series chronologically ordered, we need different UTC offsets for the repeated 1:00 AM hour
    #eg 1:00:00-04, 1:15:00-04, 1:30:00-04, 1:45:00-04, 1:00:00-05, 1:15:00-05, etc
  #If we don't have this, the dtimes will sort incorrectly (00:45:00, 1:00:00, 1:00:00, 1:15:00, etc)
    #It will also violate the uniqueness constraint in our SQL tables, where only one combination of each dtime and radar_uid is permissible
  #Only the repeated hour will have this problem
    #It will correctly give a -05 offset at 03:00:00, when America/New_York always has an offset of -05
    #Likewise, the spring-forwards 4:00:00 hour will correctly have an offset of -04, because 4 AM on the spring-forwards day is always -04
  
  #In order to correct the repeated hour, we need to use the force_tzs function to coerce the offsets of the repeated times into their correct form
  
  #This bug took me 6 hours to fix, and I tried everything from parsing the data frame in two separate batches (needlessly complex)
    #To manually composing a -05/-04 UTC offset string to feed to ymd_hm (creates parse errors)
    #As far as I know, this is the most elegant solution that exists with our current tools
  
  finalCurrentData <- currentdata %>%
    filter(radar_uid %in% phillycells$radar_uid) %>% #Only the grid cells in Philadelphia county
        #Normally, we would strip the 0 values so we don't store gigabytes of extra data
        #For now, we will not strip them, so it gives us a chance to correctly parse the DST fallback
    mutate(dtime_intermediate = ymd_hm(dtime_raw, tz = "America/New_York"),
           dtime = force_tzs(dtime_intermediate, tzones = tzone)) %>% #Correct for the above time zone offset error
    select(dtime, radar_uid, rainfall_in)

  #Write the data to the table, to test for the uniqueness constraint validation
  dbWriteTable(marsDBCon, RPostgres::Id(schema = "data", table = "test_tbl_radar_rain"), finalCurrentData, append= TRUE, row.names = FALSE)
  #Succeeds!

#S.2 Reread the data and validate it as identical

#Read the data
radardata_mars <- dbGetQuery(marsDBCon, "select * from data.test_tbl_radar_rain")

#To validate the data, we will...
  # Count the rows of each data frame
  # Sum the rainfall values for each radar grid cell
  # Recompose the ymd_hm string from the original file and do a symdiff()

#Count the rows...
  rowsEqual <- nrow(radardata_mars) == nrow(finalCurrentData)

#Sum the rainfall for each grid cell
  fileTotals <- group_by(finalCurrentData, radar_uid) %>%
    summarize(fileTotal_in = sum(rainfall_in))

  marsTotals <- group_by(radardata_mars, radar_uid) %>%
    summarize(marsTotal_in = sum(rainfall_in))

  unitedTotals <- left_join(fileTotals, marsTotals) %>%
    mutate(equal = fileTotal_in == marsTotal_in)

  totalsEqual <- all(unitedTotals$equal == TRUE)

#Recompose ymd_hm and do a symdiff()
  #Recompose the dtime_raw from the data from our DB
  mars_recomposed <- radardata_mars %>%
    mutate(dtime_parsed = as.character(dtime),
          dtime_stripped = str_extract(dtime_parsed, "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}"),
            #Regex notes
              #The raw date is a YYYY-MM-DD HH:MM
              #When R parses it with ymd_hm(), it returns YYYY-MM-DD HH:MM:SS on non-midnight times
                #and YYYY-MM-DD on midnights
            #In order to make sure the parsed datetimes match the raw ones
              #we need to strip that terminal :00 from non-midnights
              #and add 00:00 to midnights
            #The regex matches DDDD-DD-DD DD:DD, see the regex in the previous section for more info
              #The final transformation is YYYY-MM-DD HH:MM:SS -> YYYY-MM-DD HH:MM for non-midnights
              #Midnights return NA, which we will handle next
          dtime_midnights = str_replace(dtime_parsed, "(\\d{4}-\\d{2}-\\d{2})$", "\\1 00:00"),
            #Regex notes
            #The regex pattern matches DDDD-DD-DD, see the regex in the previous section for more info
              #The terminal $ refers to the end of the string, so this will only match strings that have no trailing HH:MM values
              #This will only happen when the clock time is midnight, as explained above
            #This pattern is inside of a capture group, so we can replace it in the subsequent string
              #The replacement is \\1 (ie, the contents of capture group 1) plus a 00:00 (midnight on the clock)
            #The final transformation is YYYY-MM-DD -> YYYY-MM-DD 00:00 for all midnights
              #and NA for every non-midnight
              #We will unite these values with a coalesce() next
          dtime_reconstructed = coalesce(dtime_stripped, dtime_midnights)) %>% #This returns the first non-missing value, like an SQL coalesce()
  select(dtime_raw = dtime_reconstructed, radar_uid, rainfall_in)

  #Prepare the raw data for comparison
  raw_comparison <- currentdata %>%
    filter(radar_uid %in% phillycells$radar_uid) %>% #Only grid cells in philadelphia
    select(-tzone) #Drop the tzone column since mars_recomposed won't have one

  differences <- symdiff(raw_comparison, mars_recomposed)

  datasetsEqual <- nrow(differences) == 0

  if(all(rowsEqual == TRUE, totalsEqual == TRUE, datasetsEqual == TRUE)){
    print("Hooray!")
  }
}