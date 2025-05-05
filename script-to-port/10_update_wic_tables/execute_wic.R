library(lubridate)
library(knitr)

###Section 1: Set execution parameters
#Date string for filenames.
current_date = now()
datestring = format(current_date, "%Y%m%dT%H%M")

###Section 2: Run the R script that will generate the updates for the database
#We'll be composing a string that will be sent to the command line via a subprocess

folder = "C:/Users/mars_db/bin/01-production-scripts/prod-maintenance/10_update_wic_tables"
r_script = "update_wic_tables_V2.0.rmd"
output = paste0("logs/", datestring, "_update_wic_tables.html")

rmarkdown::render(paste0(folder, "/", r_script), output_file = paste0(folder, "/", output))