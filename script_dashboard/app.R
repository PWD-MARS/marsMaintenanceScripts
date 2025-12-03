# SET UP
#0.0: load libraries --------------
#shiny
library(shiny)
#pool for database connections
library(pool)
#odbc for database connections
library(odbc)
#tidyverse for data manipulations
library(tidyverse)
#shinythemes for colors
library(shinythemes)
#lubridate to work with dates
library(lubridate)
#shinyjs() to use easy java script functions
library(shinyjs)
#DT for datatables
library(DT)
#reactable for reactable tables
library(reactable)
#DBI FOR writing to DB
library(DBI)

#0.1: database connection and global options --------

#set default page length for datatables
options(DT.options = list(pageLength = 15))
version = '1.2.6'

#set db connection
#using a pool connection so separate connnections are unified
#gets environmental variables saved in local or pwdrstudio environment
poolConn <- tryCatch({
  dbPool(
    drv = RPostgres::Postgres(),
    host = "PWDMARSDBS1",
    port = 5434,
    dbname = "mars_prod",
    user= Sys.getenv("admin_uid"),
    password = Sys.getenv("admin_pwd"),
    timezone = NULL)},
  error = function(e){e})

#disconnect from db on stop 
onStop(function(){
 poolClose(poolConn)
})


# Define UI
ui <- navbarPage(paste("Script Dashboard", version), theme = shinytheme("slate"),
    
    tabPanel("Condensed View",
      DTOutput("logs")),
    tabPanel("Write Log View",
             DTOutput("writes")),
    tabPanel("Documentation", value = "readme",
      titlePanel("MARS Maintenance Script Dashboard v 1.1.0"),
      column(width = 5,
        h2("Scripts"),
        h5("(In order of execution"),
        
        h3("DB Backup"),
        h5("Use pg_dump to archive our database to a folder on \\pwdoows. Use pg_restore to restore a test copy of that archive. Prune backups older than 1 month, saving one monthly copy."),
        a(href="https://github.com/taywater/marsMaintenanceScripts/tree/master/01_database_backup", "Github Link"),
        
        h3("Access DBs"),
        h5("Crawl the MARS main folder for Access DBs in site folders and register them to be checked for monitoring data."),
        a(href="https://github.com/taywater/marsMaintenanceScripts/tree/master/02_update_accessdb_tables", "Github Link"),
        
        h3("OW Data"),
        h5("Sequentially query site folder Access DBs for new monitoring data and add it to our database."),
        a(href="https://github.com/taywater/marsMaintenanceScripts/tree/master/03_update_ow_data", "Github Link"),
        
        h3("Baro Data"),
        h5("Check designated baro site folders for new baro files and import them into our database."),
        a(href="https://github.com/taywater/marsMaintenanceScripts/tree/master/04_update_baro_tables", "Github Link"),
        
        h3("Rainfall Data"),
        h5("Query CentralDB for new rainfall data, and process that data into events. Write the events and data to our database."),
        a(href="https://github.com/taywater/marsMaintenanceScripts/tree/master/05_update_rainfall_tables", "Github Link"),
        
        h3("Dataconv Scrape"),
        h5("Import several tables from the DataConv GIS database."),
        a(href="https://github.com/taywater/marsMaintenanceScripts/tree/master/06_update_dataconv_tables", "Github Link"),
        
        h3("GreenIT Scrape"),
        h5("Import several tables from GreenIT."),
        a(href="https://github.com/taywater/marsMaintenanceScripts/tree/master/07_update_greenit_tables", "Github Link"),
        
        h3("CIPIT Scrape"),
        h5("Import several tables from CIPIT."),
        a(href="https://github.com/taywater/marsMaintenanceScripts/tree/master/08_update_cipit_tables", "Github Link"),
        
        h3("SMP Metadata"),
        h5("Download some spatial data from DataConv and use it to update each SMP's location, nearest rain gage, and radar grid cell in our database."),
        a(href="https://github.com/taywater/marsMaintenanceScripts/tree/master/09_update_smp_tables", "Github Link"),
        
        h3("WIC Data"),
        h5("Query Cityworks for WIC records, perform GIS operations to locate them relative to SMPs, and update these records in our database."),
        a(href="https://github.com/taywater/marsMaintenanceScripts/tree/master/10_update_wic_tables", "Github Link"),
        
        h3("Plan Review Scrape"),
        h5("Import several tables from the Plan Review DB."),
        a(href="https://github.com/taywater/marsMaintenanceScripts/tree/master/11_update_planreview_tables", "Github Link"),

        h3("Deployment Metadata"),
        h5("Reconcile HOBO deployment times with construction milestones to establish construction phases of monitoring operations."),
        a(href="https://github.com/taywater/marsMaintenanceScripts/tree/master/12_update_deployment_metadata", "Github Link"),

        h2("Error Alerts"),
        
        h3("Orange: Error"),
        h5("The script halted mid-execution, or failed at a defined break point. Check the table message, and the server logs."),

        h3("Red: Script Not Run"),
        h5("The script has not executed between yesterday and today. A scheduled task might be configured wrong, or the server might be down. Log into the server and try to execute the scheduled task in Task Scheduler.")

        
      ),
      column(width = 5,
        h2("Release Notes"),
        h3("V1.0.0"),
        h5("Initial release"),
        
        h3("v1.1.0"),
        h5("Added documentation page."),
        
        h3("v1.1.5"),
        h5("Documentation for error colors and script 12."),
        
        h3("v1.2"),
        h5("Added write log section."),
        
        h3("v1.2.5"),
        h5("Dark mode completed.")
        
        
        
      )
    )
        
)

# Server logic
server <- function(input, output) {
  print("Querying DB. This should only happen once.")
  
  script_table <- dbGetQuery(poolConn, "select * from log.viw_script_dashboard") %>%
    transmute(Script = script, `Task Order` = as.numeric(task_order), Location = location,`Date` = date, `Exit Code` = coalesce(status, 0), Note = note) %>%
    arrange(`Task Order`) %>%
    select(-`Task Order`)
  script_names <- pull(script_table, Script) %>% unique
  
  writes_table <- dbGetQuery(poolConn, "select * from log.viw_writes_dashboard") %>%
    transmute(Script = script, `Task Order` = as.numeric(task_order), `Date` = date, `Results` = results) %>%
    arrange(`Task Order`) %>%
    select(-`Task Order`)
  script_names <- pull(script_table, Script) %>% unique
  
  date_cutoff <- lubridate::today() - days(2) #Script runs older than this indicate a non-firing script
  
  output$logs <- renderDT(
    DT::datatable(
      script_table,
      rownames = FALSE,
      style = 'bootstrap',
      options = list(
        columnDefs = list(list(className = 'dt-center', targets = c(2)))
      )
    ) %>% formatStyle(
        columns = names(script_table),
        color = 'white'
    ) %>% formatStyle(
      4, #Exit Code column
      target = 'row',
      backgroundColor = styleInterval(0, c(NA, '#CC7F50'))
    ) %>% formatStyle(
      3, #Date column,
      target = 'row',
      backgroundColor = styleInterval(date_cutoff, c('#770000', NA)),
      color = styleInterval(date_cutoff, c('white', 'black'))
    )
  ) 
  output$writes <- renderDT(
    DT::datatable(
      writes_table,
      rownames = FALSE,
      style = 'bootstrap',
      options = list(
        columnDefs = list(list(className = 'dt-center', targets = c(2)))
      )
    ) %>%
      formatStyle(
        columns = names(writes_table),
        color = 'white'
    )
  )
}

# Complete app with UI and server components
shinyApp(ui, server)
