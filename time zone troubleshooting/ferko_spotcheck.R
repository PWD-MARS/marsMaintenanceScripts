mars <- dbPool(
  drv = RPostgres::Postgres(),
  host = "PWDMARSDBS1",
  port = 5434,
  dbname = "mars_data",
  user= Sys.getenv("shiny_uid"),
  password = Sys.getenv("shiny_pwd"),
  timezone = NULL)

ferko_rain_mars <- dbGetQuery(mars, "select * from data.tbl_gage_rain where dtime_edt > '2023-09-10 00:00:00' and dtime_edt < '2023-09-11 00:00:00' and gage_uid = 7")
ferko_level <- dbGetQuery(mars, "select * from data.tbl_ow_leveldata_raw where ow_uid = 1378 and dtime_est > '2023-09-10 00:00:00' and dtime_est < '2023-09-11 00:00:00'")

marsplot <- pwdgsi::marsCombinedPlot(event = "MARS record",
                         structure_name = "Ferko Playground 411-1-1 CS1",
                         obs_datetime = ferko_level$dtime_est,
                         obs_level_ft = ferko_level$level_ft,
                         rainfall_datetime = ferko_rain_mars$dtime_edt,
                         rainfall_in = ferko_rain_mars$rainfall_in,
                         storage_depth_ft = 10) #Just freestyling this

#Visible jog of 4-5 hours between rainfall and level data

centraldb <- dbPool(
  drv = RPostgres::Postgres(),
  host = "192.168.131.120",
  port = 5432,
  dbname = "CentralDB",
  user= Sys.getenv("central_uid"),
  password = Sys.getenv("central_pwd"),
  timezone = NULL)

ferko_rain_central <- dbGetQuery(centraldb, "select * from pwdrg.tblModelRain t where \"DateTime\" > '2023-09-10 00:00:00' and \"DateTime\" < '2023-09-11 00:00:00' and \"GaugeNo\" = 7")

hhplot <- pwdgsi::marsCombinedPlot(event = "CentralDB Record",
                                      structure_name = "Ferko Playground 411-1-1 CS1",
                                      obs_datetime = ferko_level$dtime_est,
                                      obs_level_ft = ferko_level$level_ft,
                                      rainfall_datetime = ferko_rain_central$DateTime,
                                      rainfall_in = ferko_rain_central$Rainfall,
                                      storage_depth_ft = 10) #Just freestyling this

#Plots look the same - jog is still there and same size

ferko_rain_central <- transmute(ferko_rain_central, gage_uid = GaugeNo, dtime_edt = DateTime, rainfall_in = Rainfall)
ferko_rain_mars <- select(ferko_rain_mars, -gage_rain_uid)

symdiff(ferko_rain_central, ferko_rain_mars) #Identical series!


