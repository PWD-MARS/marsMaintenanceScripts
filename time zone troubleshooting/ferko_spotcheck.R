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


#Visible jog of 1 hour between rainfall and level data

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

#-------------------------------------------------------- January 13th storm

#Pulling from MARS
ferko_rain_january <- dbGetQuery(mars, "select * from data.tbl_gage_rain where dtime_edt > '2024-01-12 18:00:00' and dtime_edt < '2024-01-13 06:00:00' and gage_uid = 7")
ferko_level_january <- dbGetQuery(mars, "select * from data.tbl_ow_leveldata_raw where ow_uid = 1378 and dtime_est > '2024-01-12 18:00:00' and dtime_est < '2024-01-13 06:00:00'")

januaryplot <- pwdgsi::marsCombinedPlot(event = "January record",
                                     structure_name = "Ferko Playground 411-1-1 CS1",
                                     obs_datetime = ferko_level_january$dtime_est,
                                     obs_level_ft = ferko_level_january$level_ft,
                                     rainfall_datetime = ferko_rain_january$dtime_edt,
                                     rainfall_in = ferko_rain_january$rainfall_in,
                                     storage_depth_ft = 6)  #Just freestyling this

#No jog!

#-------------------------------------------------------- March 23rd storm


#Pulling from MARS
ferko_rain_march <- dbGetQuery(mars, "select * from data.tbl_gage_rain where dtime_edt > '2024-03-23 00:00:00' and dtime_edt < '2024-03-24 00:00:00' and gage_uid = 7")
ferko_level_march <- dbGetQuery(mars, "select * from data.tbl_ow_leveldata_raw where ow_uid = 1378 and dtime_est > '2024-03-23 00:00:00' and dtime_est < '2024-03-24 00:00:00'")

marchplot <- pwdgsi::marsCombinedPlot(event = "March record",
                                        structure_name = "Ferko Playground 411-1-1 CS1",
                                        obs_datetime = ferko_level_march$dtime_est,
                                        obs_level_ft = ferko_level_march$level_ft,
                                        rainfall_datetime = ferko_rain_march$dtime_edt,
                                        rainfall_in = ferko_rain_march$rainfall_in,
                                        storage_depth_ft = 6)  #Just freestyling this


#1 hour jog

#--------------------------------------------------------- Belmont School OW1 April 2018

#Pulling from MARS
belmont_rain_april <- dbGetQuery(mars, "select * from data.tbl_gage_rain where dtime_edt > '2018-04-15 12:00:00' and dtime_edt < '2018-04-16 12:00:00' and gage_uid = 9")
belmont_level_april <- dbGetQuery(mars, "select * from data.tbl_ow_leveldata_raw where ow_uid = 779 and dtime_est > '2018-04-15 12:00:00' and dtime_est < '2018-04-16 12:00:00'")

aprilplot <- pwdgsi::marsCombinedPlot(event = "April record",
                                      structure_name = "Belmont School OW1",
                                      obs_datetime = belmont_level_april$dtime_est,
                                      obs_level_ft = belmont_level_april$level_ft,
                                      rainfall_datetime = belmont_rain_april$dtime_edt,
                                      rainfall_in = belmont_rain_april$rainfall_in,
                                      storage_depth_ft = 6)  #Just freestyling this


#1 hour jog


#--------------------------------------------------------- Belmont School OW1 January 2024

#Pulling from H&H
belmont_rain_january <- dbGetQuery(centraldb, "select * from pwdrg.tblModelRain t where \"DateTime\" > '2024-01-09 08:00:00' and \"DateTime\" < '2024-01-10 12:00:00' and \"GaugeNo\" = 9")
belmont_rain_january <- transmute(belmont_rain_january, gage_uid = GaugeNo, dtime_edt = DateTime, rainfall_in = Rainfall)

belmont_level_january <- dbGetQuery(mars, "select * from data.tbl_ow_leveldata_raw where ow_uid = 779 and dtime_est > '2024-01-09 08:00:00' and dtime_est < '2024-01-10 12:00:00'")

januaryplot <- pwdgsi::marsCombinedPlot(event = "January record",
                                      structure_name = "Belmont School OW1",
                                      obs_datetime = belmont_level_january$dtime_est,
                                      obs_level_ft = belmont_level_january$level_ft,
                                      rainfall_datetime = belmont_rain_january$dtime_edt,
                                      rainfall_in = belmont_rain_january$rainfall_in,
                                      storage_depth_ft = 6)  #Just freestyling this


#No jog

#--------------------------------------------------------- Belmont School OW1 February 2016

#Pulling from H&H
belmont_rain_february <- dbGetQuery(centraldb, "select * from pwdrg.tblModelRain t where \"DateTime\" > '2016-02-24 12:00:00' and \"DateTime\" < '2016-02-25 12:00:00' and \"GaugeNo\" = 9")
belmont_rain_february <- transmute(belmont_rain_february, gage_uid = GaugeNo, dtime_edt = DateTime, rainfall_in = Rainfall)

belmont_level_february <- dbGetQuery(mars, "select * from data.tbl_ow_leveldata_raw where ow_uid = 779 and dtime_est > '2016-02-24 12:00:00' and dtime_est < '2016-02-25 12:00:00'")

februaryplot <- pwdgsi::marsCombinedPlot(event = "February record",
                                        structure_name = "Belmont School OW1",
                                        obs_datetime = belmont_level_february$dtime_est,
                                        obs_level_ft = belmont_level_february$level_ft,
                                        rainfall_datetime = belmont_rain_february$dtime_edt,
                                        rainfall_in = belmont_rain_february$rainfall_in,
                                        storage_depth_ft = 6)  #Just freestyling this


#No jog


