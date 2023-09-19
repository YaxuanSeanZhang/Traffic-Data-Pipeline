source("_pull_data.R")

source("_db_connect.R") 

con <- db_connect()

min_search_date = dbGetQuery(con,"SELECT MAX(START_DATE) from RTMC_15MIN")

need_dates <- data.frame(data_date = seq(
  from = as.Date(as.character(min_search_date)) + 1,
  to = as.Date(Sys.Date() - 3),
  by = "days"
))


#pull sensor data
tic()
GetSensor(need_dates)
toc()