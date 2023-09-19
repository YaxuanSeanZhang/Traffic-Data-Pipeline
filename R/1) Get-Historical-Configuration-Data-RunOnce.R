source("_pull_data.R")

source("_db_connect.R") 

con <- db_connect()

# write log file column name-----
column_definitions <- c(
  "Update_Date DATE", "Change VARCHAR(255)", "DETECTOR_NAME VARCHAR(255)",
  "Old_Value VARCHAR(255)", "New_Value VARCHAR(255)", "Last_Update_Date DATE"
)

query <- sprintf(
  "CREATE TABLE %s (%s)", "RTMC_CONFIG_CHANGELOG",
  paste(column_definitions, collapse = ", ")
)

dbExecute(con, query)

# store the oldest version-----
url <- "http://iris.dot.state.mn.us/metro_config/metro_config_20190731.xml.gz"
configuration_old <- GetHistoricalData(url)
configuration_old[,START_DATE:= as.Date("1900-01-01")]
configuration_old[,END_DATE := as.Date("2100-01-01")]
configuration_old[,DEACTIVATE := FALSE]

configuration_old[configuration_old == ""] <- NA
dbWriteTable(con, "RTMC_CONFIG_HISTORICAL", configuration_old, overwrite = TRUE)

#set up retrieval time range
min_search_date = as.Date("2019-08-01")
max_search_date = as.Date("2023-06-23")

days <- seq.Date(min_search_date, max_search_date, by = "1 day")

lapply(days, \(d){
  message('iteration ', d)
  url <- paste0("http://iris.dot.state.mn.us/metro_config/metro_config_"
                , d %>% format("%Y%m%d")
                , ".xml.gz")
  
  configuration_new <- tryCatch(
    {
      configuration_new <- GetHistoricalData(url)
    },
    error = function(e) {
      configuration_new <- NA
    }
  )
  
  #track change and update sql database
  Track_Config_Change(configuration_new, d)
})