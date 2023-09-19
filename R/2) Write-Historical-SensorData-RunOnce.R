source("_pull_data.R")

source("_db_connect.R") 

con <- db_connect()

# write RTMC_15MIN file column name-----
column_definitions <- c(
  "DETECTOR_NAME VARCHAR(30) NOT NULL", 
  "START_DATETIME DATETIME2 NOT NULL",
  "START_DATE DATE",
  "VOLUME_PCT_NULL NUMERIC(4,1)", "VOLUME_SUM INT",
  "VOLUME_SUM_IMPUTE INT", 
  "OCCUPANCY_PCT_NULL NUMERIC(4,1)","OCCUPANCY_SUM INT", 
  "OCCUPANCY_SUM_IMPUTE INT",
  "SPEED NUMERIC(4,1)", 
  "NODE_NAME VARCHAR(30)", "CORRIDOR_ROUTE VARCHAR(50)" 
)

query <- sprintf(
  "CREATE TABLE %s (%s)", "RTMC_15MIN1",
  paste(column_definitions, collapse = ", ")
)

dbExecute(con, query)

#set up retrieval time range
min_search_date = "2018-01-01"
max_search_date = "2023-08-12"

need_dates <- data.frame(data_date = seq(
  from = as.Date(min_search_date),
  to = as.Date(max_search_date),
  by = "days"
))

#pull sensor data
tic()
GetSensor(need_dates)
toc()
