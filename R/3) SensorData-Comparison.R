source("_analyze_data.R")

# connection to SQL Server Database
source("_db_connect.R") # loads function db_connect()

con <- db_connect()

min_date = as.Date("2020-03-01")
max_date = as.Date(Sys.Date() - 3)

hourly_diff = data_comparison(min_date,max_date,'hour')

daily_diff = data_comparison(min_date,max_date,'day')
