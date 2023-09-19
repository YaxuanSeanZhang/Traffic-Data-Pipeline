source("_analyze_data.R")

# connection to SQL Server Database
source("_db_connect.R") # loads function db_connect()

con <- db_connect()

# write log file column name-----
column_definitions <- c(
  "NODE_NAME VARCHAR(30)", "PREDICT_TIME SMALLDATETIME",
  "VOLUMN_PREDICTION INT", "CORRIDOR_ROUTE VARCHAR(50)"
)

query <- sprintf(
  "CREATE TABLE %s (%s)", "RTMC_PREDICT_HOUR",
  paste(column_definitions, collapse = ", ")
)

dbExecute(con, query)

#Modeling------
modeling_node('hour')
