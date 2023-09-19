library(tc.sensors)

# Data manipulation:
library(tidyr)
library(dplyr)
library(lubridate)
library(data.table)

# Database packages:
library(DBI)
library(odbc)

# visualization tools:
library(leaflet)
library(geosphere)

# Parallel processing packages:
library(future)
library(furrr)
# helpful package for running processes in parallel (using more cores = faster!)

# for speed testing
library(tictoc)

library(styler)

#gam modeling
library(mgcv)

source("_analyze_data.R")

source("_db_connect.R")
con <- db_connect()

# 1) Hourly Prediction
if (!dbExistsTable(con, "RTMC_PREDICT_HOUR")) {
  source("1) SensorData-Modeling-Hourly-RunOnce.R")
}

# 2) Daily Prediction
if (!dbExistsTable(con, "RTMC_PREDICT_DAY")) {
  source("2) SensorData-Modeling-Daily-RunOnce.R")
}

# 3) Get difference between actual and prediction
#source("3) SensorData-Comparison.R")