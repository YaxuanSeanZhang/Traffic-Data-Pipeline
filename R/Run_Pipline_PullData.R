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
library(parallel)
library(pbmcapply)

# for speed testing
library(tictoc)

library(styler)

source("_pull_data.R")

source("_db_connect.R")
con <- db_connect()

# 1) Get Historical Configuration Data
if ((!dbExistsTable(con, "RTMC_CONFIG_HISTORICAL")) &
  (!dbExistsTable(con, "RTMC_CONFIG_CHANGELOG"))) {
  source("1) Get-Historical-Configuration-Data-RunOnce.R")
}

# 2) Get Historical Sensor Data
if (!dbExistsTable(con, "RTMC_15MIN")) {
  source("2) Write-Historical-SensorData-RunOnce.R")
}

# 3) Update Configuration Data Nightly
#source("3) Update-Configuration-Nightly.R")

# 4) Update Sensor Data Nightly
source("4) Update-SensorData-Nightly.R")
