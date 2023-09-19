# aggregate from 15min data to hourly/daily level and node level
# aggregate <- function(sensor_data, time_level) {
#   dat_node <- sensor_data[
#     , .(
#       VOLUME_SUM_IMPUTE = sum(VOLUME_SUM_IMPUTE, na.rm = T),
#       OCCUPANCY_SUM_IMPUTE = sum(OCCUPANCY_SUM_IMPUTE, na.rm = T),
#       VOLUME_PCT_NULL = mean(VOLUME_PCT_NULL, na.rm = T),
#       OCCUPANCY_PCT_NULL = mean(OCCUPANCY_PCT_NULL, na.rm = T),
#       SPEED = mean(SPEED, na.rm = T)
#     ),
#     keyby = .(
#       DETECTOR_NAME,
#       NODE_NAME,
#       START_DATETIME = as_datetime(START_DATETIME) %>%
#         floor_date(unit = time_level),
#       CORRIDOR_ROUTE
#     )
#   ][
#     , .(
#       VOLUME_SUM_IMPUTE = VOLUME_SUM_IMPUTE %>% sum(na.rm = T),
#       VOLUME_NUM = VOLUME_SUM_IMPUTE %>% na.omit() %>% length(),
#       OCCUPANCY_SUM_IMPUTE = OCCUPANCY_SUM_IMPUTE %>% sum(na.rm = T),
#       OCCUPANCY_NUM = OCCUPANCY_SUM_IMPUTE %>% na.omit() %>% length(),
#       SPEED = SPEED %>% mean(na.rm = T),
#       SPEED_NUM = SPEED %>% na.omit() %>% length()
#     ),
#     keyby = .(NODE_NAME, START_DATETIME, CORRIDOR_ROUTE)
#   ]
#   return(dat_node)
# }

# time_level = 'hour' or 'day'

# qaqc for sensor data (2018 - 2019)
data_qaqc_formodel <- function(sensor_nodeset, time_level = "hour") {
  time_mapping <- c("hour" = 1, "day" = 24)

  ## deal with date
  sensor_nodeset[, c("idate", "itime") := IDateTime(START_DATETIME)]
  sensor_nodeset[, c("itime") := as.ITime(itime, ms = "truncate")]
  sensor_nodeset[, weekday := factor(
    weekdays(idate, abbr = T),
    levels = c("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")
  )]

  sensor_nodeset[, year := year(idate)]
  sensor_nodeset[, yday := yday(idate)]


  ## test 1: correct number of detectors/volume values at this node:
  sensor_nodeset <- sensor_nodeset[VOLUME_NUM == DETECTOR_NUM]

  ## test 2: at least 100 rows, median volume > 40 cars/hour ----
  sensor_nodeset[, count := .N, by = .(NODE_NAME)]
  sensor_nodeset[, median_volume := median(VOLUME_SUM_IMPUTE),
    by = .(NODE_NAME)
  ]

  sensor_nodeset <-
    sensor_nodeset[count > 100 & median_volume >= 40 * time_mapping[time_level]]

  ## test 3: 75% complete in 2019 and 2018 -----
  # 75% complete data in 2019 and 2018
  sensor_nodeset[, pct_complete := .N / (365 * 24 / time_mapping[time_level]),
    by = .(NODE_NAME, year)
  ]
  complete_nodes <-
    sensor_nodeset[, min(pct_complete),
      by = .(NODE_NAME)
    ][V1 >= 0.75]$NODE_NAME

  sensor_nodeset <-
    sensor_nodeset[NODE_NAME %in% complete_nodes]

  return(sensor_nodeset)
}

# qaqc for sensor data (2020 - now)
data_qaqc <- function(sensor_nodeset, time_level = "hour") {
  time_mapping <- c("hour" = 1, "day" = 24)

  ## test 1: correct number of detectors/volume values at this node:
  sensor_nodeset <- sensor_nodeset[VOLUME_NUM == DETECTOR_NUM]

  ## test 2: at least 100 rows, median volume > 40 cars/hour ----
  sensor_nodeset[, count := .N, by = .(NODE_NAME)]
  sensor_nodeset[, median_volume := median(VOLUME_SUM_IMPUTE),
    by = .(NODE_NAME)
  ]

  sensor_nodeset <-
    sensor_nodeset[count > 100 & median_volume >= 40 * time_mapping[time_level]]

  return(sensor_nodeset)
}

# data modeling
modeling_node <- function(time_level = "hour") {
  # read configuration file and get node & corridor info
  configuration <- dbGetQuery(con, "SELECT * from RTMC_CONFIG_HISTORICAL") %>%
    as.data.table()

  # get node info and activated date of each detector
  config_node <-
    configuration[
      , .(DETECTOR_NUM = DETECTOR_NAME %>% na.omit() %>% length()),
      keyby = .(NODE_NAME,
        CORRIDOR_ROUTE,
        START_DATE = as.Date(START_DATE),
        END_DATE = as.Date(END_DATE)
      )
    ]

  # read 15 min sensor data (2018-2019)
  # sensor_data <- dbGetQuery(
  #   con, "SELECT * from RTMC_15MIN1
  #   WHERE DATEPART(YEAR, START_DATE) IN (2023)"
  # ) %>%
  #   as.data.table()

  query = paste0("SELECT
  NODE_NAME,
  START_DATETIME,
  CORRIDOR_ROUTE,
  SUM(VOLUME_SUM_IMPUTE) AS VOLUME_SUM_IMPUTE,
  COUNT(VOLUME_SUM_IMPUTE) AS VOLUME_NUM,
  SUM(OCCUPANCY_SUM_IMPUTE) AS OCCUPANCY_SUM_IMPUTE,
  COUNT(OCCUPANCY_SUM_IMPUTE) AS OCCUPANCY_NUM,
  AVG(SPEED) AS SPEED,
  COUNT(SPEED) AS SPEED_NUM
  FROM (
  SELECT
  DETECTOR_NAME,
  NODE_NAME,
  DATEADD(",time_level,", 
  DATEDIFF(",time_level, ", 0, START_DATETIME), 0) AS START_DATETIME,
  CORRIDOR_ROUTE,
  SUM(VOLUME_SUM_IMPUTE) AS VOLUME_SUM_IMPUTE,
  SUM(OCCUPANCY_SUM_IMPUTE) AS OCCUPANCY_SUM_IMPUTE,
  AVG(SPEED) AS SPEED
  FROM RTMC_15MIN
  WHERE DATEPART(YEAR, START_DATE) IN (2018,2019)
  GROUP BY
  DETECTOR_NAME,
  NODE_NAME,
  CORRIDOR_ROUTE,
  DATEADD(",time_level,", DATEDIFF(",time_level, ", 0, START_DATETIME), 0)
  ) AGGREAGATE_TIME
  GROUP BY
  NODE_NAME,
  START_DATETIME,
  CORRIDOR_ROUTE")
  
  dat_node <- dbGetQuery(con, query) %>% as.data.table()
  
  # calculate the number of active detectors within the node
  dat_node <- dat_node[
    , DATE := as.Date(START_DATETIME)
  ][
    config_node,
    on = .(NODE_NAME, DATE >= START_DATE, DATE <= END_DATE),
    nomatch = NULL
  ][
    , .(DETECTOR_NUM = DETECTOR_NUM %>% sum(na.rm = T)),
    keyby = .(
      NODE_NAME, START_DATETIME,
      VOLUME_SUM_IMPUTE, VOLUME_NUM,
      OCCUPANCY_SUM_IMPUTE, OCCUPANCY_NUM,
      SPEED, SPEED_NUM, CORRIDOR_ROUTE
    )
  ]

  # qaqc for modeling
  dat_node <- data_qaqc_formodel(dat_node, time_level)

  # parallel processing
  config_node <- unique(configuration[, .(NODE_NAME, CORRIDOR_ROUTE)])
  config_node[
    , thirty := rep(1:1000, each = 30, length.out = nrow(config_node))
  ]

  config_30 <- split(config_node, f = config_node$thirty)

   # prediction modeling
  lapply(config_30, \(a_node_set){
    dat_nodeset <- dat_node[NODE_NAME %in% a_node_set$NODE_NAME]

    ### Parallel process across nodes in a set of nodes ----
    if (nrow(dat_nodeset) > 0) {
      dat_nodeset <- base::split(dat_nodeset,
        f = dat_nodeset$NODE_NAME
      )

      message(paste0("Starting models!"))

      tictoc::tic()

      predictions_nodeset <-
        pbmcapply::pbmclapply(
          names(dat_nodeset),
          function(a_node) {
            dat_onenode <- dat_nodeset[[a_node]]

            # hourly gam or daily gam----

            if (time_level == "hour") {
              this_gam <- gam(
                data = dat_onenode,
                VOLUME_SUM_IMPUTE ~
                  s(itime, by = weekday, bs = "cs") # time of day
                  + s(yday, bs = "cs") # day of year
                  + weekday,
                family = nb()
                # negative binomial family for low-count data (non-zero)
              )
            } else if (time_level == "day") {
              this_gam <- gam(
                data = dat_onenode,
                VOLUME_SUM_IMPUTE ~
                  s(yday, bs = "cs", k = 12)
                  + weekday,
                family = nb()
                # negative binomial family for low-count data (non-zero)
              )
            }

            datetime_range <-
              c(seq.POSIXt(
                as.POSIXct("2020-01-01 00:00:00", tz = "UTC"),
                as.POSIXct("2030-12-31 00:00:00", tz = "UTC"),
                by = time_level
              ))

            # empty dataset of predictions ----
            predict_dat <- data.table(
              datetime = datetime_range,
              IDateTime(datetime_range)
            )

            predict_dat[, weekday := lubridate::wday(idate, abbr = T, label = T)]
            predict_dat[, yday := yday(idate)]
            predict_dat[, hour := hour(itime)]
            predict_dat[, NODE_NAME := dat_onenode$NODE_NAME[[1]]]
            predict_dat[, CORRIDOR_ROUTE := dat_onenode$CORRIDOR_ROUTE[[1]]]

            # generate predictions ----
            predict_dat[, VOLUMN_PREDICTION :=
              round(
                mgcv::predict.gam(
                  object = this_gam,
                  newdata = predict_dat,
                  se.fit = F,
                  type = "response"
                )
              )]

            predict_dat <-
              predict_dat[, .(
                NODE_NAME, datetime, VOLUMN_PREDICTION,
                CORRIDOR_ROUTE
              )]

            setnames(
              predict_dat,
              old = c("datetime"),
              new = c("PREDICT_TIME")
            )

            predict_dat[, PREDICT_TIME :=
              as.POSIXct(PREDICT_TIME, tz = "UTC")]

            predict_dat <-
              predict_dat[!is.na(PREDICT_TIME)]

            # return value:
            predict_dat
          },
          mc.cores = 5
        ) %>% rbindlist() # end parallel process for nodes within a set of nodes

      # Speed test result - models:
      tictoc::toc()

      # write to sql table ----
      msg <-
        paste0(
          "Writing ",
          prettyNum(nrow(predictions_nodeset), big.mark = ","),
          " rows to sql table"
        )
      message(msg)

      tictoc::tic()

      DBI::dbWriteTable(
        con,
        paste0("RTMC_PREDICT_", toupper(time_level)),
        predictions_nodeset,
        append = T
      )

      tictoc::toc()
    } # end check for data to model
  }) # end for loop over node set
}

# compare actual vs prediction
data_comparison <- function(min_date, max_date, time_level = "hour") {
  # read configuration file and get node & corridor info
  configuration <- dbGetQuery(con, "SELECT * from RTMC_CONFIG_HISTORICAL") %>%
    as.data.table()

  config_node <-
    configuration[
      , .(DETECTOR_NUM = DETECTOR_NAME %>% na.omit() %>% length()),
      keyby = .(
        NODE_NAME,
        CORRIDOR_ROUTE,
        as.Date(START_DATE),
        as.Date(END_DATE)
      )
    ]

  # read 15 min sensor data (2020-now)
  sensor_data <- dbGetQuery(
    con, "SELECT * from RTMC_15MIN
    WHERE START_DATE >= ? AND START_DATE <= ?",
    params = list(min_date, max_date)
  ) %>%
    as.data.table()

  # aggregation sensor data to node level (actual volume)
  dat_node <- sensor_data %>% aggregate(time_level)

  # calculate the number of active detectors within the node
  dat_node <- dat_node[
    , DATE := as.Date(START_DATETIME)
  ][
    config_node,
    on = .(NODE_NAME, DATE >= START_DATE, DATE <= END_DATE),
    nomatch = NULL
  ][
    , .(DETECTOR_NUM = DETECTOR_NUM %>% sum(na.rm = T)),
    keyby = .(
      NODE_NAME, START_DATETIME,
      VOLUME_SUM_IMPUTE, VOLUME_NUM,
      OCCUPANCY_SUM_IMPUTE, OCCUPANCY_NUM,
      SPEED, SPEED_NUM, CORRIDOR_ROUTE
    )
  ]

  # qaqc
  dat_node <- data_qaqc(dat_node, time_level)

  # read node-level prediction results  (predict volume)
  predict_data <- dbGetQuery(
    con,
    paste0(
      "SELECT * from RTMC_PREDICT_",
      toupper(time_level),
      " WHERE CAST(PREDICT_TIME AS DATE) >= ? AND
             CAST(PREDICT_TIME AS DATE) <= ?"
    ),
    params = list(min_date, max_date)
  ) %>%
    as.data.table()

  # compare difference between actual and predict volume
  dat_node_diff <- dat_node[
    predict_data,
    on = c(
      "NODE_NAME" = "NODE_NAME",
      "START_DATETIME" = "PREDICT_TIME"
    ),
    nomatch = NULL
  ][
    , .(
      NODE_NAME, START_DATETIME,
      VOLUME_SUM_IMPUTE, VOLUMN_PREDICTION,
      CORRIDOR_ROUTE
    )
  ][
    , VOLUME_DIFF := VOLUME_SUM_IMPUTE - VOLUMN_PREDICTION
  ]

  return(dat_node_diff)
}
