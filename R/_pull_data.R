source("_db_connect.R")
con <- db_connect()

# get data from xml url and convert to data.table
GetHistoricalData <- function(url) {
  tmp <- tempfile()
  utils::download.file(url, tmp, quiet = TRUE)
  metro_config <- xml2::read_xml(gzfile(tmp))

  # ------------------
  # PATHS
  # ------------------

  # Detector paths - connect rnodes and corridors to this
  detector_paths <- tibble::enframe(
    xml2::xml_path(
      xml2::xml_find_all(metro_config, "//detector")
    )
  ) %>%
    dplyr::mutate(detector_path = value) %>%
    tidyr::separate(
      detector_path,
      into = c(
        "front", "tms_config", "device",
        "rnode", "detector"
      ),
      sep = "/"
    ) %>%
    tidyr::unite(
      rnode_path, front, tms_config, device,
      rnode,
      sep = "/"
    ) %>%
    dplyr::mutate(rnode_path = trimws(rnode_path)) %>%
    dplyr::mutate(corridor_path = rnode_path) %>%
    tidyr::separate(
      corridor_path,
      into = c(
        "front", "tms_config", "device",
        "rnode"
      ),
      sep = "/"
    ) %>%
    tidyr::unite(
      corridor_path, front, tms_config,
      device,
      sep = "/"
    ) %>%
    dplyr::mutate(corridor_path = trimws(corridor_path)) %>%
    dplyr::select(-name) %>%
    dplyr::rename(detector_path = value)

  # Rnode paths
  rnode_paths <- tibble::enframe(
    xml2::xml_path(
      xml2::xml_find_all(metro_config, "//r_node")
    )
  ) %>%
    dplyr::transmute(rnode_path = value)

  # Corridor paths
  corridor_paths <- tibble::enframe(
    xml2::xml_path(
      xml2::xml_find_all(metro_config, "//corridor")
    )
  ) %>%
    dplyr::transmute(corridor_path = value)

  # ------------------
  # ATTRIBUTES (rnodes & detectors)
  # ------------------


  d_attr_ls <- list(
    "name", "label", "category", "lane", "field",
    "abandoned"
  )

  rn_attr_ls <- list(
    "name", "n_type", "transition", "label",
    "lon", "lat", "lanes", "shift", "s_limit",
    "station_id", "attach_side"
  )

  c_attr_ls <- list("route", "dir")


  attr_all_ls <- list(d_attr_ls, rn_attr_ls, c_attr_ls)
  categories <- list("detector", "r_node", "corridor")

  attributes_full <- purrr::map2(
    categories, attr_all_ls, attr_to_df, metro_config
  )

  # Bind paths to attributes
  d_paths_attr <- dplyr::bind_cols(detector_paths, attributes_full[[1]])
  rnode_paths_attr <- dplyr::bind_cols(rnode_paths, attributes_full[[2]])
  corr_paths_attrs <- dplyr::bind_cols(corridor_paths, attributes_full[[3]])

  detector_rnodes_full <- dplyr::left_join(
    d_paths_attr, rnode_paths_attr,
    by = c("rnode_path")
  )
  configuration <- dplyr::left_join(
    detector_rnodes_full, corr_paths_attrs,
    by = c("corridor_path")
  )

  config_tidy <- configuration %>%
    dplyr::select(
      -rnode, -rnode_path, -detector, -detector_path,
      -corridor_path
    ) %>%
    # extract date from url information
    dplyr::mutate(
      last_change_date = as.Date(
        sub(".*config_", "", sub(".xml.*", "", url)),
        "%Y%m%d"
      )
    )


  config_tidy <- config_tidy %>%
    as.data.table() %>%
    rename(
      NODE_NAME = r_node_name, NODE_N_TYPE = r_node_n_type,
      NODE_TRANSITION = r_node_transition,
      NODE_LABEL = r_node_label, NODE_LON = r_node_lon,
      NODE_LAT = r_node_lat, NODE_LANES = r_node_lanes,
      NODE_SHIFT = r_node_shift, NODE_S_LIMIT = r_node_s_limit,
      NODE_STATION_ID = r_node_station_id,
      NODE_ATTACH_SIDE = r_node_attach_side
    ) %>%
    rename_all(toupper)

  return(config_tidy)
}

# get configuration data
Track_Config_Change <- function(configuration_new, day) {
  if (!is.null(nrow(configuration_new))) {
    configuration_new[configuration_new == ""] <- NA

    configuration_old <- dbReadTable(con, "RTMC_CONFIG_HISTORICAL") %>%
      as.data.table()

    # Compare new and old configuration file--------------
    #        1) New detector Added-----
    new_detector <- configuration_new[!DETECTOR_NAME %in%
      configuration_old$DETECTOR_NAME]

    if (nrow(new_detector) > 0) {
      new_detector[, START_DATE := day]
      new_detector[, END_DATE := as.Date("2100-01-01")]
      new_detector[, DEACTIVATE := FALSE]

      # append to SQL table
      dbWriteTable(
        con, "RTMC_CONFIG_HISTORICAL",
        new_detector,
        overwrite = FALSE,
        append = TRUE
      )

      # write log file
      log <- data.frame(
        Update_Date = day, Change = "New Detector Added",
        DETECTOR_NAME = new_detector[, DETECTOR_NAME],
        Old_Value = NA,
        New_Value = new_detector[, DETECTOR_NAME],
        Last_Update_Date = NA
      )
      dbWriteTable(
        con,
        "RTMC_CONFIG_CHANGELOG",
        log,
        overwrite = FALSE, append = TRUE
      )
    }

    #        2) Detector Info Removed-----
    delete_detector <- configuration_old[!DETECTOR_NAME %in%
      configuration_new$DETECTOR_NAME]
    delete_detector <- delete_detector[DEACTIVATE != TRUE]

    if (nrow(delete_detector) > 0) {
      # update the end_date
      update_query <- paste(
        "UPDATE RTMC_CONFIG_HISTORICAL SET END_DATE = ?, DEACTIVATE = ?
        WHERE DETECTOR_NAME = ?"
      )
      dbExecute(
        con, update_query,
        params = list(
          rep(day, nrow(delete_detector)),
          rep(TRUE, nrow(delete_detector)),
          delete_detector$DETECTOR_NAME
        )
      )

      # write log file
      log <- data.frame(
        Update_Date = day, Change = "Detector Removed",
        DETECTOR_NAME = delete_detector[, DETECTOR_NAME],
        Old_Value = delete_detector[, DETECTOR_NAME],
        New_Value = NA,
        Last_Update_Date = delete_detector[, LAST_CHANGE_DATE]
      )
      dbWriteTable(
        con,
        "RTMC_CONFIG_CHANGELOG",
        log,
        overwrite = FALSE, append = TRUE
      )
    }


    #        3) Attribute Changed -----
    # Compare the columns and return the subset with any differences
    joined_table <- merge(
      configuration_old, configuration_new,
      by = "DETECTOR_NAME", all = F, suffixes = c("", ".new")
    )
    cols_to_compare <- setdiff(
      names(configuration_old),
      c(
        "DETECTOR_NAME", "LAST_CHANGE_DATE",
        "START_DATE", "END_DATE", "DEACTIVATE"
      )
    )
    cols_to_compare_new <- paste0(cols_to_compare, ".new")

    different_rows <- joined_table[apply(
      joined_table[, ..cols_to_compare] !=
        joined_table[, ..cols_to_compare_new],
      1, any
    )]

    if (nrow(different_rows) > 0) {
      for (i in 1:nrow(different_rows)) {
        different_columns <- setdiff(
          cols_to_compare, cols_to_compare[mapply(
            identical,
            different_rows[i, ..cols_to_compare],
            different_rows[i, ..cols_to_compare_new]
          )]
        )
        for (j in 1:length(different_columns)) {
          # # Define the update query
          if (different_columns[j] == "DETECTOR_ABANDONED") {
            # abandon status from FALSE to TRUE:
            # update END_DATE and DEACTIVATE status
            if (different_rows[i, DETECTOR_ABANDONED] == "f") {
              update_query <- paste(
                "UPDATE RTMC_CONFIG_HISTORICAL SET",
                different_columns[j],
                " = ?, LAST_CHANGE_DATE = ? , END_DATE = ?, DEACTIVATE = ?
                WHERE DETECTOR_NAME = ?"
              )
              dbExecute(
                con, update_query,
                params = list(
                  different_rows[i, get(paste0(different_columns[j], ".new"))],
                  day, day, TRUE, different_rows[i, DETECTOR_NAME]
                )
              )
            } else {
              # abandon status from TRUE to FALSE:
              # unreasonable cases! deal with it carefully! mistakes from MnDOT
              update_query <- paste(
                "UPDATE RTMC_CONFIG_HISTORICAL SET",
                different_columns[j],
                " = ?, LAST_CHANGE_DATE = ? , END_DATE = ?, DEACTIVATE = ?
                WHERE DETECTOR_NAME = ?"
              )
              dbExecute(
                con, update_query,
                params = list(
                  different_rows[i, get(paste0(different_columns[j], ".new"))],
                  day, as.Date("2019-07-31"),
                  TRUE, different_rows[i, DETECTOR_NAME]
                )
              )
            }
          } else {
            # Define the update query
            update_query <- paste(
              "UPDATE RTMC_CONFIG_HISTORICAL SET",
              different_columns[j], " = ?, LAST_CHANGE_DATE = ?
              WHERE DETECTOR_NAME = ?"
            )
            dbExecute(
              con, update_query,
              params = list(
                different_rows[i, get(paste0(different_columns[j], ".new"))],
                day, different_rows[i, DETECTOR_NAME]
              )
            )
          }

          # write log file
          log <- data.frame(
            Update_Date = day,
            Change = paste("Attribute Changed:", different_columns[j]),
            DETECTOR_NAME = different_rows[i, DETECTOR_NAME],
            Old_Value = different_rows[i, get(different_columns[j])],
            New_Value = different_rows[
              i, get(paste0(different_columns[j], ".new"))
            ],
            Last_Update_Date = different_rows[i, LAST_CHANGE_DATE]
          )
          dbWriteTable(
            con,
            "RTMC_CONFIG_CHANGELOG",
            log,
            overwrite = FALSE, append = TRUE
          )
        }
      }
    }
  }
}

aggregate_detector <- function(sensor_data, config) {
  config <- config[detector_name == sensor_data$sensor[[1]]]
  interval_scans <- 0.25 * 216000 # interval_length = 0.25
  field_length <- as.numeric(config[, "detector_field"][[1]])

  sensor_data <- tc.sensors::replace_impossible(
    sensor_data = sensor_data,
    interval_length = NA
  )

  sensor_data[, start_datetime := date + hours(hour) + seconds(60 * min)]

  sensor_data_agg <-
    sensor_data[,
      .(
        volume.sum = round(sum(volume, na.rm = T)),
        occupancy.sum = round(sum(occupancy, na.rm = T)),
        volume.pct.null = round(100 * sum(is.na(volume)) / length(volume), 1),
        occupancy.pct.null = round(100 * sum(is.na(occupancy)) / length(occupancy), 1)
      ),
      keyby = .(
        sensor,
        start_datetime %>% floor_date(unit = "15 minutes")
      )
    ][
      ,
      `:=`(
        occupancy.pct,
        (occupancy.sum / interval_scans)
      )
    ][
      ,
      `:=`(
        speed,
        ifelse(volume.sum != 0 &
          occupancy.pct >= 0.002, # occupancy_pct_threshold = 0.002
        (volume.sum * (60 / 30) * field_length) / (5280 * occupancy.pct),
        NA
        )
      )
    ]
  return(sensor_data_agg)
}

GetSensor <- function(need_dates) {
  #   1) get parameter prepared------
  # get the need_day (date & detector & node)
  configuration <- dbReadTable(con, "RTMC_CONFIG_HISTORICAL") %>%
    as.data.table()

  setnames(configuration, tolower)

  # Add additional filters here if desired, for example
  # A sample of 10% of sensors (good for testing code) would be:
  need_sensors <- configuration[, .(
    detector_name, node_name, start_date,
    end_date
  )]

  need_data <- merge(need_dates, need_sensors) %>%
    as.data.table() %>%
    .[data_date >= start_date & data_date <=
      end_date]

  ## Split 'need_data' by day
  # To speed up batch upload,
  # we iterate over a days' worth of data at a time in a for-loop.
  # Our 'need_data' data.frame is broken into a list, by date.
  need_data_day <- split(need_data, by = "data_date")

  #   2) download data from RTMC trafdat server and write in the sql table-----
  # (parallel processing: pbmclaaply)

  ## Open for loop to iterate over each week
  lapply(need_data_day, \(a_day){
    ## Start the timer
    tictoc::tic()

    day <- a_day$data_date[1]

    ## Subset need_data to this week
    message(paste0("Downloading data for ", day))

    ## Download a day's worth of data----
    data_day_test <- ### Parallel process across sensors
      pbmcapply::pbmclapply(
        a_day$detector_name,
        # c('10','100','1000','5474','8680'),
        function(a_detector) {
          # get cleaned 15 min level data
          data_day <- tc.sensors::pull_sensor(
            sensor = a_detector,
            pull_date = day
          ) %>% as.data.table()

          # if we keep parallel requesting data from api, api may fail to return data
          # in this case, it will return 2880 rows NA value
          # to avoid this, we will try at most 5 more times to make sure we successfully
          # request data from api

          try <- 1

          while (data_day[, (is.na(volume) %>% sum() == 2880) |
            (is.na(occupancy) %>% sum() == 2880)] & try < 5) {
            data_day <- tc.sensors::pull_sensor(
              sensor = a_detector,
              pull_date = day
            )
            Sys.sleep(0.01)
            try <- try + 1
          }

          data_day <- data_day %>% tc.sensors::scrub_sensor() %>%
            # rolling up to 15 minute level:
            aggregate_detector(config = configuration)

          data_day <- data_day[(volume.pct.null < 100) |
            (occupancy.pct.null < 100)]

          ##### If there's no data for this-sensor day
          if (nrow(data_day) == 0) {
            ##### do nothing ----
            data_sensor_day_clean <- NULL
          } else {
            #### otherwise, clean the data
            # get rid of bad rows:
            data_sensor_day_clean <- data_day[
              !is.na(start_datetime)
            ]

            data_sensor_day_clean <- data_sensor_day_clean[
              , sensor := as.character(sensor)
            ] %>%
              # get node name by joining to configuration table:
              merge(
                configuration[
                  , .(detector_name, node_name, corridor_route)
                ],
                by.x = "sensor", by.y = "detector_name",
                all.x = T, all.y = F
              )

            # interpolate volume and occupancy based on sum and pct.null
            data_sensor_day_clean[
              , `:=`(
                volume_sum_impute =
                  fifelse(
                    volume.pct.null < 100,
                    volume.sum / (1 - volume.pct.null / 100),
                    NA_real_
                  ),
                occupancy_sum_impute =
                  fifelse(
                    occupancy.pct.null < 100,
                    occupancy.sum / (1 - occupancy.pct.null / 100),
                    NA_real_
                  )
              )
            ]

            # rolling interpolation based on temporal nearby 4 sensor data
            # (same day)
            data_sensor_day_clean[
              , volume_rollmean :=
                data.table::frollapply(
                  volume_sum_impute, 5,
                  mean,
                  # use 'center' to get previous and following 15-min sensor data
                  # this can ensure the most nearby sensor data
                  align = "center",  
                  na.rm = TRUE, hasNA = TRUE
                )
            ][
              , volume_sum_impute :=
                fifelse(
                  is.na(volume_sum_impute),
                  volume_rollmean, volume_sum_impute
                )
            ]

            data_sensor_day_clean[
              , occupancy_rollmean :=
                data.table::frollapply(
                  occupancy_sum_impute,
                  5, mean,
                  align = "center",
                  na.rm = TRUE, hasNA = TRUE
                )
            ][
              , occupancy_sum_impute :=
                fifelse(
                  is.na(occupancy_sum_impute),
                  occupancy_rollmean, occupancy_sum_impute
                )
            ]

            setnames(data_sensor_day_clean, toupper)

            # format start_datetime column
            data_sensor_day_clean[, `:=`(
              START_DATETIME = as_datetime(START_DATETIME),
              START_DATE = as.Date(START_DATETIME)
            )]

            # rolling interpolation based on historical nearby 2 sensor data
            # (same day of week)
            # only for detector with SOME missing impute values;
            # no need to impute if the data is missing the whole day
            if (sum(is.na(data_sensor_day_clean$VOLUME_SUM_IMPUTE)) /
              nrow(data_sensor_day_clean) > 0 &
              sum(is.na(data_sensor_day_clean$VOLUME_SUM_IMPUTE)) /
                nrow(data_sensor_day_clean) < 1) {
              con <- db_connect()

              # get 15min data 7 days ago
              historical_1 <- dbGetQuery(
                con, "SELECT START_DATETIME,VOLUME_SUM_IMPUTE FROM RTMC_15MIN
                  WHERE DETECTOR_NAME = ? AND START_DATE = ?",
                params = list(
                  a_detector, day - 7
                )
              ) %>% as.data.table()

              # format start_datetime column
              historical_1[, START_DATETIME := as_datetime(START_DATETIME) + days(7)]

              # get 15min data 14 days ago
              historical_2 <- dbGetQuery(
                con, paste0(
                  "SELECT START_DATETIME,VOLUME_SUM_IMPUTE FROM RTMC_15MIN
                  WHERE DETECTOR_NAME = ? AND START_DATE = ?"
                ),
                params = list(
                  a_detector, day - 14
                )
              ) %>% as.data.table()

              # format start_datetime column
              historical_2[, START_DATETIME := as_datetime(START_DATETIME) + days(14)]

              # join to current day data
              data_sensor_day_clean <- data_sensor_day_clean%>%
                merge(
                  historical_1,
                  by = "START_DATETIME", all.x = T,
                  suffixes = c("", ".H1")
                ) %>%
                merge(
                  historical_2,
                  by = "START_DATETIME", all.x = T,
                  suffixes = c("", ".H2")
                )

              # impute it as the avg of two historical days
              data_sensor_day_clean[
                , VOLUME_ROLLMEAN :=
                  rowMeans(.SD, na.rm = T),
                .SDcols = c(
                  "VOLUME_SUM_IMPUTE.H1",
                  "VOLUME_SUM_IMPUTE.H2"
                )
              ][
                , VOLUME_SUM_IMPUTE :=
                  fifelse(
                    is.na(VOLUME_SUM_IMPUTE),
                    VOLUME_ROLLMEAN, VOLUME_SUM_IMPUTE
                  )
              ]
            }

            # rolling interpolation based on historical nearby 2 sensor data
            # (same day of week)
            if (sum(is.na(data_sensor_day_clean$OCCUPANCY_SUM_IMPUTE)) /
              nrow(data_sensor_day_clean) > 0 &
              sum(is.na(data_sensor_day_clean$OCCUPANCY_SUM_IMPUTE)) /
                nrow(data_sensor_day_clean) < 1) {
              con <- db_connect()

              historical_1 <- dbGetQuery(
                con, paste0(
                  "SELECT START_DATETIME,OCCUPANCY_SUM_IMPUTE FROM RTMC_15MIN
                  WHERE DETECTOR_NAME = ? AND START_DATE = ?"
                ),
                params = list(
                  a_detector, day - 7
                )
              ) %>%
                as.data.table()

              # format start_datetime column
              historical_1[, START_DATETIME := as_datetime(START_DATETIME) + days(7)]

              historical_2 <- dbGetQuery(
                con, paste0(
                  "SELECT START_DATETIME,OCCUPANCY_SUM_IMPUTE FROM RTMC_15MIN
                  WHERE DETECTOR_NAME = ? AND START_DATE = ?"
                ),
                params = list(
                  a_detector, day - 14
                )
              ) %>%
                as.data.table()

              # format start_datetime column
              historical_2[, START_DATETIME := as_datetime(START_DATETIME) + days(14)]

              data_sensor_day_clean <- data_sensor_day_clean %>% 
                merge(historical_1,
                      by = "START_DATETIME", all.x = T,
                      suffixes = c("", ".H1")
                ) %>%
                merge(
                  historical_2,
                  by = "START_DATETIME", all.x = T,
                  suffixes = c("", ".H2")
                )
                  
              # impute it as the avg of two historical days
              data_sensor_day_clean[
                , OCCUPANCY_ROLLMEAN :=
                  rowMeans(.SD, na.rm = T),
                .SDcols = c(
                  "OCCUPANCY_SUM_IMPUTE.H1",
                  "OCCUPANCY_SUM_IMPUTE.H2"
                )
              ][
                , OCCUPANCY_SUM_IMPUTE :=
                  fifelse(
                    is.na(OCCUPANCY_SUM_IMPUTE),
                    OCCUPANCY_ROLLMEAN,
                    OCCUPANCY_SUM_IMPUTE
                  )
              ]
            }

            # select cols and format data
            data_sensor_day_clean <- data_sensor_day_clean[
              , .(
                SENSOR, START_DATETIME, START_DATE,
                VOLUME.PCT.NULL, VOLUME.SUM, VOLUME_SUM_IMPUTE,
                OCCUPANCY.PCT.NULL, OCCUPANCY.SUM, OCCUPANCY_SUM_IMPUTE,
                SPEED, NODE_NAME, CORRIDOR_ROUTE
              )
            ] %>%
              # match to database names (_ instead of '.')
              janitor::clean_names(., "all_caps") %>%
              setnames(
                old = c("SENSOR"),
                new = c("DETECTOR_NAME")
              ) %>%
              # Formatting numbers, dates, etc:
              .[, c(
                "VOLUME_PCT_NULL", "VOLUME_SUM",
                "VOLUME_SUM_IMPUTE", "OCCUPANCY_PCT_NULL",
                "OCCUPANCY_SUM", "OCCUPANCY_SUM_IMPUTE", "SPEED"
              ) :=
                .(
                  round(VOLUME_PCT_NULL, 1),
                  as.integer(round(VOLUME_SUM)),
                  as.integer(round(VOLUME_SUM_IMPUTE)),
                  round(OCCUPANCY_PCT_NULL, 1),
                  as.integer(round(OCCUPANCY_SUM)),
                  as.integer(round(OCCUPANCY_SUM_IMPUTE)),
                  round(SPEED, digits = 1)
                )]
          } # end if-else check for sensors without data
          # return value:

          data_sensor_day_clean
          # browser()
        },
        mc.cores = 5 # 5-core can avoid missing value caused by api hit failture
      ) %>% rbindlist()

    # Speed test result - download from trafdat:
    message("Download from trafdat complete:")
    tictoc::toc()

    # Write a day's worth of data to temp table -----
    # Speed test:
    message(paste0(
      "Writing ",
      nrow(data_day_test),
      "rows to temporary table."
    ))

    tictoc::tic()

    DBI::dbWriteTable(
      con, "RTMC_15MIN", data_day_test,
      append = T
    )

    tictoc::toc()
  }) # end for loop across days
}
