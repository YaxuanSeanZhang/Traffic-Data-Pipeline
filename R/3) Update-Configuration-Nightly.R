source("_pull_data.R")

source("_db_connect.R") 

con <- db_connect()

days <- Sys.Date()

#get newest configuration file
configuration_new <- tc.sensors::pull_configuration() %>%
  as.data.table() %>%
  rename(
    NODE_NAME = r_node_name,
    NODE_N_TYPE = r_node_n_type,
    NODE_TRANSITION = r_node_transition,
    NODE_LABEL = r_node_label,
    NODE_LON = r_node_lon,
    NODE_LAT = r_node_lat,
    NODE_LANES = r_node_lanes,
    NODE_SHIFT = r_node_shift,
    NODE_S_LIMIT = r_node_s_limit,
    NODE_STATION_ID = r_node_station_id,
    NODE_ATTACH_SIDE = r_node_attach_side,
    LAST_CHANGE_DATE = date
  ) %>%
  rename_all(toupper)

configuration_new[configuration_new == ""] <- NA

#track change and update sql database
Track_Config_Change(configuration_new, days)