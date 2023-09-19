db_connect <- function(uid = Sys.getenv("UID"),
                       pwd = Sys.getenv("PWD")) {
  purrr::map(c(uid, pwd), rlang:::check_string)
  
  
  
  drv <- if (grepl("mac", osVersion)) {
    "FreeTDS"
  } else {
    "SQL Server"
  }
  
  
  
  # check that db can connect
  if (drv == "FreeTDS") {
    if (DBI::dbCanConnect(odbc::odbc(),
                          Driver = drv,
                          Database = "MTS_Planning_Data",
                          Uid = uid,
                          Pwd = pwd,
                          Server = "dbsqlcl11t.test.local,65414"
    ) == FALSE) {
      cli::cli_abort("Database failed to connect")
    }
  } else if (drv == "SQL Server") {
    if (DBI::dbCanConnect(odbc::odbc(),
                          Driver = drv,
                          Database = "MTS_Planning_Data",
                          Server = "dbsqlcl11t.test.local,65414",
                          Trusted_Connection = "yes"
    ) ==
    FALSE) {
      cli::cli_abort("Database failed to connect")
    }
  }
  
  
  
  # create db connection
  
  
  
  conn <- if (drv == "FreeTDS") {
    DBI::dbConnect(odbc::odbc(),
                   Driver = drv,
                   Database = "MTS_Planning_Data",
                   Uid = uid,
                   Pwd = pwd,
                   Server = "dbsqlcl11t.test.local,65414"
    )
  } else if (drv == "SQL Server") {
    DBI::dbConnect(odbc::odbc(),
                   Driver = drv,
                   Database = "MTS_Planning_Data",
                   Server = "dbsqlcl11t.test.local,65414",
                   Trusted_Connection = "yes"
    )
  }
  
  
  
  return(conn)
}