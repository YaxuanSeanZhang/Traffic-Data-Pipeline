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
                          Database = "test",
                          Uid = uid,
                          Pwd = pwd,
                          Server = "test"
    ) == FALSE) {
      cli::cli_abort("Database failed to connect")
    }
  } else if (drv == "SQL Server") {
    if (DBI::dbCanConnect(odbc::odbc(),
                          Driver = drv,
                          Database = "test",
                          Server = "test",
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
                   Database = "test",
                   Uid = uid,
                   Pwd = pwd,
                   Server = "test"
    )
  } else if (drv == "SQL Server") {
    DBI::dbConnect(odbc::odbc(),
                   Driver = drv,
                   Database = "test",
                   Server = "test",
                   Trusted_Connection = "yes"
    )
  }
  
  
  
  return(conn)
}
