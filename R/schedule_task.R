library(taskscheduleR)
myscript <- "C:/Users/ZhangY/Desktop/Pull-Data/Run_Pipline_PullData.R"

## run script once within 62 seconds
#ONCE
taskscheduler_create(taskname = "PULL_once", rscript = myscript, 
                     schedule = "ONCE", starttime = format(Sys.time() + 60, "%H:%M"))

#EVERY 5MIN
taskscheduler_create(taskname = "PULL_5min", rscript = myscript,
                     schedule = "MINUTE", startdate = format(Sys.Date(), "%m/%d/%Y"),
                     starttime = "23:50", modifier = 5)
#DAILY
taskscheduler_create(taskname = "PULL_day", rscript = myscript, 
                     schedule = "DAILY",
                     starttime = "09:49",
                     startdate = format(Sys.Date(), "%m/%d/%Y"))

taskscheduler_delete(taskname = "PULL_once")
taskscheduler_delete(taskname = "PULL_5min")
taskscheduler_delete(taskname = "PULL_day")