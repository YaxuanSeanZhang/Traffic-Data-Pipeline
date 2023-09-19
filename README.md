# Traffic-Data-Pipeline

## Overview
LoopDetectorData is a comprehensive pipeline designed for retrieving data from the Minnesota Department of Transportation (MnDOT) loop detectors, which are installed across the Minnesota Freeway system. The pipeline operates on a nightly basis, facilitating the aggregation, modeling, and exploration of traffic volume. It enables the study of traffic trends throughout the Twin Cities freeway system, both before and after the COVID-19 pandemic. The resulting data is meticulously prepared to be seamlessly integrated into the [Rshiny app](https://metrotransitmn.shinyapps.io/freeway-traffic-trends/).

## Files
This repository consists of the following essential files:

### Data Pulling Pipeline
  * **_db_connect.R**: This script establishes a secure connection to the SQL database.
  * **_pull_data.R**: The core functionalities of this script involve retrieving historical configuration files, tracking configuration changes, and extracting sensor data from the API. It also encompasses the processes of data aggregation (15-minute intervals), interpolation of missing values, and data cleaning.
  * **1) Get-Historical-Configuration-Data-RunOnce.R**: This script retrieves detector configuration files ranging from 07-31-2019 to 06-23-2023. Additionally, it actively monitors changes within the configuration file, subsequently saving the historical configuration as _RTMC_CONFIG_HISTORICAL_ and the change log as _RTMC_CONFIG_CHANGELOG_.
  * **2) Write-Historical-SensorData-RunOnce.R**: This script retrieves historical data from the detector sensors. To define the desired time range for retrieval, navigate to Line 28 and Line 29. The extracted sensor data is systematically aggregated at 15-minute intervals and stored as _RTMC_15MIN_.
  * **3) Update-Configuration-Nightly.R**: On a nightly basis, this script updates the detector configuration data and concurrently monitors configuration file changes.
  * **4) Update-SensorData-Nightly.R**: This script facilitates the nightly retrieval of detector sensor data, introducing a three-day delay in the process.
  * **Run_Pipline_PullData.R**: Designed as an encompassing pipeline script, this file efficiently sources the aforementioned scripts in a seamless manner.

### Data Modeling Pipeline
  * **_db_connect.R**: This script establishes a secure connection to the SQL database.
  * **_analyze_data.R**: The core functionalities of this script involve aggregation of sensor data, Quality Assurance and Quality Control (QAQC) processes, Generalized Additive Model (GAM) modeling, and a comprehensive comparison between actual and predicted traffic volumes.
  * **1) SensorData-Modeling-Hourly-RunOnce.R**: This script fits the pre-COVID traffic volume data into a GAM model. This model operates at the hourly node level and effectively predicts traffic volume post-2020, in a scenario unaffected by the COVID-19 impact. The prediction results are save as _RTMC_PREDICT_HOUR_.
  * **2) SensorData-Modeling-Daily-RunOnce.R**: This script fits the pre-covid traffic volume into GAM model.This model operates at the daily node level and effectively predicts traffic volume post-2020, in a scenario unaffected by the COVID-19 impact. The prediction results are save as _RTMC_PREDICT_DAY_.
  * **3) SensorData-Comparison.R**: This script compares the actual traffic volume data (reflecting the COVID-19 impact) and the predicted traffic volume data (in the absence of COVID-19 impact). The outcomes of this script are primed for integration into the [Rshiny app](https://metrotransitmn.shinyapps.io/freeway-traffic-trends/).
  * **Run_Pipline_AnalyzeData.R**: Designed as an encompassing pipeline script, this file efficiently sources the aforementioned scripts in a seamless manner.

## Output
The execution of this pipeline yields a total of 5 distinct tables:
*  RTMC_CONFIG_HISTORICAL
*  RTMC_CONFIG_CHANGELOG
*  RTMC_15MIN
*  RTMC_PREDICT_HOUR
*  RTMC_PREDICT_DAY

## Future Work
The future work of this pipeline can be done by:
* Three-day Data Pulling Delay: Acknowledge that there exists a three-day interval between the extraction of configuration data and sensor data. 
* Automated Identification of Missing Data Days:  In cases where data updates are delayed on the MnDOT server, future versions of the pipeline could automatically identify these gaps in data. The pipeline would then smartly fetch and include the missing data for those days.
  
Please keep these issues in mind when using the Traffic-Data-Pipeline repo.
