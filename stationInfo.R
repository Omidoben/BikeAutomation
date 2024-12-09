library(BikeDataCollection)
library(dplyr)
library(DBI)
library(RMySQL)
library(dotenv)

collect_station_info <- function(){
  station_info <- feeds_urls() %>%
    filter(name == "station_information") %>%
    pull(url) %>%
    get_data()

  # Tidy the data
  station_info <- station_info %>%
    magrittr::extract2("data") %>%
    dplyr::mutate(last_updated = station_info$last_updated) %>%
    dplyr::select(
      station_id,
      name,
      lat,
      lon,
      capacity,
      last_updated
    )

  # Database connection
  con <- dbConnect(
    MySQL(),
    dbname = "bikeshare_data",
    host = Sys.getenv("DB_HOST"),
    user = Sys.getenv("DB_USER"),
    password = Sys.getenv("DB_PASSWORD"),
    port = 3306,
    local_infile = 1,
    client.flag = CLIENT_LOCAL_FILES
  )

  # Check if table exists and handle accordingly
  if(dbExistsTable(con, "bike_station_info")){
    # Option 1: Truncate existing table
    dbExecute(con, "TRUNCATE TABLE bike_station_info")

    # Write new data
    dbWriteTable(
      con,
      name = "bike_station_info",
      value = station_info,
      append = FALSE,
      overwrite = TRUE,
      row.names = FALSE
    )

    message("Station information updated successfully.")
  } else {
    # Create table if it doesn't exist
    dbCreateTable(
      con,
      name = "bike_station_info",
      fields = station_info
    )

    dbWriteTable(
      con,
      name = "bike_station_info",
      value = station_info,
      append = FALSE,
      row.names = FALSE
    )

    message("Station information collected and stored successfully.")
  }

  # Close connection
  dbDisconnect(con)

  return(station_info)
}















