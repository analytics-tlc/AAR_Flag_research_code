library(fasttime)
library(lubridate)
library(RODBC)
library(data.table)
library(sqldf)
library(ggplot2)
library(dplyr)
library(httr)
library(jsonlite)
library(RCurl)
library(pbapply)
library(tidyverse)
library(taskscheduleR)
library(openxlsx)
library(readxl)
library(reshape)
library(ggthemes)
library(scales)
library(RSocrata)

tp2 = odbcConnect("azure_trip_data", uid = "wongph@tlc.nyc.gov")


#EHail TPEP Data
aar_data <- sqlQuery(tp2, "SELECT medallion, COUNT(DateTimeID)
                           FROM [TPEPDW].[dbo].[EHAIL_FF_TPEP] 
                           WHERE datetimeid >= 2021060100 and datetimeid < 2021070100
                           GROUP BY medallion")

aar_trips_day <- sqlQuery(tp2, "SELECT *
                                FROM [TPEPDW].[dbo].[EHAIL_FF_TPEP]
                                WHERE datetimeid >= 2021110100 and datetimeid < 2021110200")

yellow_aar_trips_day <- sqlQuery(tp2, "SELECT * 
                          FROM [TPEPDW].[dbo].TPEP2_Triprecord
                          WHERE datetimeid >= 2021110100 and datetimeid < 2021110200
                          AND AAR_Request IS NULL
                          ")
mismatched_trip_distance <- setdiff(yellow_aar_trips_day$tpep_pickup_datetime, aar_trips_day$pickup_datetime)
matched_trips <- union(yellow_aar_trips_day$tpep_pickup_datetime, aar_trips_day$pickup_datetime)
x <- data.frame(matched_trips)

aar_medallions <- unique(aar_data$medallion)
#Flex Fare TPEP Data
yellows <- sqlQuery(tp2, "SELECT medallion, COUNT(DateTimeID) 
                          FROM [TPEPDW].[dbo].TPEP2_Triprecord
                          WHERE datetimeid >= 2021060100 and datetimeid < 2021070100
                          AND AAR_Request = '0'
                          GROUP BY medallion")

yellows <- sqlQuery(tp2, "SELECT medallion, COUNT(DateTimeID) 
                          FROM [TPEPDW].[dbo].TPEP2_Triprecord
                          WHERE datetimeid >= 2021060100 and datetimeid < 2021070100
                          AND AAR_Request IS NULL
                          GROUP BY medallion")

yellows <- sqlQuery(tp2, "SELECT medallion, COUNT(DateTimeID) 
                          FROM [TPEPDW].[dbo].TPEP2_Triprecord
                          WHERE datetimeid >= 2021060100 and datetimeid < 2021070100
                          AND AAR_Request = '1'
                          GROUP BY medallion")


#aggregate trips by month for the last few years
#left join to compare ehail table vs yellow table -> look for any discrepancies

yellow_medallions <- unique(yellows$medallion)

mismatched_trip_distance <- setdiff(yellows$trip_distance, aar_data$trip_distance)
matched_trip_distance <- union(yellows$trip_distance, aar_data$trip_distance)

mismatched_total_amount <- setdiff(yellows$total_amount, aar_data$total_amount)
matched_total_amount <- union(yellows$total_amount, aar_data$total_amount)

mismatched_PULocationID <- setdiff(yellows$PULocationID, aar_data$PULocationID)
matched_PULocationID <- union(yellows$PULocationID, aar_data$PULocationID)

mismatched_vins <- setdiff(yellow_medallions, aar_medallions)
matched_vins <- union(yellow_medallions, aar_medallions)

matched <- left_join(yellows, aar_data, by = c('medallion', 'DateTimeID'))

                 