# This script collects and exports 90 percent club data.

# Revision History #

# 4/7/20 - Updated variables for March. Added system summary for OTP. Corrected threshold.

# 7/2/20 - Changed 90-percent club threshold to 300, per instruction from Director of Transportation.
#        - Added option to pull month-before-last, as requested by Public Affairs

# 8/28/20 - Changed 90-percent club threshold back to 675, per instructions from Ops VP and DVP

library(tidyverse)

con2 <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW",
                       Database = "DW_IndyGo", Port = 1433)

# set date variables

last_month_Avail <- as.Date(format(Sys.Date() - 30, '%Y-%m-01'))

this_month_Avail <- as.Date(format(Sys.Date() , '%Y-%m-01'))

# pull month before last for big board (public affairs)

month_before_last <- as.Date(format(Sys.Date() - 60, '%Y-%m-01'))

last_month_Avail <- as.Date(format(Sys.Date() - 30, '%Y-%m-01'))

# create calendar vector

last_month_calendar <- seq.Date(last_month_Avail,
                                (this_month_Avail - 1),
                                by = "day")

month_before_last_calendar <- seq.Date(month_before_last,
                                       (last_month_Avail - 1),
                                       by = "day")

# get calendar key

DimDate <- tbl(con2, "DimDate") %>%
  filter(CalendarDate < this_month_Avail, CalendarDate >= last_month_Avail) %>%
  collect()

DimDate_PA <- tbl(con2, "DimDate") %>%
  filter(CalendarDate < last_month_Avail, CalendarDate >= month_before_last) %>%
  collect()

# get operator key

DimUser <- tbl(con2, "DimUser") %>% collect()

# get patterns

Patterns <- tbl(con2, "DimPattern") %>% collect()

DimRoute <- tbl(con2, "DimRoute") %>% collect()

# get adhernece from last two months

FactTimepointAdherence_raw <- tbl(con2, "FactTimepointAdherence") %>% 
  filter(DateKey %in% local(DimDate$DateKey) | DateKey %in% local(DimDate_PA$DateKey)) %>% 
  select(everything(),-CoordinateList,-TimepointGeom) %>% 
  collect()

# check to make sure we have data for all dates

all(last_month_calendar == sort(unique(left_join(FactTimepointAdherence_raw,
                                             DimDate,
                                             by = "DateKey")$CalendarDate)))

Avail_OTP_operator_report <- FactTimepointAdherence_raw %>%
  filter(DateKey %in% DimDate$DateKey) %>% # comment out as needed
  # filter(DateKey %in% DimDate_PA$DateKey) %>% # comment out as needed
  left_join(Patterns, by = "PatternKey") %>%
  filter(DeadheadInd == 0) %>%
  filter(!is.na(DepartTimeVarianceSecs)) %>%
  left_join(select(DimUser, UserKey, LogonID, UserReportLabel), by = "UserKey") %>%
  left_join(select(DimDate, DateKey, CalendarDate), by = "DateKey") %>%
  mutate(Late = as.numeric(ifelse(DepartTimeVarianceSecs >= 300, "1", "0")),
         Early = as.numeric(ifelse(DepartTimeVarianceSecs <= -60, "1", "0")),
         On_Time = as.numeric(ifelse(DepartTimeVarianceSecs < 300 &
                                       DepartTimeVarianceSecs > -60  , "1", "0"))) %>%
  group_by(UserReportLabel, LogonID) %>%
  summarise(Actual_Departures = (sum(Early, na.rm = TRUE) +
                                   sum(Late, na.rm = TRUE) +
                                   sum(On_Time, na.rm = TRUE)),
            On_Time_Departures =  sum(On_Time, na.rm = TRUE),
            Early_Departures = sum(Early, na.rm = TRUE),
            Late_Departures = sum(Late, na.rm = TRUE),
            OTP = 
              sum(On_Time, na.rm = TRUE) / (sum(Early, na.rm = TRUE) +
                                              sum(Late, na.rm = TRUE)+
                                              sum(On_Time, na.rm = TRUE)
              ))

View(Avail_OTP_operator_report)

Ninety_percent_club <- Avail_OTP_operator_report %>%
  filter(Actual_Departures >= 675, # changed from 675 to 300 per instructions from Lois.. and back to 675
         (On_Time_Departures / Actual_Departures) >= 0.895) %>%
  mutate(OTP = scales::percent(OTP, accuracy = 1)) %>%
  select(Operator = UserReportLabel, LogonID,
         "Actual Departures" = Actual_Departures, 
         "On-Time Departures" = On_Time_Departures, 
         "Early Departures" = Early_Departures,
         "Late Departures" = Late_Departures, everything())

write.csv(Ninety_percent_club, file = "202007_Ninety_Percent_Club.csv",
          row.names = FALSE)



Avail_OTP_route_report <- FactTimepointAdherence_raw %>%
  filter(DateKey %in% DimDate$DateKey) %>% # comment out as needed
  # filter(DateKey %in% DimDate_PA$DateKey) %>% # comment out as needed
  left_join(Patterns, by = "PatternKey") %>%
  filter(DeadheadInd == 0) %>%
  filter(!is.na(DepartTimeVarianceSecs)) %>%
  left_join(DimRoute, by = "RouteKey") %>%
  left_join(select(DimDate, DateKey, CalendarDate), by = "DateKey") %>%
  mutate(Late = as.numeric(ifelse(DepartTimeVarianceSecs >= 300, "1", "0")),
         Early = as.numeric(ifelse(DepartTimeVarianceSecs <= -60, "1", "0")),
         On_Time = as.numeric(ifelse(DepartTimeVarianceSecs < 300 &
                                       DepartTimeVarianceSecs > -60, "1", "0"))) %>%
  group_by(RouteFareboxID) %>%
  summarise(OTP= 
              sum(On_Time, na.rm = TRUE) / (sum(Early, na.rm = TRUE) +
                                              sum(Late, na.rm = TRUE) +
                                              sum(On_Time, na.rm = TRUE)
              ),
            EP = 
              sum(Early, na.rm = TRUE) / (sum(Early, na.rm = TRUE) +
                                            sum(Late, na.rm = TRUE) +
                                            sum(On_Time, na.rm = TRUE)
              ),
            LP = 
              sum(Late, na.rm = TRUE) / (sum(Early, na.rm = TRUE) +
                                           sum(Late, na.rm = TRUE) +
                                           sum(On_Time, na.rm = TRUE)
              )) %>%
  mutate_at(., c("OTP", "EP", "LP"), ~scales::percent(., accuracy = 1)) %>%
  select(ROUTE = RouteFareboxID, 
         "ON-TIME %" = OTP, 
         "EARLY %" = EP, 
         "LATE %" = LP)

# now get system OTP.

System_OTP <- FactTimepointAdherence_raw %>%
  filter(DateKey %in% DimDate$DateKey) %>% # comment out as needed
  # filter(DateKey %in% DimDate_PA$DateKey) %>% # comment out as needed
  left_join(Patterns, by = "PatternKey") %>%
  filter(DeadheadInd == 0) %>%
  filter(!is.na(DepartTimeVarianceSecs)) %>%
  left_join(DimRoute, by = "RouteKey") %>%
  left_join(select(DimDate, DateKey, CalendarDate), by = "DateKey") %>%
  mutate(Late = as.numeric(ifelse(DepartTimeVarianceSecs >= 300, "1", "0")),
         Early = as.numeric(ifelse(DepartTimeVarianceSecs <= -60, "1", "0")),
         On_Time = as.numeric(ifelse(DepartTimeVarianceSecs < 300 &
                                       DepartTimeVarianceSecs > -60, "1", "0"))) %>%
  # group_by(RouteFareboxID) %>%
  summarise(OTP= 
              sum(On_Time, na.rm = TRUE) / (sum(Early, na.rm = TRUE) +
                                              sum(Late, na.rm = TRUE) +
                                              sum(On_Time, na.rm = TRUE)
              ),
            EP = 
              sum(Early, na.rm = TRUE) / (sum(Early, na.rm = TRUE) +
                                            sum(Late, na.rm = TRUE) +
                                            sum(On_Time, na.rm = TRUE)
              ),
            LP = 
              sum(Late, na.rm = TRUE) / (sum(Early, na.rm = TRUE) +
                                           sum(Late, na.rm = TRUE) +
                                           sum(On_Time, na.rm = TRUE)
              )) %>%
  mutate_at(., c("OTP", "EP", "LP"), ~scales::percent(., accuracy = 1)) %>%
  select("ON-TIME %" = OTP, 
         "EARLY %" = EP, 
         "LATE %" = LP)

System_OTP
System_OTP <- data.frame(Route = "TOTAL", System_OTP)            

colnames(System_OTP) <- colnames(Avail_OTP_route_report)

# bind

Avail_OTP_route_report <- rbind(Avail_OTP_route_report, System_OTP)

View(Avail_OTP_route_report )
write.csv(Avail_OTP_route_report, file = "202007_Route_OTP.csv",
          row.names = FALSE)


FactTimepointAdherence_raw %>%
  filter(DateKey %in% DimDate$DateKey) %>% # comment out as needed
  # filter(DateKey %in% DimDate_PA$DateKey) %>% # comment out as needed
  left_join(Patterns, by = "PatternKey") %>%
  filter(DeadheadInd == 0) %>%
  filter(!is.na(DepartTimeVarianceSecs)) %>%
  left_join(select(DimUser, UserKey, LogonID, UserReportLabel), by = "UserKey") %>%
  left_join(select(DimDate, DateKey, CalendarDate), by = "DateKey") %>%
  mutate(Late = as.numeric(ifelse(DepartTimeVarianceSecs >= 300, "1", "0")),
         Early = as.numeric(ifelse(DepartTimeVarianceSecs <= -60, "1", "0")),
         On_Time = as.numeric(ifelse(DepartTimeVarianceSecs < 300 &
                                       DepartTimeVarianceSecs > -60  , "1", "0"))) %>%
  filter(UserReportLabel == "Barnes, Paris") %>%
  data.table::fwrite("barnes_sam.csv")


