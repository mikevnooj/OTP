# 90 percent club, but make it data.table

library(data.table)
library(dplyr)

con_dw <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "AVAILDWHP01VW",
                         Database = "DW_IndyGo", Port = 1433)

#set dates
last_month <- as.IDate(format(Sys.Date() - 30, '%Y-%m-01'))

this_month <- as.IDate(format(Sys.Date(), '%Y-%m-01'))

#this one's for public affairs
month_before_last <- as.Date(format(Sys.Date() - 60, '%Y-%m-01'))

#create calendar
last_month_calendar <- seq.Date(last_month
                                ,(this_month - 1)
                                ,"day")

month_before_last_calendar <- seq.Date(month_before_last
                                       ,(last_month - 1)
                                       ,"day")

#get calendar key
DimDate <- tbl(con_dw,"DimDate") %>% 
 filter(CalendarDate < this_month, CalendarDate >= last_month) %>%
  collect() %>% 
  data.table(key = "DateKey")

DimDate_PA <- tbl(con_dw,"DimDate") %>% 
  filter(CalendarDate < last_month, CalendarDate >= month_before_last) %>%
  collect() %>% 
  data.table(key = "DateKey")

#get operator key

DimUser <- tbl(con_dw, "DimUser") %>% 
  collect() %>%
  data.table(key = "UserKey")

# get patterns

Patterns <- tbl(con_dw, "DimPattern") %>%
  collect() %>%
  data.table(key = "PatternKey")

DimRoute <- tbl(con_dw, "DimRoute") %>%
  collect() %>% 
  data.table(key = "RouteKey")

#get adherence form last two months
FactTimePointAdherenceRaw <-
  tbl(con_dw,"FactTimepointAdherence") %>% 
  filter(DateKey %in% local(DimDate$DateKey) | DateKey %in% local(DimDate_PA$DateKey)) %>% 
  select(everything(),-CoordinateList,-TimepointGeom) %>% 
  collect() %>% 
  data.table()

#date check
all(last_month_calendar == sort(unique(left_join(FactTimePointAdherenceRaw,
                                                 DimDate,
                                                 by = "DateKey")$CalendarDate)))


# operator otp ------------------------------------------------------------

OTP_Operator_Report <- Patterns[
  #join patterns to FactTimePointAdherenceRaw
  FactTimePointAdherenceRaw
  ,on = "PatternKey"
][
  #remove deadhead
  DeadheadInd == 0
][
  #this will filter, since DimDate is just the days we're looking for
  DimDate
  ,on = "DateKey"
][
  #join users
  DimUser
  ,on = "UserKey"
][
  #filter out n/as
  !is.na(DepartTimeVarianceSecs)
][
  #get T/F Late, Early, On_Time
  ,`:=` (
    Late = DepartTimeVarianceSecs >=  300
    ,Early = DepartTimeVarianceSecs <= -60
    ,On_Time = DepartTimeVarianceSecs < 300 & DepartTimeVarianceSecs > -60
  )
][
  #get on time, early, late
  ,.(
    On_Time_Departures = sum(On_Time)
    ,Early_Departures = sum(Early)
    ,Late_Departures = sum(Late)
  )
  ,.(UserReportLabel,LogonID)
][
  #get actual departs
  ,`:=`(
    Actual_Departures = 
      On_Time_Departures +
      Early_Departures+
      Late_Departures
  )
][
  #get otp
  ,OTP := On_Time_Departures / (Actual_Departures)
]

Ninety_Percent_Club <- 
  OTP_Operator_Report[
    Actual_Departures >= 675 &
      OTP > 0.895
  ][
    ,.(
      Operator = UserReportLabel
      ,LogonID
      ,"Actual Departures" = Actual_Departures
      ,"On-Time Departures" = On_Time_Departures
      ,"Early Departures" = Early_Departures
      ,"Late Departures" = Late_Departures
      ,OTP = scales::percent(OTP, accuracy = 1)
    )
  ][order(Operator)]
  
fwrite(Ninety_Percent_Club,paste0(format(last_month, "%Y%m"),"_Ninety_Percent_Club.csv"))


# route and system otp --------------------------------------------------------------
OTP_Route_Report <- Patterns[
  #join patterns to FactTimePointAdherenceRaw
  FactTimePointAdherenceRaw
  ,on = "PatternKey"
][
  #remove deadhead
  DeadheadInd == 0
][
  #this will filter, since DimDate is just the days we're looking for
  DimDate
  ,on = "DateKey"
][
  DimRoute
  ,on = "RouteKey"
][
  !is.na(DepartTimeVarianceSecs)
][
  #get T/F Late, Early, On_Time
  ,`:=` (
    Late = DepartTimeVarianceSecs >=  300
    ,Early = DepartTimeVarianceSecs <= -60
    ,On_Time = DepartTimeVarianceSecs < 300 & DepartTimeVarianceSecs > -60
    )
][
  #get on time, early, late
  ,.(
    On_Time_Departures = sum(On_Time)
    ,Early_Departures = sum(Early)
    ,Late_Departures = sum(Late)
  )
  ,.(RouteFareboxID)
][
  #get actual departs
  ,`:=`(
    Actual_Departures = 
      On_Time_Departures +
      Early_Departures+
      Late_Departures
    )
][
  #get otp
  ,OTP := On_Time_Departures / (Actual_Departures)
][
  #change some names
  ,.(
    Route = RouteFareboxID
    ,"On-Time %" = scales::percent(OTP, accuracy = 1)
    ,"Early %" = scales::percent(Early_Departures/Actual_Departures, accuracy = 1)
    ,"Late %" = scales::percent(Late_Departures/Actual_Departures, accuracy = 1)
  )
][
  order(Route)
]

System_OTP <- Patterns[
  #join patterns to FactTimePointAdherenceRaw
  FactTimePointAdherenceRaw
  ,on = "PatternKey"
][
  #remove deadhead
  DeadheadInd == 0
][
  #this will filter, since DimDate is just the days we're looking for
  DimDate
  ,on = "DateKey"
][
  DimRoute
  ,on = "RouteKey"
][
  !is.na(DepartTimeVarianceSecs)
][
  #get T/F Late, Early, On_Time
  ,`:=` (
    Late = DepartTimeVarianceSecs >=  300
    ,Early = DepartTimeVarianceSecs <= -60
    ,On_Time = DepartTimeVarianceSecs < 300 & DepartTimeVarianceSecs > -60
    )
][
  #get on time, early, late
  ,.(
    On_Time_Departures = sum(On_Time)
    ,Early_Departures = sum(Early)
    ,Late_Departures = sum(Late)
    )
][
  #get actual departs
  ,`:=`(
    Actual_Departures = 
      On_Time_Departures +
      Early_Departures+
      Late_Departures
    )
][
  #get otp
  ,OTP := On_Time_Departures / (Actual_Departures)
][
  #change some names
  ,.(
    Route = "TOTAL"
    ,"On-Time %" = scales::percent(OTP, accuracy = 1)
    ,"Early %" = scales::percent(Early_Departures/Actual_Departures, accuracy = 1)
    ,"Late %" = scales::percent(Late_Departures/Actual_Departures, accuracy = 1)
    )
]

OTP_Route_With_Totals <- rbind(OTP_Route_Report,System_OTP)

fwrite(OTP_Route_With_Totals, paste0(format(last_month, "%Y%m"),"_Route_OTP.csv"))
