rm(list=ls(all=TRUE))

library(data.table)
#Reads Adar's object as an example of how data will be needed for Kodi's code
adarobject<-as.data.table(readRDS("C:\\Users\\Iván\\Dropbox\\MEXICO_TEMPERATURE_MODEL\\DATA_CLEANING\\tmpcleaning\\data\\all_stations_final.rds", refhook = NULL))

library(readxl)
library(plyr)
library(dplyr)
library(lubridate)
library(openair)
library(reshape2)

#Temperature
setwd("C:/Users/Iván/Dropbox/MEXICO_TEMPERATURE_MODEL/DATA_CLEANING/tmpcleaning/data/REDMET")
temp = list.files(pattern="*TMP.xls")
myfiles = lapply(temp, read_excel)
df <- bind_rows(myfiles, .id = "id")

#Estimates daily mean, min & max temperature with at least 75% daily data per site
df$date<- with(df, ymd_h(paste(FECHA, HORA, sep= ' ')))
df<-df[,c(which(colnames(df)=="date"),which(colnames(df)!="date"))]
df[df == -99] <- NA

l = lapply(c("high", "low", "mean"), function(type)
{temp <- timeAverage(df[-2:-4], avg.time = "day",
                     statistic = list(mean = "mean", low = "min", high = 
                                        "max")[[type]],
                     data.thresh = 75)
temp <- melt(temp, id.vars = "date")
colnames(temp)[colnames(temp) == 'value'] <- paste0(type, ".temp")
temp})
df_temp <- left_join(l[[1]], l[[2]], by = c("date", "variable")) %>%
  left_join(l[[3]], by = c("date", "variable"))
colnames(df_temp)[colnames(df_temp) == 'variable'] <- 'cve'


#  Relative humidity_______________________________________________________________________________________________#
temp = list.files(pattern="*RH.xls")
myfiles = lapply(temp, read_excel)
df2 <- bind_rows(myfiles, .id = "id")
df2$date<- with(df2, ymd_h(paste(FECHA, HORA, sep= ' ')))
df2<-df2[,c(which(colnames(df2)=="date"),which(colnames(df2)!="date"))]
df2[df2 == -99] <- NA
#Estimates daily mean relative humidity with at least 75% daily data per site
df_r.humidity<- timeAverage(df2[-2:-4], avg.time = "day", data.thresh = 75)
df_r.humidity<- melt(df_r.humidity, id.vars = "date")
colnames(df_r.humidity)[colnames(df_r.humidity) == 'value'] <- 'r.humidity.mean'
colnames(df_r.humidity)[colnames(df_r.humidity) == 'variable'] <- 'cve'
df_r.humidity$cve<-as.character(df_r.humidity$cve)
df_REDMET<-left_join(df_temp, df_r.humidity, by = c("date","cve"))

# Wind speed_______________________________________________________________________________________________________#
temp = list.files(pattern="*WSP.xls")
myfiles = lapply(temp, read_excel)
df3 <- bind_rows(myfiles, .id = "id")
df3$date<- with(df3, ymd_h(paste(FECHA, HORA, sep= ' ')))
df3<-df3[,c(which(colnames(df3)=="date"),which(colnames(df3)!="date"))]
df3[df3 == -99] <- NA
#Estimates daily mean relative humidity with at least 75% daily data per site
df_windsp<- timeAverage(df3[-2:-4], avg.time = "day", data.thresh = 75)
df_windsp<- melt(df_windsp, id.vars = "date")
colnames(df_windsp)[colnames(df_windsp) == 'value'] <- 'wind.speed.mean'
colnames(df_windsp)[colnames(df_windsp) == 'variable'] <- 'cve'
df_windsp$cve<-as.character(df_windsp$cve)
df_REDMET<-left_join(df_REDMET, df_windsp, by = c("date","cve"))

#Atmospheric pressure______________________________________________________________________________________________#
setwd("C:/Users/Iván/Dropbox/MEXICO_TEMPERATURE_MODEL/DATA_CLEANING/tmpcleaning/data/ATM_PRESSURE")
temp = list.files(pattern="*PA.xls")
myfiles = lapply(temp, read_excel)
df4 <- bind_rows(myfiles, .id = "id")
df4$date<- with(df4, ymd_h(paste(FECHA, HORA, sep= ' ')))
df4<-df4[,c(which(colnames(df4)=="date"),which(colnames(df4)!="date"))]
df4[df4 == -99] <- NA
#Mean daily atmospheric pressure
df_bar.mean<- timeAverage(df4[-2:-4], avg.time = "day", data.thresh = 75)
df_bar.mean<- melt(df_bar.mean, id.vars = "date")
colnames(df_bar.mean)[colnames(df_bar.mean) == 'value'] <- 'bar.mean'
colnames(df_bar.mean)[colnames(df_bar.mean) == 'variable'] <- 'cve'
df_bar.mean$cve<-as.character(df_bar.mean$cve)
df_REDMET<-left_join(df_REDMET, df_bar.mean, by = c("date","cve")) 

#Precipitation_____________________________________________________________________________________________________#
setwd("C:/Users/Iván/Dropbox/MEXICO_TEMPERATURE_MODEL/DATA_CLEANING/tmpcleaning/data/REDDA")
temp = list.files(pattern="*PPH.xls")
myfiles = lapply(temp, read_excel)
df5 <- bind_rows(myfiles, .id = "id")
df5$FECHA <- as.Date(df5$FECHA, format = "%Y-%m-%d")
colnames(df5)[colnames(df5) == 'FECHA'] <- 'date'
df5[df5 == -99] <- NA
#Rain
df_rain<- melt(df5[-1], id.vars = "date")
colnames(df_rain)[colnames(df_rain) == 'value'] <- 'rain.mean'
colnames(df_rain)[colnames(df_rain) == 'variable'] <- 'cve'
df_rain$cve<-as.character(df_rain$cve)
df_REDMET$date<-as.Date(df_REDMET$date)
df_REDMET<-left_join(df_REDMET, df_rain, by = c("date","cve")) 

rm(df, df_temp, df2, df_r.humidity, df3, df_windsp, df4, df_bar.mean, df5, df_rain)

# Geographic Coordinates of REDMET stations________________________________________________________________________#
cat_estacion <- read_excel("C:/Users/Iván/Dropbox/MEXICO_TEMPERATURE_MODEL/DATA_CLEANING/tmpcleaning/data/REDMET/cat_estacion.xlsx", 
                           col_types = c("text", "text", "numeric", "numeric", "numeric", "skip", "numeric"))
colnames(cat_estacion)[colnames(cat_estacion) == 'cve_estac'] <- 'cve'
cat_estacion$cve<-as.character(cat_estacion$cve)
colnames(cat_estacion)[colnames(cat_estacion) == 'nom_estac'] <- 'NOMBRE'
colnames(cat_estacion)[colnames(cat_estacion) == 'alt'] <- 'Altitud'
colnames(cat_estacion)[colnames(cat_estacion) == 'latitud'] <- 'latitude'
colnames(cat_estacion)[colnames(cat_estacion) == 'longitud'] <- 'longitude'
colnames(cat_estacion)[colnames(cat_estacion) == 'id_station'] <- 'stn'

cat_estacion$NOMBRE<-paste(cat_estacion$cve,"-",cat_estacion$NOMBRE)
cat_estacion$X<-round(cat_estacion$longitude,digits = 2)
cat_estacion$Y<-round(cat_estacion$latitude,digits = 2)
cat_estacion$ESTADO<-as.character("MCMA")
cat_estacion <- cat_estacion[, c(1,6,7:9,2,5,4,3)]

# Joins Adar's object with REDMET data_____________________________________________________________________________#
options(scipen = 50)
df_REDMET<-merge(df_REDMET,cat_estacion, by="cve")
df_REDMET_01_15<-subset(df_REDMET, date>"2001-01-01" & date<"2015-12-31")
df_REDMET_01_15<-df_REDMET_01_15[,c(10,1:9,11:17)]
df_REDMET_01_15<-df_REDMET_01_15[rowSums(is.na(df_REDMET_01_15[, c(4:10)])) != 7,]
df_REDMET_01_15$date<-as.Date(df_REDMET_01_15$date)
df_SMN_REDMET<-rbind(adarobject,df_REDMET_01_15, fill=T)
rm(cat_estacion, df_REDMET_01_15)

saveRDS(df_SMN_REDMET, file = "C:/Users/Iván/Dropbox/MEXICO_TEMPERATURE_MODEL/DATA_CLEANING/tmpcleaning/data/SMN_REDMET_2015_barmean_rain.rds")
  

####################################################################################################################

# Data cleaning from the SMN
# Get data from Onservatories
library(readr)
library(data.table)
library(tidyverse)
library(stringi) 
#library(MESS)
#library(naniar)

x = read_csv_chunked("C:/Users/Iván/Dropbox/MEXICO_TEMPERATURE_MODEL/DATA_CLEANING/tmpcleaning/data/SMN_2018_individual_files/OBS_2018/OBS_HLY.CSV",
                     chunk_size = 1e5,
                     DataFrameCallback$new(function(d, pos) 
                                           d[!is.na(d[["YEAR-MONTH-DAY"]]) &
                                               d[["YEAR-MONTH-DAY"]] >= "2003-01-01",]))
setDT(x)
unique(x$`ELEMENT-CODE`)
#We only need code 101 for temperature
x <- x[ which(`ELEMENT-CODE`==101),]
x <- x %>% select(-contains("FLAG"))
y <- x[,-c(1,3)]
y$`YEAR-MONTH-DAY` <- as.character(y$`YEAR-MONTH-DAY`)
colnames(y)[colnames(y) =='YEAR-MONTH-DAY'] <- 'date'
as.character(colnames(y)[colnames(y) =='Station-ID'] <- 'id')
y[y==-99999 | y>=50]<-NA
#y$non_NA <- apply(y[,c(3:26)], 1, function(y) sum(!is.na(y)))
saveRDS(y, file = "C:/TMP_MEX/cleaning/tempcleaning/obs_y")


#Melts "y" in order to apply the OpenAir package to calculate daily mean, min and max
#temperatures with 75% suficiency

z1 <-melt(y[,1:26], id.vars = c("id", "date"),
         variable.name = "time_v", 
         value.name = "temp")
z1$time <-as.numeric(z1$time_v)
z1$date <-with(z1, ymd_h(paste(date, time, sep= '_')))
z1$id <-as.character(with(z1, paste("id",id, sep= '_')))
z1$time_v <-NULL
z1$time <-NULL
z2 <-as.data.table(dcast(z1, date~rowid(date), value.var="temp"))
#The output removes 7 ids out of 70 yielding a "wide" data.table with only 64 columns, the first being for date

l = lapply(c("high", "low", "mean"), function(type)
{temp <- timeAverage(z2, avg.time = "day",
                     statistic = list(mean = "mean", low = "min", high =  "max")[[type]],
                     data.thresh = 75)
temp <- melt(temp, id.vars = "date")
colnames(temp)[colnames(temp) == 'value'] <- paste0(type, ".temp")
temp})
df_z2 <- left_join(l[[1]], l[[2]], by = c("date", "variable")) %>%
  left_join(l[[3]], by = c("date", "variable"))
colnames(df_z2)[colnames(df_z2) == 'variable'] <- 'stn'


#Second attemp to calculate daily mean, min and max temperatures without having to melt "y"
#and using our a function across columns in "y" to obtain daily mean, min and max with 75% daily 
#sufficiency

conditional <- function(y) {
  if (sum(!is.na(y[3:26])) >= 18) {
    c(max(y[3:26], na.rm = T), min(y[3:26], na.rm = T), mean(y[3:26], na.rm = T))
  } else {
    c(NA, NA, NA)
  }
}
z <- y[ ,apply(y[,c(3:26)], 1, FUN = conditional)]


conditional <- function(y) {
  if (sum(!is.na(y[3:26])) >= 18) {
    list(max(y[3:26], na.rm = T), min(y[3:26], na.rm = T), mean(y[3:26], na.rm = T))
  } else {
    c(NA, NA, NA)
  }
}
z <- y[ ,c(conditional[3:26]), by=date]

