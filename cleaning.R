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

df_temp.mean<- timeAverage(df[-2:-4], avg.time = "day", data.thresh = 75)
df_temp.mean<- melt(df_temp.mean, id.vars = "date")
colnames(df_temp.mean)[colnames(df_temp.mean) == 'value'] <- 'temp.mean'

df_temp.low<- timeAverage(df[-2:-4], avg.time = "day", statistic = "min", data.thresh = 75)
df_temp.low<- melt(df_temp.low, id.vars = "date")
colnames(df_temp.low)[colnames(df_temp.low) == 'value'] <- 'low.temp'

df_temp.hi<- timeAverage(df[-2:-4], avg.time = "day", statistic = "max", data.thresh = 75)
df_temp.hi<- melt(df_temp.hi, id.vars = "date")
colnames(df_temp.hi)[colnames(df_temp.hi) == 'value'] <- 'hi.temp'

df_temp <- left_join(df_temp.hi, df_temp.low, by = c("date", "variable")) %>%
  left_join(df_temp.mean, by = c("date", "variable"))
colnames(df_temp)[colnames(df_temp) == 'variable'] <- 'cve'
df_temp$cve<-as.character(df_temp$cve)

rm(df_temp.hi, df_temp.low, df_temp.mean)

#  Relative humidity___________________________________________________________________________________________________#
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

# Wind speed___________________________________________________________________________________________________#
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

#Atmospheric pressure___________________________________________________________________________________________________#
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

#Precipitation___________________________________________________________________________________________________#
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

# Geographic Coordinates of REDMET stations___________________________________________________________________________________________________#
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

# Joins Adar's object with REDMET data___________________________________________________________________________________________________#
options(scipen = 50)
df_REDMET<-merge(df_REDMET,cat_estacion, by="cve")
df_REDMET_01_15<-subset(df_REDMET, date>"2001-01-01" & date<"2015-12-31")
df_REDMET_01_15<-df_REDMET_01_15[,c(10,1:9,11:17)]
df_REDMET_01_15<-df_REDMET_01_15[rowSums(is.na(df_REDMET_01_15[, c(4:10)])) != 7,]
df_REDMET_01_15$date<-as.Date(df_REDMET_01_15$date)
df_SMN_REDMET<-rbind(adarobject,df_REDMET_01_15, fill=T)
rm(cat_estacion, df_REDMET_01_15)

saveRDS(df_SMN_REDMET, file = "C:/Users/Iván/Dropbox/MEXICO_TEMPERATURE_MODEL/DATA_CLEANING/tmpcleaning/data/SMN_REDMET_2015_barmean_rain.rds")