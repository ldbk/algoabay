# raw AIS data to parquet

library(arrow) #deal with parquet
library(here)  # for path
library(parallel) #mclapply and co
library(data.table)
library(dplyr)
library(readxl)

#set the root of the project directory
here::i_am("algoabay.Rproj")


xlsxfile<-list.files(path=here("data","raw","ais"),full=T,rec=T)
#read the xlsx file
aiscol<-c("MMSI","IMO","LAT","LON","SPEED","HEADING","COURSE","TIMESTAMP","SHIPNAME","SHIPTYPE")
dbpath<-here("data","processed","ais","parquet")
#guessing column type to avoid problem
coltype<-c("numeric","text","numeric","numeric","text","text","text","date","text","text")

dat2023<-read_xlsx(xlsxfile[1],col_names=aiscol,col_types=coltype)
dat2023<-dat2023%>%mutate(year=substr(TIMESTAMP,1,4),
			  month=substr(TIMESTAMP,6,7))
dat2023%>%data.frame()%>%
		group_by(year,month)%>%
		write_dataset(path=dbpath,format="parquet")
dat2024<-read_xlsx(xlsxfile[2],col_names=aiscol,col_types=coltype)
dat2024<-dat2024%>%mutate(year=substr(TIMESTAMP,1,4),
			  month=substr(TIMESTAMP,6,7))
dat2024%>%data.frame()%>%
		group_by(year,month)%>%
		write_dataset(path=dbpath,format="parquet")

#control
nb1<-nrow(dat2023)+nrow(dat2024)
ds<-open_dataset(here("data/processed/ais/parquet"))
#nb2<-ds%>%group_by(AN)%>%summarise(n=n())%>%collect()
nb2<-ds%>%nrow()
#nb2<-ds%>%nrow()
print(paste(nb1,nb2))
stopifnot("line number diff"=nb1==nb2)



