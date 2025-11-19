# raw AIS data to parquet

library(arrow) #deal with parquet
library(here)  # for path
library(parallel) #mclapply and co
library(data.table)
library(dplyr)

#set the root of the project directory
here::i_am("algoabay.Rproj")

# 1. convert csv to zip file if any csv file in raw
csvfile<-dir(path=here("data","raw","ais"),patt=".csv",full=T,rec=T)
if(length(csvfile)>0){
	#function to zip a 'uu' csv file and to delete it
	fctzip<-function(uu){
		zip(sub(".csv",".zip",uu),uu)
		unlink(uu)
	}
	mclapply(csvfile,fctzip)
}

# 2. if needed convert csv zipped file into parquet format #{{{ 
# function to read a zipped csv file 'a' and to convert
# its content in parquet in 'dbpath' (cat for arrow is define by the group_by)
zip2parquet<-function(a,dbpath){
	dat<-fread(cmd= paste0('unzip -cq ',a),header=T,dec=".",encoding="Latin-1",sep=",",na.strings="NULL")
	ndat<-nrow(dat)
	print(paste(a,ndat))
        #define group on month	
	dat<-dat%>%mutate(year=substr(TIMESTAMP_UTC,1,4),
			  month=substr(TIMESTAMP_UTC,6,7))
	dbpath=here("data","processed","ais","parquet")
	dat%>%data.frame()%>%
		group_by(year,month)%>%
		write_dataset(path=dbpath,format="parquet")
	return(ndat)
}
#}}}
aisfile<-list.files(path=here("data","raw","ais"),full=T,rec=T)
nbline<-mclapply(aisfile,zip2parquet,dbpath=here("data","processed","ais","parquet"))

#control
nb1<-sum(do.call("rbind",nbline))
ds<-open_dataset(here("data/processed/ais/parquet"))
#nb2<-ds%>%group_by(AN)%>%summarise(n=n())%>%collect()
nb2<-ds%>%nrow()
#nb2<-ds%>%nrow()
print(paste(nb1,nb2))
stopifnot("line number diff"=nb1==nb2)



