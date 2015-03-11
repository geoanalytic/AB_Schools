# Read the Authority and School spreadsheet from Alberta Education
# extract the unique records and put them in two database tables
# D. Currie 2015-03-10
# TODO:  this version requires the file to be downloaded and exported as CSV
#        future version could read directly from the website (if the name is consistent)
#        direct use of geocoder.ca api could be added, for now a csv file is used

library(dplyr)
csv.File<-"T:/Projects/PN2015-0924/01_Incoming/Authority_and_School.csv"
gc.File<-"T:/Projects/PN2015-0924/01_Incoming/geocoded_schools.csv"

authority_tbl<-function(csvFile=csv.File){
  # return the unique authority entries as a dataframe
  rawdat<-read.csv(csvFile,stringsAsFactors=FALSE)
  authRcds<-grep("Authority",names(rawdat))
  authDF<-unique(rawdat[,authRcds])
  authDF<-mutate(authDF,Fulladr=paste(Authority.Address.1,Authority.Address.2,Authority.City,Authority.Province,"Canada",Authority.Postal.Code))
  
  return(authDF)
}

school_tbl<-function(csvFile=csv.File){
  # return the unique school entries as a dataframe
  rawdat<-read.csv(csvFile,stringsAsFactors=FALSE)
  schRcds<-grep("School",names(rawdat))
  schRcds<-c(schRcds,grep("Authority.Code",names(rawdat)))
  schDF<-unique(rawdat[,schRcds])
  schDF<-mutate(schDF,Fulladr=paste(School.Address.1,School.Address.2,School.City,School.Province,"Canada",School.Postal.Code))
  return(schDF)
}

unique_addrs<-function(inDF){
  # extract address strings from a dataframe
  # return a dataframe with addresses and a list of school or authority codes 
  colNames<-names(inDF)
  adcols<-c(grep(".Address.1",colNames),grep(".Address.2",colNames),grep(".City",colNames),grep(".Province",colNames),grep(".Postal.Code",colNames))
  mutate(inDF,addr=paste(eval(colNames[adcols])))
  
  
}

gc_temp<-function(inDF, gcFile=gc.File){
  # geocode using the initial geocoded file 
  gS<-read.csv(gcFile, stringsAsFactors=FALSE)
  result<-left_join(inDF,gS[,c("School.Code","Latitude","Longitude")],by="School.Code")
  return(result)
}

tempstuff<-function(){
aS<-read.csv("T:/Projects/PN2015-0924/01_Incoming/Authority_and_School.csv")
gS<-read.csv("T:/Projects/PN2015-0924/01_Incoming/geocoded_schools.csv")
gS$School.Name<-sapply(gS$School.Code, FUN=function(x) aS[aS$School.Code==x,"School.Name"])
gS.out<-gS[,c("School.Name","Latitude","Longitude")]
write.csv(gS.out,file="T:/Projects/PN2015-0924/01_Incoming/geocoded_schools_min.csv",row.names=FALSE)

}