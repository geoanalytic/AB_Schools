# Read the Authority and School spreadsheet from Alberta Education
# extract the unique records and put them in three database tables
# D. Currie 2015-03-10
#
# Revised 2015-03-27 new data model
#
# TODO:  this version requires the file to be downloaded and exported as CSV
#        future version could read directly from the website (if the name is consistent)
#        direct use of geocoder.ca api could be added, for now a csv file is used

library(dplyr)

csv.File<-"../../01_Incoming/Authority_and_School.csv"
gc.File<-"../../01_Incoming/geocoded_schools.csv"
gc2.File<-"../../01_Incoming/geocoded_auths.csv"
data.File<-"../../01_Incoming/01_Clean/schooldatatest.RData"
out.File<-"../../01_Incoming/to_geocode.csv"
db.Name<-"../../02_Prototypes/db/everactive.sqlite3"

authority_tbl<-function(csvFile=csv.File){
  # return the unique authority entries as a dataframe
  # read the full dataset
  rawdat<-read.csv(csvFile,stringsAsFactors=FALSE)
  # list the columns with the right flavour
  authRcds<-grep("Authority",names(rawdat))
  # get the unique records 
  authDF<-unique(rawdat[,authRcds])
  # create a full address field
  authDF<-mutate(authDF,Fulladr=paste(Authority.Address.1,Authority.Address.2,Authority.City,Authority.Province,"Canada",Authority.Postal.Code))
  
  return(authDF)
}

school_tbl<-function(csvFile=csv.File){
  # return the unique school entries as a dataframe
  # similar to  authority_tbl() but adds the Authority.Code as a key
  rawdat<-read.csv(csvFile,stringsAsFactors=FALSE)
  schRcds<-grep("School",names(rawdat))
  schRcds<-c(schRcds,grep("Authority.Code",names(rawdat)))
  schDF<-unique(rawdat[,schRcds])
  schDF<-mutate(schDF,Fulladr=paste(School.Address.1,School.Address.2,School.City,School.Province,"Canada",School.Postal.Code))
  return(schDF)
}

gc_schools<-function(inDF, gcFile=gc.File){
  # geocode using the initial geocoded file 
  gS<-read.csv(gcFile, stringsAsFactors=FALSE)
  result<-left_join(inDF,gS[,c("School.Code","Latitude","Longitude")],by="School.Code")
  return(result)
}

gc_auths<-function(inDF, gcFile=gc2.File){
  # geocode using the authorities geocoded file 
  gS<-read.csv(gcFile, stringsAsFactors=FALSE)
  result<-left_join(inDF,gS[,c("Authority.Code","Latitude","Longitude")],by="Authority.Code")
  return(result)
}

to_geocode<-function(inDF, outFile=out.File){
  # create a list of addresses for geocoding
  # first figure out which columns to pass to the geocoder
  outCols<-which(names(aTbl) %in% c("School.Code","Authority.Code","Fulladr"))

  # now check if any records are coded
  if(with(inDF,exists('Latitude'))==FALSE){
    # in which case get all the address combos
    result<-inDF[,outCols]
  } else {
    # otherwise return only uncoded records
    missing<-which(is.na(inDF$Latitude))
    result<-inDF[missing,outCols]
  }
  # test for bad addresses here
  badAdrs<-which(gsub(" ","",result$Fulladr)=="Canada")

  if(length(badAdrs)>0){
    result<-result[-badAdrs,]  
    message(paste0(length(badAdrs)," invalid address records were removed"))
  }
  if(!is.null(outFile))
    write.csv(result,file=outFile,row.names=FALSE)
  return(result)
}

create_tables<-function(csvFile=csv.File, gcFile=gc.File, gc2File=gc2.File, dataFile=data.File, dbName=db.Name){
  aTbl<-authority_tbl(csvFile)
  sTbl<-school_tbl(csvFile)
  sTbl<-gc_schools(sTbl, gcFile)
  aTbl<-gc_auths(aTbl, gc2File)
  
  # make sure lat and long columns are numeric
  aTbl$Latitude<-as.numeric(aTbl$Latitude)
  aTbl$Longitude<-as.numeric(aTbl$Longitude)
  sTbl$Latitude<-as.numeric(sTbl$Latitude)
  sTbl$Longitude<-as.numeric(sTbl$Longitude)
  
  # save them now
  save(aTbl,sTbl,file=dataFile)

}

create_db<-function(dataFile=data.File, dbName=db.Name){
  # create a sqlite database and push the schools and authorities tables into it
  # retrieve the clean school and authorities table
  load(dataFile)
  
  # add location id values to the tables before we start
  aTbl$loc.ID<-c(1:dim(aTbl)[[1]])
  offs<-dim(aTbl)[[1]]+1000
  sTbl$loc.ID<-c(offs:(dim(sTbl)[[1]]+offs-1))
  
  # make the location table
  colSuf<-c("Address.1","Address.2","City","Province","Postal.Code")
  locCols<-append(paste0("Authority.",colSuf),c("Fulladr","Latitude","Longitude","loc.ID"))
  loc1<-aTbl[,locCols]
  locCols<-append(paste0("School.",colSuf),c("Fulladr","Latitude","Longitude","loc.ID"))
  loc2<-sTbl[,locCols]
  locCols<-gsub("School.","",locCols)
  colnames(loc1)<-locCols
  colnames(loc2)<-locCols  
  Location<-rbind(loc1,loc2)
  # now deal with duplicated location records
  # this approach looks for duplicates in the Fulladr column, there may still be duplicates afterwards
  idCol<-which(names(Location)=="loc.ID")
  addCol<-which(names(Location)=="Fulladr")
  dupLocs<-aggregate(Location[idCol],Location[addCol],unique)

  # the result of the aggregate command is to give a list of duplicated locations in the idCol column
  # extract only the multiples
  mults<-dupLocs[which(lapply(dupLocs$loc.ID,length)>1),2]
  singles<-dupLocs[which(lapply(dupLocs$loc.ID,length)==1),2]
  fm<-lapply(mults,FUN=function(x) x[[1]])
  loclist<-append(singles,fm)
  Location<-Location[Location$loc.ID %in% loclist,]

  # set the location ids to the first one in the list
  lapply(mults,FUN=function(x) aTbl[aTbl$loc.ID %in% x,"loc.ID"]<-x[[1]]) 
  lapply(mults,FUN=function(x) sTbl[sTbl$loc.ID %in% x,"loc.ID"]<-x[[1]]) 

  # now remove the address columns from the school and auth tables
  locCols<-append(paste0("Authority.",colSuf),c("Fulladr","Latitude","Longitude"))
  aTbl[,locCols]<-list(NULL)
  locCols<-append(paste0("School.",colSuf),c("Fulladr","Latitude","Longitude"))
  sTbl[,locCols]<-list(NULL)

  # create an empty database
  theDB<-src_sqlite(dbName, create = T)
  
  # clean all the dots out of the column names
  names(sTbl)<-gsub("\\.","",names(sTbl))
  names(aTbl)<-gsub("\\.","",names(aTbl))
  names(Location)<-gsub("\\.","",names(Location))
  # add the tables
  schools_sql<-copy_to(theDB, sTbl, temporary=FALSE,indexes = list("SchoolCode","locID"))
  auth_sql<-copy_to(theDB, aTbl, temporary=FALSE,indexes = list("AuthorityCode","locID"))
  loc_sql<-copy_to(theDB, Location, temporary=FALSE,indexes = list("locID"))
}

