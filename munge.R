# Read the Authority and School spreadsheet from Alberta Education
# extract the unique records and put them in three database tables
# Also scrapes some data - not very pretty I'm afraid
# D. Currie 2015-03-10
#
# Revised 2015-03-27 new data model
#         2014-04-15 newer data model
#
# TODO:  this version requires the file to be downloaded and exported as CSV
#        future version could read directly from the website (if the name is consistent)
#        direct use of geocoder.ca api could be added, for now a csv file is used

library(RSQLite)
library(data.table)
library(dplyr)
library(stringi)

csv.File<-"../../01_Incoming/Authority_and_School.csv"
gc.File<-"../../01_Incoming/geocoded_schools.csv"
gc2.File<-"../../01_Incoming/geocoded_auths.csv"
data.File<-"../../01_Incoming/01_Clean/schooldatatest.RData"
data2.File<-"../../01_Incoming/01_Clean/schooldata2.RData"
out.File<-"../../01_Incoming/to_geocode.csv"
db.Name<-"../../02_Prototypes/db/everactive6.sqlite3"

run_all<-function(dbName=db.Name){
  # run all the things
  create_db(dbName=dbName)
  basic_lists(dbName=dbName)
  facility_table(dbName=dbName)
  scrape_plo(dbName=dbName)
}

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
  aTbl$location_id<-c(1:dim(aTbl)[[1]])
  offs<-dim(aTbl)[[1]]+1000
  sTbl$location_id<-c(offs:(dim(sTbl)[[1]]+offs-1))
  
  # make the location table
  colSuf<-c("Address.1","Address.2","City","Province","Postal.Code")
  locCols<-append(paste0("Authority.",colSuf),c("Fulladr","Latitude","Longitude","location_id"))
  loc1<-aTbl[,locCols]
  locCols<-append(paste0("School.",colSuf),c("Fulladr","Latitude","Longitude","location_id"))
  loc2<-sTbl[,locCols]
  locCols<-gsub("School.","",locCols)
  colnames(loc1)<-locCols
  colnames(loc2)<-locCols  
  location<-rbind(loc1,loc2)
  # now deal with duplicated location records
  # this approach looks for duplicates in the Fulladr column, there may still be duplicates afterwards
  idCol<-which(names(location)=="location_id")
  addCol<-which(names(location)=="Fulladr")
  dupLocs<-aggregate(location[idCol],location[addCol],unique)

  # the result of the aggregate command is to give a list of duplicated locations in the idCol column
  # extract only the multiples
  mults<-dupLocs[which(lapply(dupLocs$location_id,length)>1),2]
  singles<-dupLocs[which(lapply(dupLocs$location_id,length)==1),2]
  fm<-lapply(mults,FUN=function(x) x[[1]])
  loclist<-append(singles,fm)
  location<-location[location$location_id %in% loclist,]

  # set the location ids to the first one in the list
  lapply(mults,FUN=function(x) aTbl[aTbl$location_id %in% x,"location_id"]<-x[[1]]) 
  lapply(mults,FUN=function(x) sTbl[sTbl$location_id %in% x,"location_id"]<-x[[1]]) 

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
  names(location)<-gsub("\\.","",tolower(names(location)))
  # add the tables
  schools_sql<-copy_to(theDB, sTbl, temporary=FALSE,indexes = list("SchoolCode","location_id"))
  auth_sql<-copy_to(theDB, aTbl, temporary=FALSE,indexes = list("AuthorityCode","location_id"))
  loc_sql<-copy_to(theDB, location, temporary=FALSE,indexes = list("location_id"))
  dbDisconnect(theDB$con)
}

# Create basic lists of types for data entry
basic_lists<-function(dataFile=data2.File, dbName=db.Name){
  # Facilities types
  facilitytype<-data.frame(id=1:3,
                           name=c("School","Authority","Other"),
                           description=c("A school building","Mailing address of school board or authority","A place"))
  
  # Program Types
  load(dataFile)
  prgms<-as.character(layerList[layerList$Type %in% c("Program","Event"),"Layer"])
  programtype<-data.frame(id=1:length(prgms),
                          name=prgms,
                          description=rep("To be added",length(prgms)))
  
  # open the (existing) database
  theDB<-src_sqlite(dbName)
  fac_list<-copy_to(theDB,facilitytype,temporary=FALSE,indexes=list("id"))
  prg_list<-copy_to(theDB,programtype,temporary=FALSE,indexes=list("id")) 
  dbDisconnect(theDB$con)
}


# scrape the PLO list from the csv file, created from the Word doc
scrape_plo<-function(ploFile="../../01_Incoming/plo_data.csv", dbName=db.Name){
  cLine<-readLines(ploFile)
  # workshop names
  temp<-strsplit(cLine[grep("Workshop",cLine)],",")
  workshop<-sapply(temp,FUN=function(x) x[[2]])
  # dates
  temp<-strsplit(cLine[grep("Date",cLine)],",")
  wkshp_date<-sapply(temp,FUN=function(x) x[[2]])
  # locations
  temp<-strsplit(cLine[grep("Location",cLine)],",")
  location<-sapply(temp,FUN=function(x) x[[2]])
  # participants
  temp<-strsplit(cLine[grep("participants",cLine)],",")
  partic<-sapply(temp,FUN=function(x) as.integer(x[[2]]))
  # make a dataframe of the workshops
  # hard code prog.id to the PLO value (6)
  wkshpDF<-data.frame(program_id=1:length(workshop),name=workshop,date=as.Date(wkshp_date,"%d-%b-%y"),location=location,progtype_id=6L)
  
  # now get the names,school,email records and assign a workshop id to each one
  strt<-list(strt=grep("^Name",cLine)+1,end=grep("^,,",cLine)-1)

  
  people<-data.frame(stringsAsFactors=FALSE)
  for(i in 1:length(strt$strt)){
    temp<-stri_split_fixed(cLine[strt$strt[i]:strt$end[i]],",",simplify=TRUE)
    colnames(temp)<-c("name","school","email")
    tempDF<-data.frame(temp,program_id=i,stringsAsFactors=FALSE)
    people<-rbind(people,tempDF)
  }
  
  # clean up the extra whitespace
  people$name<-stri_trim_both(people$name)
  people$email<-stri_trim_both(people$email)
  
  # break out the first, middle and last names
  # firstLastName<-stri_split_fixed(people$Name," ",simplify=TRUE)
  firstLastName<-stri_split_regex(people$name,"[ .]",simplify=TRUE,n=3)
  people$firstname<-firstLastName[,1]
  people$middlename<-ifelse(firstLastName[,3]=="","",firstLastName[,2])
  people$lastname<-ifelse(firstLastName[,3]=="",firstLastName[,2],firstLastName[,3])
  
  # try to match the school field with the school list in the database
 # db.Name<-"../../02_Prototypes/db/everactive3.sqlite3"
  theDB<-src_sqlite(dbName, create = F)
  sTbl<-collect(tbl(theDB,"sTbl"))
  fTbl<-collect(tbl(theDB,"facility"))
  # join the school and facility tables
  tempTbl<-left_join(sTbl,fTbl)
#  sTblDT<-data.table(sTbl)
  sTblDT<-data.table(tempTbl)
  test<-lapply(people$school,FUN=function(x) sTblDT[SchoolName %like% x,facility_id])
  # create a facilityID column and null it out
  people$facility_id<-NA
  # assign the easy matches first - School Name has only one match
  people$facility_id[which(sapply(test,length)==1)]<-simplify2array(test[which(sapply(test,length)==1)])
  
  # temporarily create a people table - just the good ones
  peopleTmp<-people[which(!is.na(people$facility_id)),c('firstname','middlename','lastname','email','facility_id','program_id')]
  peopleTmp$person_id<-1:dim(peopleTmp)[1]
  # create a join table - this is person_has_program indicating attendees
  j_person_program<-peopleTmp[,c('person_id','program_id')]
  peopleTmp$program_id<-NULL
  # create the join table - person_has_facility
  j_person_facility<-peopleTmp[,c('person_id','facility_id')]
  peopleTmp$facility_id<-NULL
  # is this next thing needed?
  # names(peopleTmp)<-c('firstname','middlename','lastname','email','facility_id','person_id')
  # stuff the database - note these are creation statements, future loads will be additions
  pTmp<-copy_to(theDB,peopleTmp,name='person',indexes=list('person_id'),temporary=FALSE)
  prog<-copy_to(theDB,wkshpDF,name='program',indexes=list('program_id'),temporary=FALSE)
  php<-copy_to(theDB,j_person_program,temporary=FALSE)
  phf<-copy_to(theDB,j_person_facility,temporary=FALSE)
  dbDisconnect(theDB$con)
}


# create the facility table and also the authority table 
facility_table<-function(dbName=db.Name){
  theDB<-src_sqlite(dbName, create = F)
  # combine the school and authority tables using a sql union
  facility_sql<-tbl(theDB,sql("select AuthorityCode as authority_id,SchoolName as name,SchoolPhone as phone,SchoolEmail as email,SchoolWebsite as website,SchoolFax as fax,location_id as location_id,SchoolCode as schoolcode,1 as 'facilitytype_id'
from sTbl
union all
select AuthorityCode as authority_id, AuthorityName as name, AuthorityTelephone as phone, AuthorityEmail as email, AuthorityWebsite as website, AuthorityFax as fax, location_id as location_id, NULL as 'schoolcode',2 as 'facilitytype_id' 
from aTbl"))
  # convert to a dataframe and add a facility_id column
  facility<-collect(facility_sql)
  facility$facility_id<-1:dim(facility)[1]
  new_fac<-copy_to(theDB,facility,indexes=list('facility_id'),temporary=FALSE)
  # new authority table from aTbl
  authority<-collect(tbl(theDB,sql("select AuthorityCode as authority_id, AuthorityName as name, AuthorityTelephone as phone, AuthorityEmail as email, AuthorityWebsite as website, AuthorityFax as fax from aTbl")))
  new_auth<-copy_to(theDB,authority,indexes=list('authority_id'),temporary=FALSE)
  dbDisconnect(theDB$con)
}