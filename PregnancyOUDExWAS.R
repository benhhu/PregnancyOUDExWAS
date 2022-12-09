library(sqldf)  
library(data.table)
library(tableone)
library(stringr)
library(lubridate)
library(dplyr)
library(table1)
library(eeptools)
library(ggplot2)
library(tidyr)
library(bestNormalize)
library(parallel)
library(WGCNA)
library(mice)
library(speedglm)
library(lme4)
library(caTools)
library(reshape2)

#set working directory
setwd("...") #specify the working directory here

### Data engineering for linked OneFL and VSBR ###

#load vsbr file
load("/VSBR/mother1.Rda")
load("/VSBR/mother2.Rda")
#load("/VSBR/baby1.Rda")
#load("/VSBR/baby2.Rda")

#load birth-OneFL linked identifiers
mother1_id<-read.csv("/OneFLVSBRLinkage/mother1_linked.csv",stringsAsFactors = F)
mother2_id<-read.csv("/OneFLVSBRLinkage/mother2_linked.csv",stringsAsFactors = F)
colnames(mother1_id)[1]<-"PATID"
colnames(mother2_id)[1]<-"PATID"

#load linked OneFL EHR data
mother1_demo<-read.csv("/OneFL/mother1/demographic.csv",header = T,stringsAsFactors = F)
mother2_demo<-read.csv("/OneFL/mother2/demographic.csv",header = T,stringsAsFactors = F)
colnames(mother1_demo)[1]<-"linkage_patid"
colnames(mother2_demo)[1]<-"linkage_patid"
mother1_demo<-mother1_demo[order(mother1_demo$linkage_patid),] 
mother2_demo<-mother2_demo[order(mother2_demo$linkage_patid),] 

q<-"select mother1_demo.*, mother1_id.OneFloridaID from mother1_demo left outer join mother1_id on mother1_demo.linkage_patid=mother1_id.PATID;"
mother1_demo<-sqldf(q)
q<-"select mother2_demo.*, mother2_id.OneFloridaID from mother2_demo left outer join mother2_id on mother2_demo.linkage_patid=mother2_id.PATID;"
mother2_demo<-sqldf(q)

#load enrollment table 
mother1_erl<-read.csv("/OneFL/mother1/enrollment.csv",stringsAsFactors = F)
mother2_erl<-read.csv("/OneFL/mother2/enrollment.csv",stringsAsFactors = F)
colnames(mother1_erl)[1]<-"linkage_patid"
colnames(mother2_erl)[1]<-"linkage_patid"
mother1_erl<-unique(mother1_erl)
mother2_erl<-unique(mother2_erl)

mother1_erl<-mother1_erl[order(mother1_erl$linkage_patid),]
mother2_erl<-mother2_erl[order(mother2_erl$linkage_patid),]

mother1_erl$ENR_START_DATE<-as.Date(mother1_erl$ENR_START_DATE,"%Y-%m-%d")
mother1_erl$ENR_END_DATE<-as.Date(mother1_erl$ENR_END_DATE,"%Y-%m-%d")
mother2_erl$ENR_START_DATE<-as.Date(mother2_erl$ENR_START_DATE,"%Y-%m-%d")
mother2_erl$ENR_END_DATE<-as.Date(mother2_erl$ENR_END_DATE,"%Y-%m-%d")

#Exclusion 1. Exclude patid with babydob not covered by the enrollment period
#join baby's dob to mother1_erl and mother2_erl
q<-"select mother1_erl.*, mother1.babydob from mother1_erl left outer join mother1 on mother1_erl.linkage_patid=mother1.patid;"
mother1_erl<-sqldf(q)
q<-"select mother2_erl.*, mother2.babydob from mother2_erl left outer join mother2 on mother2_erl.linkage_patid=mother2.patid;"
mother2_erl<-sqldf(q)

#check whether baby's dob is covered by enrollment period
mother1_erl$covered<-0
mother1_erl$covered[mother1_erl$ENR_START_DATE<=mother1_erl$babydob & mother1_erl$ENR_END_DATE>=mother1_erl$babydob]<-1
mother2_erl$covered<-0
mother2_erl$covered[mother2_erl$ENR_START_DATE<=mother2_erl$babydob & mother2_erl$ENR_END_DATE>=mother2_erl$babydob]<-1

#exclude
mother1_patid<-unique(mother1_erl$linkage_patid[mother1_erl$covered %in% c(1)]) 
mother2_patid<-unique(mother2_erl$linkage_patid[mother2_erl$covered %in% c(1)])

#Exclusion 2. Exclude patid with different mother's dob
#join mother's dob, sex, and race from vsbr to demo
mother1_demo<-mother1_demo[mother1_demo$linkage_patid %in% mother1_patid,] 
mother2_demo<-mother2_demo[mother2_demo$linkage_patid %in% mother2_patid,] 

q<-"select mother1_demo.*, mother1.dob as vsbr_dob, mother1.sex as vsbr_sex, mother1.race as vsbr_race from mother1_demo left outer join mother1 on mother1_demo.linkage_patid=mother1.patid;"
mother1_demo<-sqldf(q)
q<-"select mother2_demo.*, mother2.dob as vsbr_dob, mother2.sex as vsbr_sex, mother2.race as vsbr_race from mother2_demo left outer join mother2 on mother2_demo.linkage_patid=mother2.patid;"
mother2_demo<-sqldf(q)

mother1_demo$BIRTH_DATE<-as.Date(mother1_demo$BIRTH_DATE,"%Y-%m-%d")
mother1_demo$vsbr_dob<-as.Date(mother1_demo$vsbr_dob,origin = "1970-01-01")
mother2_demo$BIRTH_DATE<-as.Date(mother2_demo$BIRTH_DATE,"%Y-%m-%d")
mother2_demo$vsbr_dob<-as.Date(mother2_demo$vsbr_dob,origin = "1970-01-01")

#exclude
mother1_demo<-mother1_demo[mother1_demo$BIRTH_DATE == mother1_demo$vsbr_dob,] 
mother2_demo<-mother2_demo[mother2_demo$BIRTH_DATE == mother2_demo$vsbr_dob,]

#Exclusion 3. Exclude patid with OneFL SEX = M
mother1_demo<-mother1_demo[!(mother1_demo$SEX %in% c("M")),] #256604->256599
mother2_demo<-mother2_demo[!(mother2_demo$SEX %in% c("M")),] #321714->321708

#Exclusion 4. Exclude patid with OneFL RACE in (01-05) and VSBR race in (01-05) but not matching each other
mother1_demo<-mother1_demo[!( (mother1_demo$RACE %in% c("01","02","03","04","05")) & (mother1_demo$vsbr_race %in% c("01","02","03","04","05")) & (mother1_demo$RACE!=mother1_demo$vsbr_race) ),] 
mother2_demo<-mother2_demo[!( (mother2_demo$RACE %in% c("01","02","03","04","05")) & (mother2_demo$vsbr_race %in% c("01","02","03","04","05")) & (mother2_demo$RACE!=mother2_demo$vsbr_race) ),]

#get unique patid
mother1_patid<-unique(mother1_demo$linkage_patid) #252076
mother2_patid<-unique(mother2_demo$linkage_patid) #316262
length(unique(c(mother1_patid,mother2_patid))) #329880

#Exclusion 5. Exclude patid with delivery dates in OneFL and VSBR not matching each other
memory.limit(size=20000)

#diagnosis and procedure codes related to live birth delivery
#diagnosis codes
icd9<-c("644.21","645.11","645.21","649.81","649.82","650","651.01","651.11","651.21","651.31","651.41","651.51","651.61","669.70","669.71","V270","V27.2","V27.3","V27.5","V27.6","64421","64511","64521","64981","64982","650","65101","65111","65121","65131","65141","65151","65161","66970","66971","V270","V272","V273","V275","V276")

icd10<-c("O480","O481","O7582","O80","O30001","O30002","O30003","O30101","O30102","O30103","O30201","O30202","O30203","O82","Z370","Z372","Z373","Z3759","Z3769","O6012X0","O6013X0","O6014X0","O3111X0","O48.0","O48.1","O75.82","O80","O30.001","O30.002","O30.003","O30.101","O30.102","O30.103","O30.201","O30.202","O30.203","O82","Z37.0","Z37.2","Z37.3","Z37.59","Z37.69","O60.12X0","O60.13X0","O60.14X0","O31.11X0","O31.11X0","O31.11X0","O6012","O6013","O6014","O3111")

#procedure codes
cpt<-c("59612","59614","59620")

icd9_p<-c("72.0","72.1","72.2","72.21","72.29","72.3","72.31","72.39","72.4","72.5","72.51","72.52","72.53","72.54","72.6","72.7","72.71","72.79","72.8","72.9","73.0","73.01","73.09","73.1","73.2","73.22","73.3","73.4","73.5","73.51","73.59","73.6","73.8","73.9","73.91","73.92","73.93","73.94","73.99","74","74.0","74.1","74.2","74.4","74.9","74.99","720","721","722","7221","7229","723","7231","7239","724","725","7251","7252","7253","7254","726","727","7271","7279","728","729","730","7301","7309","731","732","7322","733","734","735","7351","7359","736","738","739","7391","7392","7393","7394","7399","74","740","741","742","744","749","7499")

icd10_p<-c("10D07Z3", "0W8NXZZ", "10D07Z4", "10D07Z5", "10S07ZZ", "10D07Z6", "10D07Z8", "10900ZC", "10903ZC", "10904ZC", "10907ZC", "10908ZC", "0U7C7ZZ", "10D07Z7", "10J07ZZ", "3E030VJ", "3E033VJ", "3E040VJ", "3E043VJ", "3E050VJ", "3E053VJ", "3E060VJ", "3E063VJ", "3E0DXGC", "3E0P3VZ", "3E0P7VZ", "10E0XZZ", "10907ZA", "10908ZA", "10A07ZZ", "10A08ZZ", "10S0XZZ", "0Q820ZZ", "0Q823ZZ", "0Q824ZZ", "0Q830ZZ", "0Q833ZZ", "0Q834ZZ", "10D07ZB", "10D00Z0", "10D00Z1", "10D00Z2")

mother1_diag<-fread("/OneFL/mother1/diag_short.csv",header=T, stringsAsFactors = F)
mother2_diag<-fread("/OneFL/mother2/diag_short.csv",header=T, stringsAsFactors = F)

mother1_proc<-fread("/OneFL/mother1/procedures_short.csv",header=T, stringsAsFactors = F)
mother2_proc<-fread("/OneFL/mother2/procedures_short.csv",header=T, stringsAsFactors = F)

mother1_diag<-mother1_diag[mother1_diag$linkage_patid %in% mother1_patid,] 
mother2_diag<-mother2_diag[mother2_diag$linkage_patid %in% mother2_patid,] 

mother1_proc<-mother1_proc[mother1_proc$linkage_patid %in% mother1_patid,] 
mother2_proc<-mother2_proc[mother2_proc$linkage_patid %in% mother2_patid,] 

#clear blanks/special characters in DX/PX string  
mother1_diag$clean_dx<-str_replace_all(mother1_diag$DX, "[^[:alnum:]]", "")  
mother2_diag$clean_dx<-str_replace_all(mother2_diag$DX, "[^[:alnum:]]", "")  
mother1_proc$clean_raw_px<-str_replace_all(mother1_proc$RAW_PX, "[^[:alnum:]]", "")  
mother2_proc$clean_raw_px<-str_replace_all(mother2_proc$RAW_PX, "[^[:alnum:]]", "")  

#remove individuals without any codes related to delivery
mother1_diag<-mother1_diag[(mother1_diag$DX_TYPE %in% 9 & mother1_diag$clean_dx %in% icd9) | (mother1_diag$DX_TYPE %in% 10 & mother1_diag$clean_dx %in% icd10),]
mother2_diag<-mother2_diag[(mother2_diag$DX_TYPE %in% 9 & mother2_diag$clean_dx %in% icd9) | (mother2_diag$DX_TYPE %in% 10 & mother2_diag$clean_dx %in% icd10),]

mother1_proc<-mother1_proc[(mother1_proc$RAW_PX_TYPE %in% c("CH","CPT-4","CPT4","C4","HC","HCPCS") & mother1_proc$clean_raw_px %in% cpt) | (mother1_proc$RAW_PX_TYPE %in% c("ICD-9-CM","ICD9CM","ICD9","09") & mother1_proc$clean_raw_px %in% icd9_p) | (mother1_proc$RAW_PX_TYPE %in% c("ICD-10-CM","ICD10-PCS","ICD10","10") & mother1_proc$clean_raw_px %in% icd10_p),] #247985 --> 191910
mother2_proc<-mother2_proc[(mother2_proc$RAW_PX_TYPE %in% c("CH","CPT-4","CPT4","C4","HC","HCPCS") & mother2_proc$clean_raw_px %in% cpt) | (mother2_proc$RAW_PX_TYPE %in% c("ICD-9-CM","ICD9CM","ICD9","09") & mother2_proc$clean_raw_px %in% icd9_p) | (mother2_proc$RAW_PX_TYPE %in% c("ICD-10-CM","ICD10-PCS","ICD10","10") & mother2_proc$clean_raw_px %in% icd10_p),] #310948 --> 234743

length(unique(mother2_proc$linkage_patid))

#calculate the difference of delivery dates between oneFL and VSBR
mother1_diag$ADMIT_DATE<-as.Date(mother1_diag$ADMIT_DATE,"%Y-%m-%d")
mother2_diag$ADMIT_DATE<-as.Date(mother2_diag$ADMIT_DATE,"%Y-%m-%d")
mother1_proc$PX_DATE<-as.Date(mother1_proc$PX_DATE,"%Y-%m-%d")
mother2_proc$PX_DATE<-as.Date(mother2_proc$PX_DATE,"%Y-%m-%d")

#according to diagnosis codes
mother1_diag_dd<-sqldf("select mother1_diag.linkage_patid,ABS(mother1_diag.ADMIT_DATE - mother1.babydob) as d from mother1_diag left join mother1 on mother1_diag.linkage_patid=mother1.patid;")
mother2_diag_dd<-sqldf("select mother2_diag.linkage_patid,ABS(mother2_diag.ADMIT_DATE - mother2.babydob) as d from mother2_diag left join mother2 on mother2_diag.linkage_patid=mother2.patid;")
#according to procedure codes
mother1_proc_dd<-sqldf("select mother1_proc.linkage_patid,ABS(mother1_proc.PX_DATE - mother1.babydob) as d from mother1_proc left join mother1 on mother1_proc.linkage_patid=mother1.patid;")
mother2_proc_dd<-sqldf("select mother2_proc.linkage_patid,ABS(mother2_proc.PX_DATE - mother2.babydob) as d from mother2_proc left join mother2 on mother2_proc.linkage_patid=mother2.patid;")

#check the distributions
m1_diag_d<-sqldf("select linkage_patid, MIN(d) as min_d from mother1_diag_dd group by linkage_patid;") 
m2_diag_d<-sqldf("select linkage_patid, MIN(d) as min_d from mother2_diag_dd group by linkage_patid;") 
m1_proc_d<-sqldf("select linkage_patid, MIN(d) as min_d from mother1_proc_dd group by linkage_patid;") 
m2_proc_d<-sqldf("select linkage_patid, MIN(d) as min_d from mother2_proc_dd group by linkage_patid;") 

CreateCatTable(vars="min_d",data=m1_diag_d[m1_diag_d$min_d <= 30,])
CreateCatTable(vars="min_d",data=m2_diag_d[m2_diag_d$min_d <= 30,])
CreateCatTable(vars="min_d",data=m1_proc_d[m1_proc_d$min_d <= 30,])
CreateCatTable(vars="min_d",data=m2_proc_d[m2_proc_d$min_d <= 30,])

#Exclude those with inconsistent delivery dates ( > 7 days)
mother1_patid<-unique(c(unique(m1_diag_d$linkage_patid[m1_diag_d$min_d <= 7]),unique(m1_proc_d$linkage_patid[m1_proc_d$min_d <=7]))) 
mother2_patid<-unique(c(unique(m2_diag_d$linkage_patid[m2_diag_d$min_d <= 7]),unique(m2_proc_d$linkage_patid[m2_proc_d$min_d <=7])))

length(unique(c(mother1_patid,mother2_patid)))

linked_patid<-unique(c(mother1_patid,mother2_patid))

save(mother1_patid,file="/Rda/mother1_patid.Rda")
save(mother2_patid,file="/Rda/mother2_patid.Rda")
save(linked_patid,file="/Rda/linked_patid.Rda")

### combine mother1 and mother2 ###

memory.limit(size=200000)
load("/Rda/linked_patid.Rda")
load("/Rda/mother1_patid.Rda")
load("/Rda/mother2_patid.Rda")

mother1id<-linked_patid[linked_patid %in% mother1_patid & !(linked_patid %in% mother2_patid)] 
mother2id<-linked_patid[linked_patid %in% mother2_patid & !(linked_patid %in% mother1_patid)] 
both_id<-linked_patid[(linked_patid %in% mother1_patid) & (linked_patid %in% mother2_patid)]  

# encounter table
mother1_enc<-read.csv("/OneFL/mother1/encounter.csv",stringsAsFactors = F)
mother1_enc<-mother1_enc[mother1_enc$linkage_patid %in% mother1id,] 

mother2_enc<-read.csv("/OneFL/mother2/encounter.csv",stringsAsFactors = F)
mother2_enc<-mother2_enc[mother2_enc$linkage_patid %in% c(mother2id,both_id),] 

encounter<-rbind(mother1_enc,mother2_enc)
length(unique(encounter$linkage_patid))

rm(mother1_enc,mother2_enc)
write.csv(encounter, file="/OneFLVSBRLinkage/Linked_Mother/encounter.csv",row.names = F)

# diagnosis and procedure table
mother1_diag<-fread("/OneFL/mother1/diag_short_v2.csv",header=T, stringsAsFactors = F) 
mother1_diag<-mother1_diag[mother1_diag$linkage_patid %in% mother1id,]
mother2_diag<-fread("/OneFL/mother2/diag_short_v2.csv",header=T, stringsAsFactors = F)
mother2_diag<-mother2_diag[mother2_diag$linkage_patid %in% c(mother2id,both_id),] 

mother1_proc<-fread("/OneFL/mother1/procedures_short.csv",header=T, stringsAsFactors = F)
mother1_proc<-mother1_proc[mother1_proc$linkage_patid %in% mother1id,]
mother2_proc<-fread("/OneFL/mother2/procedures_short.csv",header=T, stringsAsFactors = F)
mother2_proc<-mother2_proc[mother2_proc$linkage_patid %in% c(mother2id,both_id),] 

diagnosis<-rbind(mother1_diag,mother2_diag) #286245
procedure<-rbind(mother1_proc,mother2_proc) #286124
rm(mother1_diag,mother2_diag,mother1_proc,mother2_proc)

#clear blanks/special characters in DX/PX string  
diagnosis$DX<-str_replace_all(diagnosis$DX, "[^[:alnum:]]", "")  
procedure$RAW_PX<-str_replace_all(procedure$RAW_PX, "[^[:alnum:]]", "")  
length(unique(diagnosis$linkage_patid))
length(unique(procedure$linkage_patid))
write.csv(diagnosis, file="/OneFLVSBRLinkage/Linked_Mother/diagnosis.csv",row.names = F)
write.csv(procedure, file="/OneFLVSBRLinkage/Linked_Mother/procedure.csv",row.names = F)

# lab results table
mother1_lab<-read.csv("/OneFL/mother1/lab_result_cm.csv",sep=",",stringsAsFactors = F,row.names=NULL)
varnames<-c("LAB_RESULT_CM_ID",colnames(mother1_lab)[3:28])
mother1_lab<-mother1_lab[,1:27]
colnames(mother1_lab)<-varnames
mother1_lab<-mother1_lab[mother1_lab$linkage_patid %in% mother1id,]

mother2_lab<-read.csv("/OneFL/mother2/lab_result_cm.csv",sep=",",stringsAsFactors = F,row.names=NULL)
mother2_lab<-mother2_lab[,1:27]
colnames(mother2_lab)<-varnames
mother2_lab<-mother2_lab[mother2_lab$linkage_patid %in% c(mother2id,both_id),]

lab_result_cm<-rbind(mother1_lab,mother2_lab)
length(unique(procedure$linkage_patid))
rm(mother1_lab,mother2_lab)

write.csv(lab_result_cm, file="/OneFLVSBRLinkage/Linked_Mother/lab_result_cm.csv",row.names = F)

# enrollment table
mother1_erl<-read.csv("/OneFL/mother1/enrollment.csv",stringsAsFactors = F)
mother2_erl<-read.csv("/OneFL/mother2/enrollment.csv",stringsAsFactors = F)
colnames(mother1_erl)[1]<-"linkage_patid"
colnames(mother2_erl)[1]<-"linkage_patid"
mother1_erl<-unique(mother1_erl)
mother2_erl<-unique(mother2_erl)

mother1_erl<-mother1_erl[order(mother1_erl$linkage_patid),]
mother2_erl<-mother2_erl[order(mother2_erl$linkage_patid),]

mother1_erl$ENR_START_DATE<-as.Date(mother1_erl$ENR_START_DATE,"%Y-%m-%d")
mother1_erl$ENR_END_DATE<-as.Date(mother1_erl$ENR_END_DATE,"%Y-%m-%d")
mother2_erl$ENR_START_DATE<-as.Date(mother2_erl$ENR_START_DATE,"%Y-%m-%d")
mother2_erl$ENR_END_DATE<-as.Date(mother2_erl$ENR_END_DATE,"%Y-%m-%d")

mother1_erl<-mother1_erl[mother1_erl$linkage_patid %in% mother1id,] 
mother2_erl<-mother2_erl[mother2_erl$linkage_patid %in% c(mother2id,both_id),] 

enrollment<-rbind(mother1_erl,mother2_erl)
length(unique(enrollment$linkage_patid))

rm(mother1_erl,mother2_erl)
write.csv(enrollment, file="/OneFLVSBRLinkage/Linked_Mother/enrollment.csv",row.names = F)

# prescribing table
mother1_pres<-read.csv("/OneFL/mother1/prescribing_short.csv",stringsAsFactors = F)
mother2_pres<-read.csv("/OneFL/mother2/prescribing_short.csv",stringsAsFactors = F)
colnames(mother1_pres)

mother1_pres$RX_ORDER_DATE<-as.Date(mother1_pres$RX_ORDER_DATE,"%Y-%m-%d")
mother1_pres$RX_START_DATE<-as.Date(mother1_pres$RX_START_DATE,"%Y-%m-%d")
mother2_pres$RX_ORDER_DATE<-as.Date(mother2_pres$RX_ORDER_DATE,"%Y-%m-%d")
mother2_pres$RX_START_DATE<-as.Date(mother2_pres$RX_START_DATE,"%Y-%m-%d")

mother1_pres<-mother1_pres[mother1_pres$linkage_patid %in% mother1id,] 
mother2_pres<-mother2_pres[mother2_pres$linkage_patid %in% c(mother2id,both_id),]

prescribing<-rbind(mother1_pres,mother2_pres)

length(unique(prescribing$linkage_patid))

write.csv(prescribing, file="/OneFLVSBRLinkage/Linked_Mother/prescribing.csv",row.names = F)
rm(mother1_pres,mother2_pres)

# dispensing table
mother1_disp<-read.csv("/OneFL/mother1/dispensing_short.csv",stringsAsFactors = F)
mother2_disp<-read.csv("/OneFL/mother2/dispensing_short.csv",stringsAsFactors = F)

mother1_disp$DISPENSE_DATE<-as.Date(mother1_disp$DISPENSE_DATE,"%Y-%m-%d")
mother2_disp$DISPENSE_DATE<-as.Date(mother2_disp$DISPENSE_DATE,"%Y-%m-%d")

mother1_disp<-mother1_disp[mother1_disp$linkage_patid %in% mother1id,] 
mother2_disp<-mother2_disp[mother2_disp$linkage_patid %in% c(mother2id,both_id),]

dispensing<-rbind(mother1_disp,mother2_disp)
length(unique(dispensing$linkage_patid))

write.csv(dispensing, file="/OneFLVSBRLinkage/Linked_Mother/dispensing.csv",row.names = F)
rm(mother1_disp,mother2_disp)

# vital table
mother1_vital<-read.csv("/OneFL/mother1/vital.csv",stringsAsFactors = F)
mother1_vital<-mother1_vital[mother1_vital$linkage_patid %in% mother1id,] 
mother1_vital<-mother1_vital[order(mother1_vital$linkage_patid),]

mother2_vital<-read.csv("/OneFL/mother2/vital.csv",stringsAsFactors = F)
mother2_vital<-mother2_vital[mother2_vital$linkage_patid %in% c(mother2id,both_id),]
mother2_vital<-mother2_vital[order(mother2_vital$linkage_patid),]

vital<-rbind(mother1_vital,mother2_vital)
write.csv(vital, file="/OneFLVSBRLinkage/Linked_Mother/vital.csv",row.names = F)
rm(mother1_vital,mother2_vital)


# lds_address_history table
mother1_lds<-read.csv("/Users/zhengyi/OneDrive - University of Florida/1.Dissertation/Data/OneFL/mother1/lds_address_history.csv",stringsAsFactors = F)
mother1_lds<-mother1_lds[mother1_lds$linkage_patid %in% mother1id,] 
mother1_lds<-mother1_lds[order(mother1_lds$linkage_patid),]

mother2_lds<-read.csv("/Users/zhengyi/OneDrive - University of Florida/1.Dissertation/Data/OneFL/mother2/lds_address_history.csv",stringsAsFactors = F)
mother2_lds<-mother2_lds[mother2_lds$linkage_patid %in% c(mother2id,both_id),] 
mother2_lds<-mother2_lds[order(mother2_lds$linkage_patid),]

lds<-rbind(mother1_lds,mother2_lds)
write.csv(lds,file = "/Users/zhengyi/OneDrive - University of Florida/1.Dissertation/Data/csv/lds_address_history.csv")

length(unique(lds$linkage_patid))

# demographic table
mother1_demo<-read.csv("/Users/zhengyi/OneDrive - University of Florida/1.Dissertation/Data/OneFL/mother1/demographic.csv",stringsAsFactors = F)
mother1_demo<-mother1_demo[mother1_demo$linkage_patid %in% mother1id,] 
mother1_demo<-mother1_demo[order(mother1_demo$linkage_patid),]

mother2_demo<-read.csv("/Users/zhengyi/OneDrive - University of Florida/1.Dissertation/Data/OneFL/mother2/demographic.csv",stringsAsFactors = F)
mother2_demo<-mother2_demo[mother2_demo$linkage_patid %in% c(mother2id,both_id),] 
mother2_demo<-mother2_demo[order(mother2_demo$linkage_patid),]

demo<-rbind(mother1_demo,mother2_demo)
write.csv(demo,file = "/Users/zhengyi/OneDrive - University of Florida/1.Dissertation/Data/csv/demographic.csv")

length(unique(demo$linkage_patid))


#Define covariates
load("vsbr1117.Rda")
dat<-vsbr1117[!(vsbr1117$EVENT_YEAR %in% 2011), ]

#mother's age at delivery
dat$age[dat$MOTHER_AGE<20]<-1
dat$age[dat$MOTHER_AGE>=20 & dat$MOTHER_AGE<25]<-2
dat$age[dat$MOTHER_AGE>=25 & dat$MOTHER_AGE<30]<-3
dat$age[dat$MOTHER_AGE>=30 & dat$MOTHER_AGE<35]<-4
dat$age[dat$MOTHER_AGE>=35 & dat$MOTHER_AGE<40]<-5
dat$age[dat$MOTHER_AGE>=40]<-6
#dat$age[dat$age %in% NA]<-99
table(dat$age,useNA = "ifany")

#mother's race/ethnicity (0=non-Hispanic White, 1=non-Hispanic Black, 2=Hispanics, 3=Others )
dat$race<-3
dat$race[!(dat$MOTHER_ETHNIC_YES %in% "Y") & (dat$MOTHER_RACE_WHITE %in% "Y")]<-0
dat$race[!(dat$MOTHER_ETHNIC_YES %in% "Y") & (dat$MOTHER_RACE_BLACK %in% "Y")]<-1
dat$race[(dat$MOTHER_ETHNIC_YES %in% "Y") & (dat$MOTHER_MEXICAN %in% "Y")]<-2
dat$race[(dat$MOTHER_ETHNIC_YES %in% "Y") & (dat$MOTHER_PR %in% "Y")]<-2
dat$race[(dat$MOTHER_ETHNIC_YES %in% "Y") & (dat$MOTHER_CUBAN %in% "Y")]<-2
dat$race[(dat$MOTHER_ETHNIC_YES %in% "Y") & (dat$MOTHER_HAITIAN %in% "Y")]<-2
dat$race[(dat$MOTHER_ETHNIC_UNKNOWN %in% "Y") | (dat$MOTHER_RACE_UNKNOWN %in% "Y")]<-NA
table(dat$race,useNA = "ifany")

#education (1= < high school, 2= high school or equivalent, 3= > high school)
dat$edu<-NA
dat$edu[dat$MOTHER_EDCODE %in% c(1,2)]<-1
dat$edu[dat$MOTHER_EDCODE %in% c(3)]<-2
dat$edu[dat$MOTHER_EDCODE %in% c(4,5,6,7,8)]<-3
#dat$edu[dat$edu %in% NA]<-99
table(dat$edu,useNA = "ifany")

#marital status (1:currently married, 0: currently not married)
#dat$marry[dat$MOTHER_MARRIED %in% "U"]<-99
dat$marry[dat$MOTHER_MARRIED %in% "Y"]<-1
dat$marry[dat$MOTHER_MARRIED %in% c("N","W")]<-0
table(dat$marry,useNA = "ifany")

#pre-pregnancy BMI
dat$MOTHER_HEIGHT_FEET[dat$MOTHER_HEIGHT_FEET %in% 9]<-NA
dat$MOTHER_HEIGHT_INCH[dat$MOTHER_HEIGHT_INCH %in% 99]<-NA
dat$MOTHER_PRE_PREG_WT[dat$MOTHER_PRE_PREG_WT %in% 999]<-NA

dat$pbmi<-NA
dat$pbmi[!(dat$PrePregnancy_BMI %in% "")]<-dat$PrePregnancy_BMI[!(dat$PrePregnancy_BMI %in% "")]+0
dat$pbmi[dat$PrePregnancy_BMI %in% c("",0)]<-dat$MOTHER_PRE_PREG_WT[dat$PrePregnancy_BMI %in% c("",0)]*703/((dat$MOTHER_HEIGHT_FEET[dat$PrePregnancy_BMI %in% c("",0)]*12+dat$MOTHER_HEIGHT_INCH[dat$PrePregnancy_BMI %in% c("",0)])*(dat$MOTHER_HEIGHT_FEET[dat$PrePregnancy_BMI %in% c("",0)]*12+dat$MOTHER_HEIGHT_INCH[dat$PrePregnancy_BMI %in% c("",0)]))

dat$pbmig<-NA
dat$pbmig[dat$pbmi<18.5]<-1
dat$pbmig[dat$pbmi>=18.5 & dat$pbmi<25]<-2
dat$pbmig[dat$pbmi>=25 & dat$pbmi<30]<-3
dat$pbmig[dat$pbmi>=30]<-4
table(dat$pbmig,useNA = "ifany")

#pregnancy smoking (1= No, 2= <10 cigs/day, 3= >=10 cigs/day)
dat$TOBACCO_AVG[dat$TOBACCO_AVG %in% 99]<-NA
dat$TOBACCO_AVG[dat$TOBACCO_USE_YESNO %in% "N"]<-0
dat$smoke[dat$TOBACCO_USE_YESNO %in% "N"]<-1
dat$smoke[dat$TOBACCO_USE_YESNO %in% c("Y") & dat$TOBACCO_AVG <10]<-2
dat$smoke[dat$TOBACCO_USE_YESNO %in% c("Y") & dat$TOBACCO_AVG >=10]<-3
table(dat$smoke,useNA = "ifany")

#prenatal care (1=no care/began in the 3rd trimester, 2=began in the 1st trimester, 3=2nd trimester, 4=had care but no info)
dat$Calc_MonthOfPrenatalBegan[dat$Calc_MonthOfPrenatalBegan %in% 99 ]<-NA
dat$Calc_MonthOfPrenatalBegan[dat$PRENATAL_YESNO %in% "N"]<-0
dat$care[(dat$PRENATAL_YESNO %in% "N") | (dat$Calc_MonthOfPrenatalBegan==0) | (dat$Calc_MonthOfPrenatalBegan>6 & dat$Calc_MonthOfPrenatalBegan<=9)]<-1
dat$care[(dat$PRENATAL_YESNO %in% "Y") & (dat$Calc_MonthOfPrenatalBegan>0) & (dat$Calc_MonthOfPrenatalBegan<=3)]<-2
dat$care[(dat$PRENATAL_YESNO %in% "Y") & (dat$Calc_MonthOfPrenatalBegan>3) & (dat$Calc_MonthOfPrenatalBegan<=6)]<-3
dat$care[(dat$PRENATAL_YESNO %in% "Y") & (dat$Calc_MonthOfPrenatalBegan %in% NA)]<-4
dat$care[dat$care  %in% 99]<-NA
table(dat$care,useNA = "ifany")

#insurance type (1= Medicaid 2=Private Insurance 3=Self-pay 8=other 9=Unknown)
dat$PRINCIPAL_SRCPAY_CODE[dat$PRINCIPAL_SRCPAY_CODE %in% 9]<-NA
table(dat$PRINCIPAL_SRCPAY_CODE,useNA = "ifany")


#Define OUD
diagnosis$clean_dx<-str_replace_all(diagnosis$DX, "[^[:alnum:]]", "") 
procedure$clean_raw_px<-str_replace_all(procedure$RAW_PX, "[^[:alnum:]]", "")  
dispense$clean_ndc<-str_replace_all(dispense$NDC, "[^[:alnum:]]", "") 
prescribe$clean_rxnorm<-str_replace_all(prescribe$RXNORM_CUI, "[^[:alnum:]]", "") 

#OUD DX and Proc CODE

# ICD-9
icd9<-c("304",	"3040",	"30400",	"30401",	"30402","30403",	"3047",	"30470",	"30471",	"30472",	"3055",	"30550",	"30551",	"30552","30553",	"76072",	"9650",	"96500",	"96501",	"96502",	"96509","97010",	"E8500",	"E8501",	"E8502",	"E9350",	"E9351" )
# ICD-10
icd10<-c("F111",	"F1110","F1111",	" F11120",	" F11121",	" F11122",	" F11129",	" F1114",	" F11150",	" F11151",	" F11159",	" F11181",	" F11182",	" F11188",	" F1119",	" F11220",	" F11221",	" F11222",	" F11229",	" F1123",	" F1124",	" F11250",	" F11251",	" F11259",	" F11281",	" F11282",	" F11288",	" F1129",	" F1190",	" F11920",	" F11921",	" F11922",	" F11929",	" F1193",	" F1194",	" F11950",	" F11951",	" F11959",	" F11981",	" F11982",	" F11988",	" F1199",	"T400X1A",	" T400X2A",	" T400X3A",	" T400X4A",	" T401X1A",	" T401X2A",	" T401X3A",	" T401X4A",	" T402X1A",	" T402X2A",	" T402X3A",	" T402X4A",	" T403X1A",	" T403X2A",	" T403X3A",	" T403X4A",	" T403X5A",	"F111",	"F1110",	"F11120",	"F11121",	"F11122",	"F11129",	"F1114",	"F11150",	"F11151",	"F11159",	"F11181",	"F11182",	"F11188",	"F1119",	"F11220",	"F11221",	"F11222",	"F11229",	"F1123",	"F1124",	"F11250",	"F11251",	"F11259",	"F11281",	"F11282",	"F11288",	"F1129",	"F1190",	"F11920",	"F11921",	"F11922",	"F11929",	"F1193",	"F1194",	"F11950",	"F11951",	"F11959",	"F11981",	"F11982",	"F11988",	"F1199",	"T400X1A",	"T400X2A",	"T400X3A",	"T400X4A",	"T401X1A",	"T401X2A",	"T401X3A",	"T401X4A",	"T402X1A",	"T402X2A",	"T402X3A",	"T402X4A",	"T403X1A",	"T403X2A",	"T403X3A",	"T403X4A",	"T403X5A",	"T404X1A",	"T404X2A",	"T404X3A",	"T404X4A",	"T40601A",	"T40602A",	"T40603A",	"T40604A",	"T40691A",	"T40692A",	"T40693A",	"T40694A")

#procedure codes
cpt<-c("J0571",	"J0572",	"J0573",	"J0574",	"J0575",	"S0109",	"J1230",	"J2315")

icd9_p<-c("9464")

icd10_p<-c("HZ81ZZZ",	"HZ84ZZZ",	"HZ85ZZZ",	"HZ86ZZZ",	"HZ91ZZZ",	"HZ94ZZZ",	"HZ95ZZZ",	"HZ96ZZZ")

#NDC for MAT
ndc<-c('58284010014',	'12496127802',	'49999063830',	'12496131002',	'49999063930',	'63874117303',	'00054017613',	'00093537856',	'00228315603',	'00378092393',
       '35356055530',	'50383092493',	'55700030230',	'68308020230',	'00054017713',	'00093537956',	'00228315303',	'00378092493',	'35356055630',	'43063066706',
       '50383093093',	'55700030330',	'68308020830',	'00054453825',	'00002107202',	'00054421725',	'00054421925',	'00002106402',	'00002106404',	'00054421625',
       '00054421825',	'00002168201',	'00002168225',	'00054121811',	'00054121842',	'00054355344',	'00019052705',	'00406052705',	'00406052710',	'00406872510',
       '00406054034',	'49999084130',	'49999084160',	'57866318801',	'57866318802',	'60687020932',	'60687020933',	'00406345434',	'49999084030',	'49999084060',
       '54868285401',	'54868285402',	'57866318701',	'57866318702',	'00406697434',	'54868440800',	'54868440801',	'66479053002',	'66591081551',	'67457021720',
       '61553014940',	'61553014978',	'00054457125',	'00054855411',	'00054855424',	'00378327201',	'00406577101',	'00406577123',	'00406577162',	'00904653060',
       '00904653061',	'10544037808',	'10544037828',	'10544037830',	'10544037860',	'13107008901',	'16590067007',	'16590067045',	'16590067060',	'16590067071',
       '16590067072',	'16590067082',	'16590067083',	'16590067090',	'23490587703',	'23490587706',	'23490587707',	'23490587709',	'35356083401',	'35356083430',
       '35356083460',	'35356083490',	'42549057802',	'42549057808',	'42549057828',	'42549057830',	'42549057856',	'42549057860',	'49999083901',	'49999083930',
       '49999083960',	'49999083990',	'52959038602',	'52959038630',	'52959038660',	'52959038690',	'54868285400',	'54868285403',	'54868494800',	'54868494801',
       '54868494802',	'54868494803',	'54868494805',	'54868494806',	'54868494807',	'55289081430',	'55289081460',	'55289081490',	'55289081493',	'55289081498',
       '55289081499',	'55887020060',	'55887020082',	'55887020090',	'57866707001',	'57866707002',	'57866707003',	'57866707006',	'63629377101',	'63629377102',
       '63629377103',	'63629377104',	'63629377105',	'63629377106',	'63739000610',	'66336017130',	'66336017142',	'66336017156',	'66336017160',	'66336017162',
       '66336017190',	'66336017194',	'66336017198',	'66689081010',	'67877011601',	'68084073801',	'68084073811',	'68115057100',	'00054355663',	'00054854916',
       '66689071216',	'00054039168',	'00054039268',	'00054355367',	'00054355467',	'51079069439',	'64019055367',	'64019055467',	'66689069430',	'66689069439',
       '66689069479',	'66689069579',	'00002215302',	'00054454725',	'00054854725',	'00406254001',	'23490779801',	'23490779803',	'51079089840',	'55887013460',
       '55887013490',	'57866708101',	'57866708102',	'64019053825',	'66689089840',	'68084097732',	'68084097733',	'00054457025',	'00054855311',	'00054855324',
       '00378326001',	'00406575501',	'00406575523',	'00406575562',	'10544037702',	'10544037728',	'13107008801',	'16590068945',	'16590068960',	'16590068972',
       '16590068990',	'23490587801',	'23490587802',	'23490587803',	'23490587809',	'35356083530',	'35356083560',	'35356083590',	'42549057702',	'42549057728',
       '43063022260',	'43063022290',	'43063022293',	'43063022298',	'49999096330',	'49999096360',	'49999096390',	'52959043530',	'54868570100',	'54868570101',
       '54868570102',	'54868570103',	'55887009082',	'55887009090',	'57866395001',	'57866395002',	'57866395003',	'60687021401',	'60687021411',	'63629378801',
       '66336017030',	'66336017060',	'66336017062',	'66336017090',	'66336017094',	'68115057200',	'00054355563',	'00054854816',	'66689071116',	'24200028363',
       '24200028149',	'00019151056',	'00019151057',	'00019151059',	'00019151061',	'00054975085',	'00054975088',	'00406151056',	'00406151057',	'00406151059',
       '38779010403',	'38779010404',	'38779010405',	'49452455301',	'49452455302',	'49452455303',	'51079068155',	'51552072801',	'51552072802',	'51552072804',
       '51552072806',	'51927101700',	'52372079601',	'52372079602',	'52372079603',	'62991140801',	'62991140802',	'62991140803',	'62991140805',	'63275910004',
       '63275910005',	'64019075088',	'66689068155',	'61553012078',	'61553013490',	'24200027503',	'00054364962',	'00054364963',	'59527000150',	'59385001201',
       '59385001230',	'59385001401',	'59385001430',	'59385001601',	'59385001630',	'12496121201',	'12496121203',	'12496120201',	'12496120203',	'12496120401',
       '12496120403',	'12496120801',	'12496120803',	'54569639900',	'55700014730',	'12496128302',	'16590066630',	'49999039507',	'49999039515',	'49999039530',
       '52959074930',	'54569549600',	'54868575000',	'63629402801',	'63874108503',	'68071151003',	'68258299903',	'12496130602',	'35356000407',	'35356000430',
       '43063018407',	'43063018430',	'52959030430',	'54569573900',	'54569573901',	'54569573902',	'54868570700',	'54868570701',	'54868570702',	'54868570703',	
       '54868570704',	'55045378403',	'63629403401',	'63629403402',	'63629403403',	'63874108403',	'66336001630',	'68071138003',	'54123090730',	'54123091430',
       '54123011430',	'54123092930',	'54123095730',	'54123098630',	'00054018813',	'00093572056',	'00228315403',	'00228315473',	'00406192303',	'42291017430',
       '50268014411',	'50268014415',	'50383029493',	'55700018430',	'60429058630',	'65162041603',	'00054018913',	'00093572156',	'00228315503',	'00228315573',
       '00406192403',	'42291017530',	'50268014511',	'50268014515',	'50383028793',	'54569640800',	'60429058730',	'65162041503',	'63459030042',	'65757030001',
       '65757030202',	'00378876793',	'00378876893',	'00406192309',	'00406192409',	'00406800503',	'00406802003',	'00490005100',	'00490005130',	'00490005160',
       '00490005190',	'00781721664',	'00781722764',	'00781723864',	'00781724964',	'16590066605',	'16590066705',	'16590066730',	'16590066790',	'23490927003',
       '23490927006',	'23490927009',	'43598057930',	'43598058030',	'43598058130',	'43598058230',	'47781035503',	'47781035603',	'47781035703',	'47781035803',
       '53217013830',	'53217032801',	'55887031204',	'55887031215',	'60429058633',	'60429058733',	'60846097003',	'60846097103',	'62175045232',	'62175045832',
       '62756096964',	'62756096983',	'62756097064',	'62756097083',	'63629402802',	'63629403404',	'63629507401',	'63629727001',	'63629727002',	'65162041509',
       '65162041609',	'66336001530',	'70518100700',	'70518168400')

#def1: Single dx code -- update DX PROC code before run 
def1<-diagnosis[(diagnosis$DX_TYPE %in% 9 & diagnosis$clean_dx %in% icd9) | (diagnosis$DX_TYPE %in% 10 & diagnosis$clean_dx %in% icd10),]
length(unique(def1$linkage_patid))  
head(def1)

#def2: Single dx code with ENC_TYPE= IP
def2<-diagnosis[(diagnosis$DX_TYPE %in% 9 & diagnosis$clean_dx %in% icd9 & diagnosis$ENC_TYPE %in% c('IP')) | (diagnosis$DX_TYPE %in% 10 & diagnosis$clean_dx %in% icd10 & diagnosis$ENC_TYPE %in% c('IP')),]
length(unique(def2$linkage_patid)) 

#def3:Single dx OR single procedure code
proc_oud<-procedure[(procedure$RAW_PX_TYPE %in% c("CH","CPT-4","CPT4","C4","HC","HCPCS") & procedure$clean_raw_px %in% cpt) | 
                      (procedure$RAW_PX_TYPE %in% c("ICD-9-CM","ICD9CM","ICD9","09") & procedure$clean_raw_px %in% icd9_p) | 
                      (procedure$RAW_PX_TYPE %in% c("ICD-10-CM","ICD10-PCS","ICD10","10") & procedure$clean_raw_px %in% icd10_p),] 
length(unique(proc_oud$linkage_patid))   
def3<-unique(c(def1$linkage_patid,proc_oud$linkage_patid)) 

#def4: multiple dx OR multiple procedure records on different dates 
#Check those with more than 1 diagnosis record: 
n_occur <- data.frame(table(def1$linkage_patid))
n_occur[n_occur$Freq > 1,]
tmp<-def1[def1$linkage_patid %in% n_occur$Var1[n_occur$Freq > 1],]
length(unique(tmp$linkage_patid))#4719 

#Check those with more than 1 OUD diagnoses on different dates
q<-"select count(distinct(ADMIT_DATE)) as num_dx,linkage_patid from tmp group by linkage_patid;"
tmp1<-sqldf(q)

def4_dx<-tmp1[tmp1$num_dx>1,"linkage_patid"]
length(def4_dx) #4153 

#Check those with more than 1 procedure record: 
n_occur <- data.frame(table(proc_oud$linkage_patid))
n_occur[n_occur$Freq > 1,]
tmp<-proc_oud[proc_oud$linkage_patid %in% n_occur$Var1[n_occur$Freq > 1],]
length(unique(tmp$linkage_patid)) #34 

#Check those with more than 1 procedure records on different dates
q<-"select count(distinct(PX_DATE)) as num_dx,linkage_patid from tmp group by linkage_patid;"
tmp1<-sqldf(q)
def4_px<-tmp1[tmp1$num_dx>1,"linkage_patid"] #23 

def4<-unique(c(def4_dx,def4_px)) #4163 

#def 5: single NDC for MAT 
def5<-dispense[dispense$clean_ndc %in% ndc,]
length(unique(def5$linkage_patid)) #1791
head(def5)

## Any single dx OR procedure OR NDC
OUD_all<-unique(c(def1$linkage_patid,proc_oud$linkage_patid,def5$linkage_patid))#7193

save(OUD1,file = "OUD1.Rda")
save(OUD_all,file = "OUD_all.Rda")

# Define onset dates

#Construct PREGNANCY START DATE
#from VSBR--gestational week, babyDOB
q<-"select STATE_FILE_NUMBER, patid, baby_dob, GESTATION_WEEKS from raw_vsbr order by patid;"
m<-sqldf(q)

#calculate Pregnancy start date by subtracting gestational week from baby's DOB
m$baby_dob<-as.Date(m$baby_dob)
m$condate<-m$baby_dob-m$GESTATION_WEEKS*7
m$conyear<-as.numeric(format(m$condate,"%Y"))
m$precondate_threemonth<-as.Date(m$condate-3*30)
m$postp_oneyear<-m$baby_dob+366

#trimesters
m$t1beg<-m$condate
m$t1end<-m$condate+7*13-1
m$t2beg<-m$condate+7*13
m$t2end<-m$condate+7*26-1
m$t3beg<-m$condate+7*26
m$t3end<-m$baby_dob

m$t1end[m$t1end>=m$baby_dob]<-m$baby_dob[m$t1end>=m$baby_dob]
m$t2beg[m$t1end>=m$baby_dob]<-NA
m$t2end[m$t1end>=m$baby_dob]<-NA
m$t3beg[m$t1end>=m$baby_dob]<-NA
m$t3end[m$t1end>=m$baby_dob]<-NA

m$t2end[!is.na(m$t2end) & m$t2end>=m$baby_dob]<-m$baby_dob[!is.na(m$t2end) & m$t2end>=m$baby_dob]
m$t3beg[!is.na(m$t2end) & m$t2end>=m$baby_dob]<-NA
m$t3end[!is.na(m$t2end) & m$t2end>=m$baby_dob]<-NA

# OUD diagnosis by pregnancy timepoints
#DX
# Any single diagnosis -- def1
length(unique(def1$linkage_patid))
colnames(def1)
def1$ADMIT_DATE<-as.Date(def1$ADMIT_DATE)

q<-"select c.patid, min(c.ADMIT_DATE) as first_d, c.condate, c.baby_dob, c.postp_oneyear from
(select a.linkage_patid,a.ADMIT_DATE, b.*  from def1 a left join m b  where a.linkage_patid = b.patid Order by a.linkage_patid) as c
group by c.patid"
t2<-sqldf(q)
t2$first_d<-as.Date(t2$first_d,origin = "1970-01-01")
head(t2,10)
length(unique(t2$patid)) 
#Exclude those with first diagnoses that are earlier than condate, and no later than postp_oneyear 
t2<-t2[t2$first_d >= t2$condate & t2$first_d <= t2$postp_oneyear,] 
length(t2$patid) 

#Any procedures -- proc_oud
length(unique(proc_oud$linkage_patid)) 
proc_oud$PX_DATE<-as.Date(proc_oud$PX_DATE)

q<-"select c.patid, min(c.PX_DATE) as first_d, c.condate, c.baby_dob, c.postp_oneyear  from
(select a.linkage_patid,a.PX_DATE, b.*  from proc_oud a left join m b  where a.linkage_patid = b.patid Order by a.linkage_patid) as c
group by c.patid"
t3<-sqldf(q)
head(t3)
length(unique(t3$patid))
t3$first_d<-as.Date(t3$first_d,origin = "1970-01-01")
# Exclude those with first diagnoses that are earlier than condate
t3<-t3[t3$first_d >= t3$condate  & t3$first_d <= t3$postp_oneyear,] 
length(t3$patid)#80 --> 34

#Any NDC -- def5
length(unique(def5$linkage_patid))
def5$DISPENSE_DATE<-as.Date(def5$DISPENSE_DATE)
q<-"select c.patid, min(c.DISPENSE_DATE) as first_d, c.condate, c.baby_dob,c.postp_oneyear from
(select a.linkage_patid,a.DISPENSE_DATE, b.*  from def5 a left join m b  where a.linkage_patid = b.patid Order by a.linkage_patid) as c
group by c.patid"
t4<-sqldf(q)
head(t4)
length(unique(t4$patid)) 
t4$first_d<-as.Date(t4$first_d,origin = "1970-01-01")
# Exclude those with first diagnoses that are earlier than condate
t4<-t4[t4$first_d >= t4$condate & t4$first_d<= t4$postp_oneyear,] 
length(t4$patid)

tf<-rbind(t2,t3,t4)
t5<-sqldf("select patid, min(first_d) as onset_oud,condate from tf group by patid")
length(unique(tf$patid)) 
t5$onset_oud<-as.Date(t5$onset_oud,origin = "1970-01-01")
head(t5)

m$oud<-0
m$oud[m$patid %in% t5$patid]<-1

m<-sqldf("select m.*, onset_oud from m left join t5 on m.patid = t5.patid;")
head(m,40)

# Change Gestation Weeks = 99 to missing
m$GESTATION_WEEKS[m$GESTATION_WEEKS == 99]<-NA
summary(m$GESTATION_WEEKS)

# Remove individuals with delivery dates before 2012
summary(m$baby_dob)
m<-m %>% filter(m$baby_dob >= as.Date("2012-01-01"))

save(m,file="vsbr_WithOnsetDates.Rda")


### ExWAS analyses ###

#1.engineering the outcome and covariates
load("vsbr_WithOnsetDates.Rda")
vsbr_comple$baby_dob<-as.Date(vsbr_comple$baby_dob,origin = "1970-01-01")

vsbr_comple<-vsbr_comple[vsbr_comple$conyear %in% 2012:2016,]

#oud during pregnancy
table(vsbr_comple$oud,useNA = "ifany")
vsbr_comple$oud_preg<-0
vsbr_comple$oud_preg[vsbr_comple$onset_oud <= vsbr_comple$baby_dob]<-1
table(vsbr_comple$oud_preg,useNA = "ifany")

#covariates double check
load("/Data/Rda/vsbr1117_linked.Rda")
vsbr1117_linked<-vsbr1117_linked[!(vsbr1117_linked$EVENT_YEAR %in% 2011), ]
oud<-merge(vsbr_comple[,c(1,44,37:42,27)],vsbr1117_linked[,c("STATE_FILE_NUMBER","care","PREG_OUTCOME_NUMBER")], by="STATE_FILE_NUMBER", all.x = T)
oud<-oud[oud$PREG_OUTCOME_NUMBER %in% 0,]

oud$oud_preg<-as.factor(oud$oud_preg)
oud$age<-as.factor(oud$age)
oud$race<-as.factor(oud$race)
oud$edu<-as.factor(oud$edu)
oud$pre_bmi<-as.factor(oud$pre_bmi)
oud$marry<-as.factor(oud$marry)
oud$care<-as.factor(oud$care)
oud$smoke<-as.factor(oud$smoke)
oud$PRINCIPAL_SRCPAY_CODE<-as.factor(oud$PRINCIPAL_SRCPAY_CODE)

CreateCatTable(vars = "oud_preg", data = oud,includeNA = T)
table(oud$oud_preg)

## Table 2 ##
vars<-c("age","race","edu","marry","pbmig","smoke","care","PRINCIPAL_SRCPAY_CODE")
table2<-NULL
for (i in 1:8){
  p1<-CreateCatTable(vars = vars[i], strata = "oud_preg", data = oud,includeNA = T)
  p2<-CreateCatTable(vars = vars[i], data = oud,includeNA = T)
  
  c1<-paste0(prettyNum(p1[[1]][[1]]$freq,big.mark = ",")," (",sprintf("%.1f",p1[[1]][[1]]$percent),")")
  c2<-paste0(prettyNum(p1[[2]][[1]]$freq,big.mark = ",")," (",sprintf("%.1f",p1[[2]][[1]]$percent),")")
  c3<-paste0(prettyNum(p2[[1]][[1]]$freq,big.mark = ",")," (",sprintf("%.1f",p2[[1]][[1]]$percent),")")
  
  np<-cbind(OUD=c2,noOUD=c1,Total=c3)
  table2<-rbind(table2,np," ")
}

#2.engineering the exposome vars

#load the vars
load("LinkedExposomeForAdminSuppplement/oud/fara.Rda")
load("LinkedExposomeForAdminSuppplement/oud/hud.Rda")
load("LinkedExposomeForAdminSuppplement/oud/ndi.Rda")
load("LinkedExposomeForAdminSuppplement/oud/ndvi.Rda")
load("LinkedExposomeForAdminSuppplement/oud/ucr.Rda")
load("LinkedExposomeForAdminSuppplement/oud/wi.Rda")
load("LinkedExposomeForAdminSuppplement/oud/zbp.Rda")

#create the labels
acaglabel<-as.data.frame(cbind(var=colnames(acag)[2:8], label=as.vector(c("Black carbon","Ammonium","Nitrate","Organic matter","Sulfate","Mineral dust","Sea salt"))))
acaglabel$category<-"Fine particulate matter compositions"
save(acaglabel,file="LinkedExposomeForAdminSuppplement/oud/label_acag.Rda")

ucrlabel<-as.data.frame(cbind(var=colnames(ucrf)[2:9], label=as.vector(c("Murder rate (per 100 population)","Forcible sex offenses rate (per 100 population)","Robbery rate (per 100 population)","Aggravated assault rate (per 100 population)","Burglary rate (per 100 population)","Larceny rate (per 100 population)","Motor vehicle theft rate (per 100 population)","Total crime rate (per 100 population)"))))
ucrlabel$category<-"Crime and safety"
save(ucrlabel,file="LinkedExposomeForAdminSuppplement/oud/label_ucr.Rda")

zbplabel<-as.data.frame(cbind(var=colnames(zbpf)[2:11], label=as.vector(c("Number of establishments in religious organizations per 10000 population","Number of establishments in civic and social associations per 10000 population","Number of establishments in business associations per 10000 population","Number of establishments in political organizations per 10000 population","Number of establishments in professional organizations per 10000 population","Number of establishments in labor organization per 10000 population","Number of establishments in bowling center per 10000 population","Number of establishments in fitness and recreational sports centers per 10000 population","Number of establishments in golf cources and country clubs per 10000 population","Number of establishments in sports teams and clubs per 10000 population"))))
zbplabel$category<-"Social capital"
save(zbplabel,file="LinkedExposomeForAdminSuppplement/oud/label_zbp.Rda")

# Merge and Transform Vars
oudexp<-merge(vsbr_comple[,c("STATE_FILE_NUMBER","oud_preg")],fara,by="STATE_FILE_NUMBER") #fara 44 vars (13 categorical var) double check the binary variables in HDP paper
oudexp<-merge(oudexp,hudf,by="STATE_FILE_NUMBER") #hudf 19 vars (all continuous)
oudexp<-merge(oudexp,ndif,by="STATE_FILE_NUMBER") #ndif 1 vars (all continuous)
oudexp<-merge(oudexp,ndvi,by="STATE_FILE_NUMBER") #ndvi 1 vars (all continuous)
oudexp<-merge(oudexp,ucrf,by="STATE_FILE_NUMBER") #ucrf 8 vars (all continuous)
oudexp<-merge(oudexp,wi,by="STATE_FILE_NUMBER") #wi 1 var  (all continuous)
oudexp<-merge(oudexp,zbpf,by="STATE_FILE_NUMBER") #zbpf 10 var (all continuous)

save(oudexp,file="oud_duringpreg/oudWithExposures.Rda")
load("oud_duringpreg/oudWithExposures.Rda")

#total: 84 vars, 71 continuous, 13 categorical

#seperate binary vars from continuous vars
binaryvarlist<-c("LILATracts_1And10","LA1and10","LILATracts_halfAnd10","LILATracts_1And20","LAhalfand10","LA1and20","LATracts_half","LATracts1","LATracts10","LATracts20","LATractsVehicle_20","HUNVFlag", "GroupQuartersFlag") #13 vars

oudcon<-oudexp[,colnames(oudexp)[!colnames(oudexp) %in% binaryvarlist]]
oudcat<-oudexp[,c("STATE_FILE_NUMBER",binaryvarlist)]

#transformation (all the continuous vars)
transformation<-data.frame(varname=character(),transformation=character(),lambda=numeric(),a=numeric(),b=numeric(),std=numeric(),stringsAsFactors = F)

for (i in c(3:ncol(oudcon))){
  t1<-Sys.time()
  temp <- bestNormalize(oudcon[,i], out_of_sample = F,allow_orderNorm = F, warn = F)
  #norm stats
  normstats<-as.data.frame(temp$norm_stats)
  colnames(normstats)<-colnames(oudcon)[i]
  save(normstats,file=paste0("oud_duringpreg/transformationExposures/normstats_exp",i,".Rda"))
  
  #chosen transformation and parameters
  ctrans<-rownames(normstats)[normstats[,colnames(oudcon)[i]]==min(normstats[,colnames(oudcon)[i]])]
  if (length(ctrans)>1){
    ctrans <- "no_transform"
  }
  transformation[i,1]<-colnames(oudcon)[i]
  transformation[i,2]<-ctrans
  if (ctrans %in% c("yeojohnson","boxcox")){
    transformation[i,3]<-temp$chosen_transform$lambda
  }
  if (ctrans=="log_x"){
    transformation[i,4]<-temp$chosen_transform$a
    transformation[i,5]<-temp$chosen_transform$b
  }
  
  #transformed values
  if (ctrans != "no_transform"){
    transformation[i,6]<-temp$chosen_transform$sd
    oudtransvar<-as.data.frame(cbind(oudcon$STATE_FILE_NUMBER,temp$x.t))
  } else if (is.null(temp$other_transforms$no_transform$x.t)) {
    transformation[i,6]<-temp$chosen_transform$sd
    oudtransvar<-as.data.frame(cbind(oudcon$STATE_FILE_NUMBER,temp$x.t))
  } else {
    transformation[i,6]<-temp$other_transforms$no_transform$sd
    oudtransvar<-as.data.frame(cbind(oudcon$STATE_FILE_NUMBER,temp$other_transforms$no_transform$x.t))
  }
  colnames(oudtransvar)<-c("STATE_FILE_NUMBER",colnames(oudcon)[i])
  
  save(oudtransvar,file=paste0("oud_duringpreg/transformed/transformed_exp",i,".Rda"))
  t2<-Sys.time()
  print(paste0("var ",i," completed, Took ",as.numeric(t2-t1)," Seconds"))
}
#this file documents the tranformation stat for the exposures #
save(transformation,file=paste0("oud_duringpreg/transformationExposures/transformation_Exposures.Rda"))  
load("oud_duringpreg/transformationExposures/transformation_Exposures.Rda")

#merge normstats
i<-3
load(paste0("oud_duringpreg/transformationExposures/normstats_exp",i,".Rda"))
ns<-normstats
ns$method<-rownames(ns)
rownames(ns)<-NULL
ns<-ns[,c(2,1)]

for (i in 4:73){
  load(paste0("oud_duringpreg/transformationExposures/normstats_exp",i,".Rda"))
  normstats$method<-rownames(normstats)
  rownames(normstats)<-NULL
  ns<-merge(ns,normstats,by="method",all=T)
  print(i)
}

save(ns,file=paste0("oud_duringpreg/transformationExposures/detailnormstats_exp.Rda"))
load(file=paste0("oud_duringpreg/transformationExposures/detailnormstats_exp.Rda"))

#merge all the exposures
oudexptrans<-oudcat
for (i in c(3:73)){
  load(paste0("oud_duringpreg/transformed/transformed_exp",i,".Rda"))
  oudexptrans<-merge(oudexptrans,oudtransvar,by="STATE_FILE_NUMBER",all.x=T)
  print(i)
}
save(oudexptrans,file="oud_duringpreg/oudWithExposuresTransformed.Rda")
load("oud_duringpreg/oudWithExposuresTransformed.Rda")

#check number of unique values for each exposure
res<-data.frame(varname=character(),nuv=numeric(),stringsAsFactors=F)
for (i in 1:ncol(oudexptrans)){
  res[i,1]<-colnames(oudexptrans)[i]
  res[i,2]<-length(unique(oudexptrans[,i]))
  print(i)
}
#"P_PQV_NOSTAT" only have 1 unique value

#check missing
res<-data.frame(varname=character(),nmissing=numeric(),pctmissing=numeric(),stringsAsFactors = F)
for (i in 1:ncol(oudexptrans)){
  res[i,1]<-colnames(oudexptrans)[i]
  res[i,2]<-sum(is.na(oudexptrans[,i]))
  res[i,3]<-sum(is.na(oudexptrans[,i]))/286213
  print(i)
}

#remove variables with missing >10%
navartoexclude<-res$varname[res$pctmissing>0.1] #"P_PQV_NOSTAT"
navartoexclude<-c(navartoexclude,"HUNVFlag") #remove HUNVFlag since it is an uninformative var

#removed 2 variable in total
oudexpfinal<-oudexptrans[,!(colnames(oudexptrans) %in% navartoexclude)]

#make categorical vars factors
oudexpfinal$LILATracts_1And10<-as.factor(oudexpfinal$LILATracts_1And10)
oudexpfinal$LA1and10<-as.factor(oudexpfinal$LA1and10)
oudexpfinal$LILATracts_halfAnd10<-as.factor(oudexpfinal$LILATracts_halfAnd10)
oudexpfinal$LILATracts_1And20<-as.factor(oudexpfinal$LILATracts_1And20)
oudexpfinal$LAhalfand10<-as.factor(oudexpfinal$LAhalfand10)
oudexpfinal$LA1and20<-as.factor(oudexpfinal$LA1and20)
oudexpfinal$LATracts_half<-as.factor(oudexpfinal$LATracts_half)
oudexpfinal$LATracts1<-as.factor(oudexpfinal$LATracts1)
oudexpfinal$LATracts10<-as.factor(oudexpfinal$LATracts10)
oudexpfinal$LATracts20<-as.factor(oudexpfinal$LATracts20)
oudexpfinal$LATractsVehicle_20<-as.factor(oudexpfinal$LATractsVehicle_20)
oudexpfinal$GroupQuartersFlag<-as.factor(oudexpfinal$GroupQuartersFlag)

save(oudexpfinal,file="oud_duringpreg/oudWithExposuresTransformedNAExcluded.Rda")
load("oud_duringpreg/oudWithExposuresTransformedNaExcluded.Rda")

#3.external ExWAS
#imputation
predictormatrix<-quickpred(oud,exclude=c("STATE_FILE_NUMBER"),minpuc = 0.4)
imp<-mice(oud,seed=528,m=1,predictorMatrix = predictormatrix)
oudimp<-complete(imp)
table(oudimp$race,useNA = "ifany")
table(oudimp$edu,useNA = "ifany")
table(oudimp$marry,useNA = "ifany")
table(oudimp$pre_bmi,useNA = "ifany")
table(oudimp$smoke,useNA = "ifany")
table(oudimp$care,useNA = "ifany")
table(oudimp$PRINCIPAL_SRCPAY_CODE,useNA = "ifany")

save(oudimp,file="oud_duringpreg/oudWithOutcomeAndCovariatesImputed.Rda")
load("oud_duringpreg/oudWithOutcomeAndCovariatesImputed.Rda")

#randomly split, 1:1
set.seed(0528) 
sample<-sample.split(oudimp$STATE_FILE_NUMBER,SplitRatio = 0.5) 
train<-subset(oudimp$STATE_FILE_NUMBER,sample==T)
test<-subset(oudimp$STATE_FILE_NUMBER,sample==F)

#ExWAS
workerFunc<-function(i){
  require(speedglm)
  require(mice)
  
  ewas<-function(oudimp,train,test,oudexpfinal) {
    res<-data.frame(var=character(),traine=numeric(),trainstd=numeric(),trainp=numeric(),teste=numeric(),teststd=numeric(),testp=numeric(),nmissing=numeric(),pmissing=numeric(),stringsAsFactors = F)
    
    #merge
    temp<-merge(oudimp,oudexpfinal[,c(1,i)],by="STATE_FILE_NUMBER",all.x=T)
    
    f<-paste0("oud_preg~",colnames(oudexpfinal)[i],"+factor(age)+factor(race)+factor(edu)+factor(marry)+factor(pbmig)+factor(smoke)+factor(care)+factor(PRINCIPAL_SRCPAY_CODE)")
    traindat<-temp[temp$STATE_FILE_NUMBER %in% train,]
    testdat<-temp[temp$STATE_FILE_NUMBER %in% test,]
    trainm<-speedglm(formula = f,data=traindat, family=binomial(link = "logit")) #
    testm<-speedglm(formula = f,data=testdat, family=binomial(link = "logit")) #
    
    res[1,1]<-colnames(oudexpfinal)[i]
    res[1,2]<-as.numeric(as.character(summary(trainm)$coefficients[2,1]))
    res[1,3]<-as.numeric(as.character(summary(trainm)$coefficients[2,2]))
    res[1,4]<-as.numeric(as.character(summary(trainm)$coefficients[2,4]))
    res[1,5]<-as.numeric(as.character(summary(testm)$coefficients[2,1]))
    res[1,6]<-as.numeric(as.character(summary(testm)$coefficients[2,2]))
    res[1,7]<-as.numeric(as.character(summary(testm)$coefficients[2,4]))
    res[1,8]<-as.numeric(sum(is.na(oudexpfinal[,i])))
    res[1,9]<-as.numeric(100*sum(is.na(oudexpfinal[,i]))/nrow(oudexpfinal))
    
    return(res)
  }
  
  res<-ewas(oudimp,train,test,oudexpfinal)
  save(res,file=paste0("oud_duringpreg/modelStats/exwasRes_var",i,".Rda"))
}  

#2:83
for (i in 2:83){
  print(i)
  workerFunc(i)
}


#merge the results
resf<-data.frame(var=character(),traine=numeric(),trainstd=numeric(),trainp=numeric(),teste=numeric(),teststd=numeric(),testp=numeric(),nmissing=numeric(),pmissing=numeric(),stringsAsFactors = F)
for (i in c(2:83)){
  load(paste0("oud_duringpreg/modelStats/exwasRes_var",i,".Rda"))
  resf<-rbind(resf,res)
  print(i)
}

#add transformation info
load(file=paste0("oud_duringpreg/transformationExposures/transformation_Exposures.Rda"))  
resf<-merge(resf,transformation,by.x="var",by.y="varname",all.x=T)

#calculate OR and 95% CIs
resf$trainor<-exp(resf$traine)
resf$testor<-exp(resf$teste)

resf$trainlci<-exp(resf$traine-1.96*resf$trainstd)
resf$trainuci<-exp(resf$traine+1.96*resf$trainstd)

resf$testlci<-exp(resf$teste-1.96*resf$teststd)
resf$testuci<-exp(resf$teste+1.96*resf$teststd)

#add labels
load("LinkedExposomeForAdminSuppplement/oud/label_fara.Rda")
load("LinkedExposomeForAdminSuppplement/oud/label_hud.Rda")
hudlabel$category<-"Address vacancy"
faravarlabel$category<-"Food access"
load("LinkedExposomeForAdminSuppplement/oud/label_ucr.Rda")
load("LinkedExposomeForAdminSuppplement/oud/label_zbp.Rda")

labels<-rbind(hudlabel,faravarlabel,ucrlabel,zbplabel)
labels$var<-as.character(labels$var)
labels$label<-as.character(labels$label)

resf<-merge(resf,labels,by="var",all.x=T)

resf$category[resf$var %in% "wi"]<-"Walkability"
resf$label[resf$var %in% "wi"]<-"National walkability index"

resf$category[resf$var %in% "ndi"]<-"Socioeconomic status"
resf$label[resf$var %in% "ndi"]<-"Neighborhood deprivation index (NDI)"

resf$category[resf$var %in% "ndvi"]<-"Greenness"
resf$label[resf$var %in% "ndvi"]<-"Normalized difference vegetation index (NDVI)"

#control for multiple testing: set tv 0 not sig, 1: sig in train, not in test, 2: sig in test, not in train, 3: both sig
#bonferroni-adjusted p
resf$trainq<-p.adjust(resf$trainp,method="bonferroni")
resf$testq<-p.adjust(resf$testp,method="bonferroni")

resf$tv<-0
resf$tv[(resf$trainq<0.05)& (resf$testq>=0.05)]<-1
resf$tv[(resf$trainq>=0.05)& (resf$testq<0.05)]<-2
resf$tv[(resf$trainq<0.05)& (resf$testq<0.05)]<-3

resf[resf$tv==3,]
res<-resf

#finalize res
res<-res[,c("var","label","category","nmissing","pmissing","transformation","lambda","a","b","std","trainmrr","trainlci","trainuci","trainp","trainq","testmrr","testlci","testuci","testp","testq","tv")]

res<-res[order(res$category),]

save(res,file="oud_duringpreg/modelStats/_exwasRes.Rda")
write.csv(res,file="oud_duringpreg/modelStats/_exwasRes.csv",row.names = F)

#volcano plot
library(ggrepel)

res$tv<-as.factor(res$tv)

table(res$tv)
table(res$category[res$tv %in% 3])
table(res$category,useNA = "ifany")

#colorcat<-c('#0B5345','#229954','#58D68D','#CE93D8','#873600','#4E84C4','#AF601A','#B71C1C')
g1<-ggplot(res,aes(x=trainmrr,y=-log10(trainp)))+geom_point(aes(color=category,shape=tv),alpha=0.75,size=3)+scale_shape_manual(labels=c("Nonsignificant (n=62)","Significant only in Discovery Set (n=4)","Significant only in Replication Set (n=1)","Significant in Both Discovery and Replication Sets (n=15)"),values=c(4,1,0,19))+scale_color_manual(values=c('#0B5345','#229954','#CE93D8','#873600','#4E84C4','#AF601A','#B71C1C'),labels=c("Vacant land (9/18)","Crime and safety (1/8)","Food access (4/43)","Greenness (0/1)","Social capital (0/10)","Socioeconomic status (0/1)","Walkability (1/1)"))+labs(y="-log10(p-value) (Discovery Set)",x="Adjusted Odds Ratio (Discovery Set)",shape="Significance",color="Category (# Significant in Both Sets / # Total)")+theme(panel.background = element_blank(),panel.grid.major = element_line(color="gray",size=0.2),panel.grid.minor = element_line(color="gray",size=0.2),panel.border = element_rect(color = "black",fill=NA,size=0.8),axis.text = element_text(size=12,face="bold"),axis.title = element_text(size=14,face="bold"))

g1+geom_hline(aes(yintercept=-log10(min(res$trainp[as.numeric(as.character(res$tv)) %in% c(0)]))),colour="dodgerblue4",linetype="dashed",size=1)

###################
#mlr without split#
###################
tvfcor<-cor(oudexpfinal[,c(21:92)],use = "pairwise.complete.obs",method="pearson")
tvfcor_melt<-melt(tvfcor)
tvfcor_melt[tvfcor_melt$value>=0.99,]

res$var[res$tv %in% c(3)]
#only keep vars with correlations less than 0.7
temp<-merge(oudimp,oudexpfinal[,c("STATE_FILE_NUMBER","wi","p_burglary","AVG_NOSTAT","P_VAC_3","P_VAC_36","laseniorshalfshare","lakids1share")],by="STATE_FILE_NUMBER",all.x=T)

predictormatrix<-quickpred(temp,exclude=c("STATE_FILE_NUMBER"),minpuc = 0.4 )
imp<-mice(temp,seed=0601,m=1,predictorMatrix = predictormatrix)
temp<-complete(imp)

varmlr<-c("wi","p_burglary","AVG_NOSTAT","P_VAC_3","P_VAC_36","laseniorshalfshare","lakids1share")
f0<-""
for (i in 1:length(varmlr)){
  f0<-paste0(f0,varmlr[i],"+")
}
f<-paste0("oud_preg~",f0,"+factor(age)+factor(race)+factor(edu)+factor(marry)+factor(pre_bmi)+factor(smoke)+factor(care)+factor(PRINCIPAL_SRCPAY_CODE)")

m<-speedglm(formula = f,data=temp, family=binomial(link = "logit"))


resmlr<-as.data.frame(summary(m)$coefficient)
resmlr$var<-rownames(resmlr)
resmlr$mrr<-exp(resmlr$Estimate)
resmlr$lci<-exp(resmlr$Estimate-1.96*resmlr$`Std. Error`)
resmlr$uci<-exp(resmlr$Estimate+1.96*resmlr$`Std. Error`)

colnames(resmlr)[4]<-"mlrp"
resmlr<-resmlr[resmlr$var %in% varmlr,c("var","mrr","lci","uci","mlrp")]

res<-res[res$tv %in% c(3),]
resmlr<-merge(res,resmlr,by="var",all.x=T)

save(resmlr,file="oud_duringpreg/modelStats/_exwasResMLR.Rda")
write.csv(resmlr,file="oud_duringpreg/modelStats/_exwasResMLR.csv",row.names = F)

resmlr$mlrp<-as.numeric(resmlr$mlrp)
resmlr[resmlr$mlrp<0.05,]


#Correlation Heatmap -all significant vars in Phase 1#

#check pairwise correlation
temp<-merge(oudimp,oudexpfinal[,c("STATE_FILE_NUMBER",res$var[res$tv==3])],by="STATE_FILE_NUMBER",all.x=T)
predictormatrix<-quickpred(temp,exclude=c("STATE_FILE_NUMBER"),minpuc = 0.4 )
imp<-mice(temp,seed=706,m=1,predictorMatrix = predictormatrix)
temp<-complete(imp)

tvfcor<-cor(temp[,c(11:25)],use = "pairwise.complete.obs",method="pearson")

tvfcor_melt<-melt(tvfcor)

temp<-res[,c("var","label")]

q<-"select tvfcor_melt.*, temp.label as label1 from tvfcor_melt left outer join temp on tvfcor_melt.Var1=temp.var;"
tvfcor_melt<-sqldf(q)
q<-"select tvfcor_melt.*, temp.label as label2 from tvfcor_melt left outer join temp on tvfcor_melt.Var2=temp.var;"
tvfcor_melt<-sqldf(q)

tvfcor_melt$value<-round(tvfcor_melt$value,2)
tvfcor_melt[tvfcor_melt$value!=1,]

ggplot(tvfcor_melt, aes(label1, label2)) + geom_tile(aes(fill = value))  + scale_fill_gradient2(name = "Correlation\ncoefficient",low = "#67a9cf", high = "#ef8a62") +theme_minimal() +theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank(),  panel.border = element_blank(),panel.background = element_blank(), axis.title = element_blank(), axis.text = element_text(size = 10),axis.text.x = element_text(angle = 90,vjust = 0,hjust=1))
