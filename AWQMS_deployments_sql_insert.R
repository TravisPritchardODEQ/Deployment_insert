library(tidyverse)
library(openxlsx)
library(RODBC)
library(rgdal)
library(lubridate)





#file <- file.choose()
file <- "A:/AWQMS/VOLMON/Deployments/LASAR_Deployments_092218.xlsx"
save_dir <- "//deqlead-lims/SERVERFOLDERS/AWQMS/Continuous/Continuous Data Scripts/Lasar Deployments/"
equipment_sheet = "Equipment from LASAR_CONT"
deployment_sheet = "One Deployment per Site"



# Read deployments sheet from dataset
import_equipment <-openxlsx:: read.xlsx(file, sheet = equipment_sheet)
import_deployments <- openxlsx::read.xlsx(file, sheet = deployment_sheet)

# set startdate and enddate as posixct type
import_deployments$startdate <- convertToDateTime(import_deployments$startdate,origin = "1900-01-01" )
import_deployments$enddate <- convertToDateTime(import_deployments$enddate,origin = "1900-01-01" )


# change the tz to PDT (without changing underlying time) so we can match what's coming in off AWQMS
import_deployments$startdate <- force_tz(import_deployments$startdate, tzone = "america/los_angeles")
import_deployments$enddate <- force_tz(import_deployments$enddate, tzone = "america/los_angeles")

# strip time out of datetime to use with merging to AWQMS query. Used so we can match deployment dates as opposed to 
# datetimes, due to changing start dates when we initially enetered some deployments in AWQMS
import_deployments$startdate_date <- as.Date(import_deployments$startdate, tz ="america/los_angeles" )

import_deployments$EquipID <- as.character(import_deployments$EquipID)

stations <- data.frame(import_deployments$org, import_deployments$Station)
colnames(stations) <- c("org_id", "mloc_id")
stations <- data.frame(lapply(stations, as.character), stringsAsFactors=FALSE)

# import_equipment[is.na(import_equipment)] <- ""
# import_deployments[is.na(import_deployments)] <- ""



# Connect to AWQMS and pull data for duplicate checks  -----------------------------


AWQMS.sql = odbcConnect('AWQMS')

AWQMS_Projects = sqlQuery(AWQMS.sql, "select project.org_uid, project.prj_id, project.prj_name,organization.org_id, organization.org_name
                          From dbo.project
                          Inner Join dbo.organization ON dbo.organization.org_uid=dbo.project.org_uid
                          ;", stringsAsFactors = FALSE) 

AWQMS_Equipment = sqlQuery(AWQMS.sql, "SELECT        organization.org_id, organization.org_name, equipment.eqp_id, equipment.eqp_name, equipment.eqp_comments
FROM            equipment INNER JOIN
                         organization ON equipment.org_uid = organization.org_uid", stringsAsFactors = FALSE)

AWQMS_deployments = sqlQuery(AWQMS.sql, "SELECT        organization.org_id, monitoring_location.mloc_id, equipment.eqp_id, equipment_deployment.eqpdpl_start_time, equipment_deployment.eqpdpl_end_time, CONVERT(date, equipment_deployment.eqpdpl_start_time) 
                         AS startdate, time_zone.tmzone_cd
FROM            organization INNER JOIN
                         monitoring_location ON organization.org_uid = monitoring_location.org_uid INNER JOIN
                         equipment_deployment ON organization.org_uid = equipment_deployment.org_uid AND monitoring_location.mloc_uid = equipment_deployment.mloc_uid INNER JOIN
                         time_zone ON equipment_deployment.tmzone_uid = time_zone.tmzone_uid INNER JOIN
                         equipment ON organization.org_uid = equipment.org_uid AND equipment_deployment.eqp_uid = equipment.eqp_uid", stringsAsFactors = FALSE)

AWQMS_mlocs <- sqlQuery(AWQMS.sql,"SELECT        organization.org_id, organization.org_name, monitoring_location.mloc_id
FROM            organization INNER JOIN
monitoring_location ON organization.org_uid = monitoring_location.org_uid", stringsAsFactors = FALSE)


AWQMS_deploy_proj <-  sqlQuery(AWQMS.sql,"SELECT        equipment_deployment_project_1.eqpdpl_uid AS Expr2, equipment_deployment_project.eqpdpl_uid, equipment_deployment_project_1.prj_uid, project.prj_id, monitoring_location.mloc_id, equipment.eqp_id, organization.org_id, 
                         equipment_deployment.eqpdpl_start_time, equipment_deployment.eqpdpl_start_time AS Expr1, CONVERT(date, equipment_deployment.eqpdpl_start_time) AS startdate
                               FROM            equipment INNER JOIN
                               equipment_deployment ON equipment.eqp_uid = equipment_deployment.eqp_uid INNER JOIN
                               organization ON equipment.org_uid = organization.org_uid AND equipment_deployment.org_uid = organization.org_uid INNER JOIN
                               equipment_deployment_project ON equipment_deployment.eqpdpl_uid = equipment_deployment_project.eqpdpl_uid INNER JOIN
                               project ON organization.org_uid = project.org_uid AND equipment_deployment_project.prj_uid = project.prj_uid INNER JOIN
                               monitoring_location ON equipment_deployment.mloc_uid = monitoring_location.mloc_uid AND organization.org_uid = monitoring_location.org_uid INNER JOIN
                               equipment_deployment_project AS equipment_deployment_project_1 ON equipment_deployment.eqpdpl_uid = equipment_deployment_project_1.eqpdpl_uid AND project.prj_uid = equipment_deployment_project_1.prj_uid",
                               stringsAsFactors = FALSE)


AWQMS_check <- sqlQuery(AWQMS.sql, "SELECT        equipment_deployment.*, equipment_deployment_project.prj_uid
FROM            equipment_deployment left JOIN
                         equipment_deployment_project ON equipment_deployment.eqpdpl_uid = equipment_deployment_project.eqpdpl_uid
Where equipment_deployment_project.prj_uid is null or len(equipment_deployment_project.prj_uid) =0", 
                        stringsAsFactors = FALSE)

odbcClose(AWQMS.sql)




# Stations ----------------------------------------------------------------

# Create a df of stations that are not loaded into AWQMS
Stations_not_AWQMS <- stations %>%
  anti_join(AWQMS_mlocs, by = c("org_id", "mloc_id"))


# IF there are stations not loaded, create a csv with station names
if (nrow(Stations_not_AWQMS) > 0) {
  write.csv(Stations_not_AWQMS, file = paste0(save_dir,"00 - Stations not in AWQMS.csv"), row.names = FALSE) 
} 


# Projects ----------------------------------------------------------------

# Filter datafrane to be only distict org, project1, project2
# Build import string using mutate.
# proj Id is the leftmost 35 characters of proj name
unique_projects <- import_deployments %>%
  distinct(org, Project1, Project2, Project3) %>%
  gather(Project1, Project2, Project3, key = "type", value = "proj", na.rm = TRUE) %>%
  mutate(proj = trimws(proj, which = 'right')) %>%
  distinct(org, proj) %>%
  mutate(proj = gsub("'", "''", proj),
         chars = nchar(proj),
         prj_id = substr(gsub(" ", "", proj, fixed = TRUE), 1, 35)) %>%
  filter(!is.null(proj),
         chars >0)


projects_not_AWQMS <- unique_projects %>%
  anti_join(AWQMS_Projects, by = c("org" = "org_id", "proj" = "prj_id")) 


projects_long <- projects_not_AWQMS %>%
  filter(chars > 35)

projects_sql <- projects_not_AWQMS %>%
  mutate(
    insert = paste0(
      "insert into project (org_uid, sdtyp_uid, usr_uid_create, usr_uid_last_change, prj_id, prj_name, prj_desc, prj_qapp_approved_yn, prj_qapp_approval_agency_name, prj_create_date, prj_last_change_date, prj_wqx_submit_required_yn, prj_wqx_submit_date, prj_beach_yn, evlog_uid_last_change, prj_comments, prj_private_yn, persnl_uid_manager) values ((select org_uid from organization where org_id = '",
      org,
      "'), NULL, NULL, NULL, '",
      prj_id,
      "','",
      proj,
      "', NULL, NULL, NULL, GETDATE(), GETDATE(), 'N', NULL, 'N', NULL, NULL, 'N', NULL)"
    )
  )


# Extract just the insert string
projects <- projects_sql$insert
# Add extra lines at beginning and at end of file
projects <- append(projects, "begin transaction;", after = 0)
projects <- append(projects, "rollback transaction;", after =length(projects))
projects <- append(projects, "commit transaction;;", after =length(projects))

#write txt files with sql extention

if (nrow(projects_not_AWQMS) == 0) {
  write(projects, file = paste0(save_dir, "01 NO NEW PROJECTS - projects.sql"))
} else {
  write(projects, file = paste0(save_dir, "01 - projects.sql"))
}

# Equipment ---------------------------------------------------------------

# Filter equipment table to be only distict org,EquipmentType, ID, Name
# Build import string using mutate.


unique_equipment <- import_equipment %>%
  mutate(ID = as.character(ID),
         Name = as.character(Name)) %>%
  distinct(org, EquipmentType, ID, Name, .keep_all = TRUE) %>%
  anti_join(AWQMS_Equipment, by = c("org" = "org_id", "ID" = "eqp_id")) %>%
  mutate(Comments = gsub("'", "''", Comments)) 

  
  
Equipment_insert <- unique_equipment %>%  
  mutate(insert = paste0("insert into equipment (org_uid, sceqp_uid, eqp_id, eqp_comments, usr_uid_last_change, eqp_last_change_date, eqp_serial_num, eqp_model, eqp_name, eqp_qaqc, eqp_continuous_yn) values((select org_uid from organization where org_id = '",
                         org,"'), (select sceqp_uid from sample_collection_equip where sceqp_name = '",
                         EquipmentType,"'),'",
                         ID, "','",
                         Comments, "',1,getdate(),'','','",
                         Name, "','','Y');"))



if (nrow(Equipment_insert) == 0) {
  write(Equipment_insert$insert,
        file = paste0(save_dir, "02 - NO NEW EQUIPMENT equipment.sql"))
} else {
  write(Equipment_insert$insert,
        file = paste0(save_dir, "02 - equipment.sql"))
}


# Deployments -------------------------------------------------------------

# set startdate form AWQMS query to be type date
AWQMS_deployments <- AWQMS_deployments %>%
  mutate(startdate = ymd(startdate)) 

unique_deployments <- import_deployments %>%
  distinct(org, EquipID,  Project1, Project2, station, startdate, enddate, Media, .keep_all = TRUE) %>%
  anti_join(AWQMS_deployments, by = c("org" = "org_id", "EquipID" = "eqp_id", "Station" = "mloc_id","startdate_date" = "startdate" )) 


deployments_sql <-unique_deployments %>%
  mutate(insert = paste0("insert into equipment_deployment (eqp_uid, org_uid, mloc_uid, eqpdpl_frequency_minutes, eqpdpl_start_time, eqpdpl_end_time, eqpdpl_depth_meters, usr_uid_last_change, eqpdpl_last_change_date, acmed_uid, amsub_uid, tmzone_uid) values ((select eqp_uid from equipment where eqp_id = '",
                         EquipID,"' and org_uid = (select org_uid from organization where org_id = '",
                         org,"')),(select org_uid from organization where org_id = '",
                         org,"'),(select mloc_uid from monitoring_location where mloc_id = '",
                         Station,"' and org_uid = (select org_uid from organization where org_id = '",
                         org,"')),NULL,convert(datetime, replace('",
                         startdate,"','/','-'),121),convert(datetime, replace('",
                         enddate, "','/','-'),121),NULL,1,GETDATE(),(select acmed_uid from activity_media where acmed_name = '",
                         Media,"'),(select amsub_uid from activity_media_subdivision where amsub_name = 'Surface Water'),(select tmzone_uid from time_zone where tmzone_cd = '",
                         TimeZone,"'));"))



if (nrow(unique_deployments) == 0) {
  write(deployments_sql$insert, file = paste0(save_dir,"03 - NO NEW DEPLOYMENTS deployments.sql"))  
} else if (nrow(Stations_not_AWQMS) > 0) {
  write(deployments_sql$insert, file = paste0(save_dir,"03 - ENTER MISSING STATIONS deployments.sql")) 
} else {
write(deployments_sql$insert, file = paste0(save_dir,"03 - deployments.sql"))  
}

# Deployments Projects ----------------------------------------------------
# set startdate form AWQMS query to be type date
AWQMS_deploy_proj <- AWQMS_deploy_proj %>%
  mutate(startdate = ymd(startdate)) 


deployment_projects <- import_deployments %>%
  distinct(org, EquipID,  Project1, Project2, station, startdate, enddate, Media, .keep_all = TRUE) %>%
  gather(Project1, Project2, Project3, key = "type", value = "proj", na.rm = TRUE) %>%
  mutate(proj = trimws(proj, which = 'right')) %>%
  mutate(proj = gsub("'", "''", proj),
         chars = nchar(proj),
         prj_id = substr(gsub(" ", "", proj, fixed = TRUE), 1, 35)) %>%
  filter(!is.na(proj)) %>%
  filter(!is.null(proj),
         chars >0) %>%
  anti_join(AWQMS_deploy_proj, by = c("org" = "org_id", "EquipID" = "eqp_id", 
                                      "Station" = "mloc_id","startdate_date" = "startdate",
                                      "proj" = "prj_id")) %>%
  mutate(insert = paste0("insert into equipment_deployment_project (eqpdpl_uid, prj_uid) values((select eqpdpl_uid from equipment_deployment where org_uid = (select org_uid from organization where org_id = '",
                         org,"') and eqp_uid = (select eqp_uid from equipment where eqp_id = '",
                         EquipID,"' and org_uid = (select org_uid from organization where org_id = '",
                         org,"')) and mloc_uid = (select mloc_uid from monitoring_location where mloc_id = '",
                         Station, "' and org_uid = (select org_uid from organization where org_id = '",
                         org, "')) and eqpdpl_start_time = convert(datetime, replace('",
                         startdate, "','/','-'),121)),(select prj_uid from project where org_uid = (select org_uid from organization where org_id = '",
                         org, "'and (prj_name = '",
                         proj, "' or prj_id = '",
                         prj_id, "'))));"
                         ))
                         


if (nrow(unique_deployments) == 0) {
write(deployment_projects$insert, file = paste0(save_dir,"05 - NO NEW deployment_projects.sql")) 
} else {
  write(deployment_projects$insert, file = paste0(save_dir,"05 - deployment_projects.sql"))   
}
    



    

