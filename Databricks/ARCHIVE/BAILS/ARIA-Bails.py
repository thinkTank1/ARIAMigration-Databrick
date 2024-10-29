# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Bail Cases
# MAGIC
# MAGIC <table style = 'float:left;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Name: </b></td>
# MAGIC          <td>ARIADM_ARM_Bails</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Description: </b></td>
# MAGIC          <td>Notebook to generate a set of HTML, JSON, and A360 files, for Bail caes.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>First Created: </b></td>
# MAGIC          <td>Sep-2024 </td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <th style='text-align: left; '><b>Changelog(JIRA ref/initials./date):</b></th>
# MAGIC          <th>Comments </th>
# MAGIC       </tr>
# MAGIC       </tr>
# MAGIC         <td style='text-align: left;'>
# MAGIC         </b>Create Bronze tables</b>
# MAGIC         </td>
# MAGIC         <td>
# MAGIC         Jira Ticket ARIADM-128</td>
# MAGIC         </td>
# MAGIC       </tr>
# MAGIC     
# MAGIC    </tbody>
# MAGIC </table>
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# run custom functions
import sys
import os
# Append the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..','..')))

import dlt
import json
from pyspark.sql.functions import when, col,coalesce, current_timestamp, lit, date_format, max,date_add
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


# COMMAND ----------

from SharedFunctionsLib.custom_functions import *

# COMMAND ----------

# Setting variables for use in subsequent cells
raw_mnt = "/mnt/ingest00rawsboxraw/ARIADM/ARM/JOH"
landing_mnt = "/mnt/ingest00landingsboxlanding/"
bronze_mnt = "/mnt/ingest00curatedsboxbronze/ARIADM/ARM/JOH"
silver_mnt = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/JOH"
gold_mnt = "/mnt/ingest00curatedsboxgold/ARIADM/ARM/JOH"

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Creating temp views of the raw tables

# COMMAND ----------

# load in all the raw tables

@dlt.table(name="raw_appeal_cases", comment="Raw Appeal Cases",path=f"{raw_mnt}/raw_appeal_cases")
def bail_raw_appeal_cases():
    return read_latest_parquet("AppealCase","tv_AppealCase","ARIA_ARM_BAIL")

@dlt.table(name="raw_case_respondents", comment="Raw Case Respondents",path=f"{raw_mnt}/raw_case_respondents")
def bail_raw_case_respondents():
    return read_latest_parquet("CaseRespondent","tv_CaseRespondent","ARIA_ARM_BAIL")

@dlt.table(name="raw_respondent", comment="Raw Respondents",path=f"{raw_mnt}/raw_respondents")
def bail_raw_respondent():
    return read_latest_parquet("Respondent","tv_Respondent","ARIA_ARM_BAIL")

@dlt.table(name="raw_main_respondent", comment="Raw Main Respondent",path=f"{raw_mnt}/raw_main_respondent")
def bail_raw_main_respondent():
    return read_latest_parquet("MainRespondent","tv_MainRespondent","ARIA_ARM_BAIL")

@dlt.table(name="raw_pou", comment="Raw Pou",path=f"{raw_mnt}/raw_pou")
def bail_raw_pou():
    return read_latest_parquet("Pou","tv_Pou","ARIA_ARM_BAIL")

@dlt.table(name="raw_file_location", comment="Raw File Location",path=f"{raw_mnt}/raw_file_location")
def bail_raw_file_location():
    return read_latest_parquet("FileLocation","tv_FileLocation","ARIA_ARM_BAIL")

@dlt.table(name="raw_case_rep", comment="Raw Case Rep",path=f"{raw_mnt}/raw_case_rep")
def bail_raw_case_rep():
    return read_latest_parquet("CaseRep","tv_CaseRep","ARIA_ARM_BAIL")

@dlt.table(name="raw_representative", comment="Raw Representative",path=f"{raw_mnt}/raw_Representative")
def bail_raw_Representative():
    return read_latest_parquet("Representative","tv_Representative","ARIA_ARM_BAIL")

@dlt.table(name="raw_language", comment="Raw Language",path=f"{raw_mnt}/raw_language")
def bail_raw_language():
    return read_latest_parquet("Language","tv_Language","ARIA_ARM_BAIL")

@dlt.table(name="raw_cost_award", comment="Raw Cost Award",path=f"{raw_mnt}/raw_cost_award")
def bail_raw_cost_award():
    return read_latest_parquet("CostAward","tv_CostAward","ARIA_ARM_BAIL") 

@dlt.table(name='raw_case_list', comment='Raw Case List',path=f"{raw_mnt}/raw_case_list")
def bail_case_list():
    return read_latest_parquet("CaseList","tv_CaseList","ARIA_ARM_BAIL")

@dlt.table(name='raw_hearing_type', comment='Raw Hearing Type',path=f"{raw_mnt}/raw_hearing_type")
def bail_hearing_type():
    return read_latest_parquet("HearingType","tv_HearingType","ARIA_ARM_BAIL")

@dlt.table(name='raw_list',comment='Raw List',path=f"{raw_mnt}/raw_list")
def bail_list():
    return read_latest_parquet("List","tv_List","ARIA_ARM_BAIL")

@dlt.table(name='raw_list_type',comment='Raw List Type',path=f"{raw_mnt}/raw_list_type")
def bail_list_type():
    return read_latest_parquet("ListType","tv_ListType","ARIA_ARM_BAIL")

@dlt.table(name='raw_court',comment='Raw Bail Court',path=f"{raw_mnt}/raw_court")
def bail_court():
    return read_latest_parquet("Court","tv_Court","ARIA_ARM_BAIL")

@dlt.table(name='raw_hearing_centre',comment='Raw  Hearing Centre',path=f"{raw_mnt}/raw_hearing_centre")
def bail_hearing_centre():
    return read_latest_parquet("HearingCentre","tv_HearingCentre","ARIA_ARM_BAIL")

@dlt.table(name='raw_list_sitting',comment='Raw List Sitting',path=f"{raw_mnt}/raw_list_sitting")
def bail_list_sitting():
    return read_latest_parquet("ListSitting","tv_ListSitting","ARIA_ARM_BAIL")

@dlt.table(name='raw_adjudicator',comment='Raw Adjudicator',path=f"{raw_mnt}/raw_adjudicator")
def bail_adjudicator():
    return read_latest_parquet("Adjudicator","tv_Adjudicator","ARIA_ARM_BAIL")

@dlt.table(name='raw_appellant',comment='Raw Bail Appellant',path=f"{raw_mnt}/raw_appellant")
def bail_appellant():
    return read_latest_parquet("Appellant","tv_Appellant","ARIA_ARM_BAIL")

@dlt.table(name='raw_case_appellant',comment='Raw Bail Case Appellant',path=f"{raw_mnt}/raw_case_appellant")
def bail_case_appellant():
    return read_latest_parquet("CaseAppellant","tv_CaseAppellant","ARIA_ARM_BAIL")

@dlt.table(name='raw_detention_centre',comment='Raw Nail Detention Centre',path=f"{raw_mnt}/raw_detention_centre")
def bail_detention_centre():
    return read_latest_parquet("DetentionCentre","tv_DetentionCentre","ARIA_ARM_BAIL")

@dlt.table(name='raw_country',comment='Raw Bail Country',path=f"{raw_mnt}/raw_country")
def bail_country():
    return read_latest_parquet("Country","tv_Country","ARIA_ARM_BAIL")

@dlt.table(name='raw_bf_diary',comment='Raw Bail BF Diary',path=f"{raw_mnt}/raw_bf_diary")
def bail_bf_diary():
    return read_latest_parquet("BFDiary","tv_BFDiary","ARIA_ARM_BAIL")

@dlt.table(name='raw_bf_type',comment='Raw Bail BF Type',path=f"{raw_mnt}/raw_bf_type")
def bail_bf_type():
    return read_latest_parquet("BFType","tv_BFType","ARIA_ARM_BAIL")

@dlt.table(name='raw_history',comment='Raw Bail History',path=f"{raw_mnt}/raw_history")
def bail_history():
    return read_latest_parquet("History","tv_History","ARIA_ARM_BAIL")

@dlt.table(name='raw_users',comment='Raw Bail Users',path=f"{raw_mnt}/raw_users")
def bail_users():
    return read_latest_parquet("Users","tv_Users","ARIA_ARM_BAIL")

@dlt.table(name='raw_link',comment='Raw Bail Link',path=f"{raw_mnt}/raw_link")
def bail_link():
    return read_latest_parquet("Link","tv_Link","ARIA_ARM_BAIL")

@dlt.table(name='raw_link_detail',comment='Raw Bail Link Detail',path=f"{raw_mnt}/raw_link_detail")
def bail_link_detail():
    return read_latest_parquet("LinkDetail","tv_LinkDetail","ARIA_ARM_BAIL")

@dlt.table(name='raw_status',comment='Raw Bail Status',path=f"{raw_mnt}/raw_status")
def bail_status():
    return read_latest_parquet("Status","tv_Status","ARIA_ARM_BAIL")

@dlt.table(name='raw_case_status',comment='Raw Bail Case Status',path=f"{raw_mnt}/raw_case_status")
def bail_case_status():
    return read_latest_parquet("CaseStatus","tv_CaseStatus","ARIA_ARM_BAIL")

@dlt.table(name='raw_status_contact',comment='Raw Bail Status Contact',path=f"{raw_mnt}/raw_status_contact")
def bail_status_contact():
    return read_latest_parquet("StatusContact","tv_StatusContact","ARIA_ARM_BAIL")

@dlt.table(name='raw_reason_adjourn',comment='Raw Bail Reason Adjourn',path=f"{raw_mnt}/raw_reason_adjourn")
def bail_reason_adjourn():
    return read_latest_parquet("ReasonAdjourn","tv_ReasonAdjourn","ARIA_ARM_BAIL")

@dlt.table(name='raw_appeal_category',comment='Raw Bail Appeal Category',path=f"{raw_mnt}/raw_appeal_category")
def bail_appeal_category():
    return read_latest_parquet("AppealCategory","tv_AppealCategory","ARIA_ARM_BAIL")

@dlt.table(name='raw_category',comment='Raw Bail Category',path=f"{raw_mnt}/raw_category")
def bail_category():
    return read_latest_parquet("Category","tv_Category","ARIA_ARM_BAIL")

@dlt.table(name="raw_case_surety",comment="Raw Bail Surety",path=f"{raw_mnt}/raw_case_surety")
def bail_case_surety():
    return read_latest_parquet("CaseSurety","tv_CaseSurety","ARIA_ARM_BAIL")






# COMMAND ----------

# MAGIC %md 
# MAGIC # Creating Bronze Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_bail_ac_cr_cs_ca_fl_cres_mr_res_lang
# MAGIC
# MAGIC SELECT 
# MAGIC -- AppealCase Fields  
# MAGIC ac.CaseNo,  
# MAGIC ac.HORef,  
# MAGIC ac.BailType,  
# MAGIC ac.CourtPreference,  
# MAGIC ac.DateOfIssue,  
# MAGIC ac.DateOfNextListedHearing,  
# MAGIC ac.DateReceived,  
# MAGIC ac.DateServed,  
# MAGIC Ac.Notes AS AppealCaseNote,  
# MAGIC ac.InCamera,  
# MAGIC ac.ProvisionalDestructionDate,  
# MAGIC ac.RemovalDate,  
# MAGIC ac.HOInterpreter,  
# MAGIC ac.Interpreter,  
# MAGIC ac.CountryId,  
# MAGIC -- Case Respondent Fields  
# MAGIC cr.Respondent AS CaseRespondent,  
# MAGIC cr.Reference AS CaseRespondentReference,  
# MAGIC cr.Contact AS CaseRespondentContact,  
# MAGIC -- Respondent Fields  
# MAGIC r.PostalName AS RespondentPostalName,  
# MAGIC r.Department AS RespondentDepartment,  
# MAGIC r.Address1 AS RespondentAddress1,  
# MAGIC r.Address2 AS RespondentAddress2,  
# MAGIC r.Address3 AS RespondentAddress3, 
# MAGIC r.Address4 AS RespondentAddress4,  
# MAGIC r.Address5 AS RespondentAddress5,  
# MAGIC r.Email AS RespondentEmail,  
# MAGIC r.Fax AS RespondentFax,  
# MAGIC r.ShortName AS RespondentShortName,  
# MAGIC r.Telephone AS RespondentTelephone ,  
# MAGIC r.Postcode AS RespondentPostcode, 
# MAGIC --POU  
# MAGIC p.ShortName AS PouShortName, 
# MAGIC p.PostalName AS PouPostalName, 
# MAGIC p.Address1 AS PouAddress1, 
# MAGIC p.Address2 AS PouAddress2, 
# MAGIC p.Address3 AS PouAddress3, 
# MAGIC p.Address4 AS PouAddress4, 
# MAGIC p.Address5 AS PouAddress5, 
# MAGIC p.Postcode AS PouPostcode, 
# MAGIC p.Telephone AS PouTelephone, 
# MAGIC p.Fax AS PouFax, 
# MAGIC p.Email AS PouEMail, 
# MAGIC -- MainRespondent Fields  
# MAGIC mr.Name AS MainRespondentName,  
# MAGIC mr.Embassy AS MainRespondentEmbassy,  
# MAGIC mr.POU AS MainRespondentPOU,  
# MAGIC mr.Respondent AS MainRespondentRespondent,  
# MAGIC -- File Location Fields  
# MAGIC fl.Note AS FileLocationNote,  
# MAGIC fl.TransferDate AS FileLocationTransferDate,  
# MAGIC -- CaseRepresentative Feilds 
# MAGIC crep.Name AS CaseRepName,  
# MAGIC crep.Address1 AS CaseRepAddress1,  
# MAGIC crep.Address2 AS CaseRepAddress2,  
# MAGIC crep.Address3 AS CaseRepAddress3,  
# MAGIC crep.Address4 AS CaseRepAddress4,  
# MAGIC crep.Address5 AS CaseRepAddress5,  
# MAGIC crep.Postcode AS CaseRepPostcode,  
# MAGIC crep.Contact AS CaseRepContact,  
# MAGIC crep.Email AS CaseRepEmail,  
# MAGIC crep.Fax AS CaseRepFax,  
# MAGIC crep.LSCCommission AS CaseRepLSCCommission,  
# MAGIC crep.Telephone AS CaseRepTelephone,  
# MAGIC crep.RepresentativeRef AS CaseRepRepresentativeRef,  
# MAGIC -- Representative Fields  
# MAGIC rep.Address1 AS RepAddress1,  
# MAGIC rep.Address2 AS RepAddress2,  
# MAGIC rep.Address3 AS RepAddress3,  
# MAGIC rep.Address4 AS RepAddress4,  
# MAGIC rep.Address5 AS RepAddress5,  
# MAGIC rep.Name AS RepName,  
# MAGIC rep.DxNo1 AS RepDxNo1,  
# MAGIC rep.DxNo2 AS RepDxNo2,  
# MAGIC rep.Postcode AS RepPostcode,  
# MAGIC rep.Telephone AS RepTelephone,  
# MAGIC rep.Fax AS RepFax,  
# MAGIC rep.Email AS RepEmail,  
# MAGIC -- Language Fields  
# MAGIC l.Description as Language,  
# MAGIC l.DoNotUse as DoNotUseLanguage,  
# MAGIC -- Cost Award Fields  
# MAGIC ca.DateOfApplication,  
# MAGIC ca.TypeOfCostAward, 
# MAGIC ca.ApplyingParty,  
# MAGIC ca.PayingPArty,  
# MAGIC ca.MindedToAward,  
# MAGIC ca.ObjectionToMindedToAward,  
# MAGIC ca.CostsAwardDecision,  
# MAGIC ca.CostsAmount,  
# MAGIC ca.OutcomeOfAppeal,  
# MAGIC ca.AppealStage  
# MAGIC FROM [ARIAREPORTS].[dbo].[AppealCase] ac  
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[CaseRespondent] cr  
# MAGIC ON ac.CaseNo = cr.CaseNo  
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Respondent] r  
# MAGIC ON cr.RespondentId = r.RespondentId  
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Pou] p 
# MAGIC ON cr.RespondentId = p.PouId 
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[MainRespondent] mr  
# MAGIC ON cr.MainRespondentId = mr.MainRespondentId  
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[FileLocation] fl  
# MAGIC ON ac.CaseNo = fl.CaseNo  
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[CaseRep] crep  
# MAGIC ON ac.CaseNo = crep.CaseNo  
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Representative] rep  
# MAGIC ON crep.RepresentativeId = rep.RepresentativeId  
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Language] l  
# MAGIC ON ac.LanguageId = l.LanguageId  
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[CostAward] ca  
# MAGIC ON ac.CaseNo = ca.CaseNo 
# MAGIC  

# COMMAND ----------

@dlt.table(
    name='bronze_bail_ac_cr_cs_ca_fl_cres_mr_res_lang',
    comment='ARIA Migration Archive Bails cases bronze table',
    path=f"{bronze_mnt}/bronze_bail_ac_cr_cs_ca_fl_cres_mr_res_lang"
)
def bronze_bail_ac_cr_cs_ca_fl_cres_mr_res_lang():
    return (
        dlt.read("raw_appeal_cases").alias("ac")
        .join(dlt.read("raw_case_respondents").alias("cr"), col("ac.CaseNo") == col("cr.CaseNo"), 'left_outer')
        .join(dlt.read("raw_respondent").alias("r"), col("cr.RespondentId") == col("r.RespondentId"), 'left_outer')
        .join(dlt.read("raw_pou").alias("p"), col("cr.RespondentId") == col("p.PouId"), 'left_outer')
        .join(dlt.read("raw_main_respondent").alias("mr"), col("cr.MainrespondentId") == col("mr.MainRespondentId"), 'left_outer')
        .join(dlt.read("raw_file_location").alias("fl"), col("ac.CaseNo") == col("fl.CaseNo"), "left_outer")
        .join(dlt.read("raw_case_rep").alias("crep"), col("ac.CaseNo") == col("crep.CaseNo"), "left_outer")
        .join(dlt.read("raw_representative").alias("rep"), col("crep.RepresentativeId") == col("rep.RepresentativeId"), "left_outer")
        .join(dlt.read("raw_language").alias("l"), col("ac.LanguageId") == col("l.LanguageId"), "left_outer")
        .join(dlt.read("raw_cost_award").alias("ca"), col("ac.CaseNo") == col("ca.CaseNo"), "left_outer")
        .select(
            # AppealCase Fields
            col("ac.CaseNo"),
            col("ac.HORef"),
            col("ac.BailType"),
            col("ac.CourtPreference"),
            col("ac.DateOfIssue"),
            col("ac.DateOfNextListedHearing"),
            col("ac.DateReceived"),
            col("ac.DateServed"),
            col("ac.Notes").alias("AppealCaseNote"),
            col("ac.InCamera"),
            col("ac.ProvisionalDestructionDate"),
            col("ac.RemovalDate"),
            col("ac.HOInterpreter"),
            col("ac.Interpreter"),
            col("ac.CountryId"),
            # Case Respondent Fields
            col("cr.Respondent").alias("CaseRespondent"),
            col("cr.Reference").alias("CaseRespondentReference"),
            col("cr.Contact").alias("CaseRespondentContact"),
            # Respondent Fields
            col("r.PostalName").alias("RespondentPostalName"),
            col("r.Department").alias("RespondentDepartment"),
            col("r.Address1").alias("RespondentAddress1"),
            col("r.Address2").alias("RespondentAddress2"),
            col("r.Address3").alias("RespondentAddress3"),
            col("r.Address4").alias("RespondentAddress4"),
            col("r.Address5").alias("RespondentAddress5"),
            col("r.Email").alias("RespondentEmail"),
            col("r.Fax").alias("RespondentFax"),
            col("r.ShortName").alias("RespondentShortName"),
            col("r.Telephone").alias("RespondentTelephone"),
            col("r.Postcode").alias("RespondentPostcode"),
            # POU Fields
            col("p.ShortName").alias("PouShortName"),
            col("p.PostalName").alias("PouPostalName"),
            col("p.Address1").alias("PouAddress1"),
            col("p.Address2").alias("PouAddress2"),
            col("p.Address3").alias("PouAddress3"),
            col("p.Address4").alias("PouAddress4"),
            col("p.Address5").alias("PouAddress5"),
            col("p.Postcode").alias("PouPostcode"),
            col("p.Telephone").alias("PouTelephone"),
            col("p.Fax").alias("PouFax"),
            col("p.Email").alias("PouEmail"),
            # Main Respondent Fields
            col("mr.Name").alias("MainRespondentName"),
            col("mr.Embassy").alias("MainRespondentEmbassy"),
            col("mr.POU").alias("MainRespondentPOU"),
            col("mr.Respondent").alias("MainRespondentRespondent"),
            # File Location Fields
            col("fl.Note").alias("FileLocationNote"),
            col("fl.TransferDate").alias("FileLocationTransferDate"),
            # Case Representative Fields
            col("crep.Name").alias("CaseRepName"),
            col("crep.Address1").alias("CaseRepAddress1"),
            col("crep.Address2").alias("CaseRepAddress2"),
            col("crep.Address3").alias("CaseRepAddress3"),
            col("crep.Address4").alias("CaseRepAddress4"),
            col("crep.Address5").alias("CaseRepAddress5"),
            col("crep.Postcode").alias("CaseRepPostcode"),
            col("crep.Contact").alias("CaseRepContact"),
            col("crep.Email").alias("CaseRepEmail"),
            col("crep.Fax").alias("CaseRepFax"),
            col("crep.LSCCommission").alias("CaseRepLSCCommission"),
            col("crep.Telephone").alias("CaseRepTelephone"),
            col("crep.RepresentativeRef").alias("CaseRepRepresentativeRef"),
            # Representative Fields
            col("rep.Address1").alias("RepAddress1"),
            col("rep.Address2").alias("RepAddress2"),
            col("rep.Address3").alias("RepAddress3"),
            col("rep.Address4").alias("RepAddress4"),
            col("rep.Address5").alias("RepAddress5"),
            col("rep.Name").alias("RepName"),
            col("rep.DxNo1").alias("RepDxNo1"),
            col("rep.DxNo2").alias("RepDxNo2"),
            col("rep.Postcode").alias("RepPostcode"),
            col("rep.Telephone").alias("RepTelephone"),
            col("rep.Fax").alias("RepFax"),
            col("rep.Email").alias("RepEmail"),
            # Language Fields
            col("l.Description").alias("Language"),
            col("l.DoNotUse").alias("DoNotUseLanguage"),
            # Cost Award Fields
            col("ca.DateOfApplication"),
            col("ca.TypeOfCostAward"),
            col("ca.ApplyingParty"),
            col("ca.PayingParty"),
            col("ca.MindedToAward"),
            col("ca.ObjectionToMindedToAward"),
            col("ca.CostsAwardDecision"),
            col("ca.CostsAmount"),
            col("ca.OutcomeOfAppeal"),
            col("ca.AppealStage")
        )

    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_bail_ac_ca_apt_country_detc
# MAGIC
# MAGIC SELECT
# MAGIC -- CaseAppellant Fields
# MAGIC ca.AppellantId,
# MAGIC ca.CaseNo,
# MAGIC ca.Relationship,
# MAGIC -- Appellant Fields
# MAGIC a.PortReference,
# MAGIC a.Name AS AppellantName,
# MAGIC a.Forenames AS AppellantForenames,
# MAGIC a.Title AS AppellantTitle,
# MAGIC a.BirthDate AS AppellantBirthDate,f
# MAGIC a.Address1 AS AppellantAddress1,
# MAGIC a.Address2 AS AppellantAddress2,
# MAGIC a.Address3 AS AppellantAddress3,
# MAGIC a.Address4 AS AppellantAddress4,
# MAGIC a.Address5 AS AppellantAddress5,
# MAGIC a.Postcode AS AppellantPostcode,
# MAGIC a.Telephone AS AppellantTelephone,
# MAGIC a.Fax AS AppellantFax,
# MAGIC a.PrisonRef AS AppellantPrisonRef,
# MAGIC a.Detained AS AppellantDetained,
# MAGIC -- DetentionCentre Fields
# MAGIC dc.Centre AS DetentionCentre,
# MAGIC dc.CentreTitle,
# MAGIC dc.DetentionCentreType,
# MAGIC dc.Address1 AS DetentionCentreAddress1,
# MAGIC dc.Address2 AS DetentionCentreAddress2,
# MAGIC dc.Address3 AS DetentionCentreAddress3,
# MAGIC dc.Address4 AS DetentionCentreAddress4,
# MAGIC dc.Address5 AS DetentionCentreAddress5,
# MAGIC dc.Postcode AS DetentionCentrePoscode,
# MAGIC dc.Fax AS DetentionCentreFax,
# MAGIC -- Country Fields
# MAGIC c.Country,
# MAGIC c.Nationality,
# MAGIC c.Code AS CountryCode,
# MAGIC c.DoNotUse AS DoNotUseCountry,
# MAGIC c.DoNotUseNationality
# MAGIC FROM [ARIAREPORTS].[dbo].[CaseAppellant] ca
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Appellant] a
# MAGIC ON ca.AppellantId = a.AppellantId
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[DetentionCentre] dc
# MAGIC ON a.DetentionCentreId = dc.DetentionCentreId
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Country] c
# MAGIC ON a.AppellantCountryId = c.CountryId

# COMMAND ----------

@dlt.table(name='bronze_bail_ac_ca_apt_country_detc', comment='ARIA Migration Archive Bails cases bronze table',path=f"{silver_mnt}/bronze_bail_ac_cr_cs_ca_fl_cres_mr_res_lang")
def bronze_bail_ac_ca_apt_country_detc():
    return (
        dlt.read("raw_case_appellant").alias("ca")
        .join(dlt.read("raw_appellant").alias("a"), col("ca.AppellantId") == col("a.AppellantId"), "left_outer")
        .join(dlt.read("raw_detention_centre").alias("dc"), col("a.DetentionCentreId") == col("dc.DetentionCentreId"), "left_outer")
        .join(dlt.read("raw_country").alias("c"), col("a.AppellantCountryId") == col("c.CountryId"), "left_outer")
        .select(
            # CaseAppellant Fields
            col("ca.AppellantId"),
            col("ca.CaseNo"),
            col("ca.Relationship"),
            # Appellant Fields
            col("a.PortReference"),
            col("a.Name").alias("AppellantName"),
            col("a.Forenames").alias("AppellantForenames"),
            col("a.Title").alias("AppellantTitle"),
            col("a.BirthDate").alias("AppellantBirthDate"),
            col("a.Address1").alias("AppellantAddress1"),
            col("a.Address2").alias("AppellantAddress2"),
            col("a.Address3").alias("AppellantAddress3"),
            col("a.Address4").alias("AppellantAddress4"),
            col("a.Address5").alias("AppellantAddress5"),
            col("a.Postcode").alias("AppellantPostcode"),
            col("a.Telephone").alias("AppellantTelephone"),
            col("a.Fax").alias("AppellantFax"),
            col("a.PrisonRef").alias("AppellantPrisonRef"),
            col("a.Detained").alias("AppellantDetained"),
            # DetentionCentre Fields
            col("dc.Centre").alias("DetentionCentre"),
            col("dc.CentreTitle"),
            col("dc.DetentionCentreType"),
            col("dc.Address1").alias("DetentionCentreAddress1"),
            col("dc.Address2").alias("DetentionCentreAddress2"),
            col("dc.Address3").alias("DetentionCentreAddress3"),
            col("dc.Address4").alias("DetentionCentreAddress4"),
            col("dc.Address5").alias("DetentionCentreAddress5"),
            col("dc.Postcode").alias("DetentionCentrePostcode"),
            col("dc.Fax").alias("DetentionCentreFax"),
            # Country Fields
            col("c.Country"),
            col("c.Nationality"),
            col("c.Code").alias("CountryCode"),
            col("c.DoNotUse").alias("DoNotUseCountry"),
            col("c.DoNotUseNationality")
    )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_ bail_ac _cl_ht_list_lt_hc_c_ls_adj
# MAGIC
# MAGIC -- Data Mapping
# MAGIC
# MAGIC SELECT 
# MAGIC
# MAGIC     -- Status
# MAGIC     s.CaseNo,
# MAGIC     
# MAGIC     -- CaseList
# MAGIC     cl.TimeEstimate AS CaseListTimeEstimate,
# MAGIC     cl.ListNumber AS CaseListNumber,
# MAGIC     cl.HearingDuration AS CaseListHearingDuration,
# MAGIC     cl.StartTime AS CaseListStartTime,
# MAGIC     
# MAGIC     -- HearingType
# MAGIC     ht.Description AS HearingTypeDesc,
# MAGIC     ht.TimeEstimate AS HearingTypeEst,
# MAGIC     ht.DoNotUse,
# MAGIC     
# MAGIC     -- List
# MAGIC     l.ListName,
# MAGIC     l.StartTime AS ListStartTime,
# MAGIC     
# MAGIC     -- ListType
# MAGIC     lt.Description AS ListTypeDesc,
# MAGIC     lt.ListType,
# MAGIC     lt.DoNotUse AS DoNotUseListType,
# MAGIC     
# MAGIC     -- Court
# MAGIC     c.CourtName,
# MAGIC     c.DoNotUse AS DoNotUseCourt,
# MAGIC     
# MAGIC     -- HearingCentre
# MAGIC     hc.Description AS HearingCentreDesc,
# MAGIC     
# MAGIC     -- ListSitting
# MAGIC     ls.Position AS ListSittingPosition,
# MAGIC     ls.DateBooked AS ListSittingDateBooked,
# MAGIC     ls.LetterDate AS ListSittingLetterDate,
# MAGIC     ls.Cancelled AS ListSittingCancelled,
# MAGIC     ls.UserId,
# MAGIC     ls.Chairman,
# MAGIC     
# MAGIC     -- Adjudicator
# MAGIC     a.Surname AS AdjudicatorSurname,
# MAGIC     a.Forenames AS AdjudicatorForenames,
# MAGIC     a.Notes AS AdjudicatorNote,
# MAGIC     a.Title AS AdjudicatorTitle
# MAGIC
# MAGIC FROM [ARIAREPORTS].[dbo].[Status] s
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[CaseList] cl ON s.StatusId = cl.StatusId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[HearingType] ht ON cl.HearingTypeId = ht.HearingTypeId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[List] l ON cl.ListId = l.ListId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[ListType] lt ON l.ListTypeId = lt.ListTypeId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Court] c ON l.CourtId = c.CourtId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[HearingCentre] hc ON l.CentreId = hc.CentreId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[ListSitting] ls ON l.ListId = ls.ListId
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Adjudicator] a ON ls.AdjudicatorId = a.AdjudicatorId;
# MAGIC

# COMMAND ----------

@dlt.table(
    name="bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj",
    comment="ARIA Migration Archive Bails cases bronze table",
    path=f"{silver_mnt}/bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj"
)
def bronze_bail_ac_cl_ht_list_lt_hc_c_ls_adj():
    return (
        dlt.read("raw_status").alias("s")
        .join(dlt.read("raw_case_list").alias("cl"), col("s.StatusId") == col("cl.StatusId"))
        .join(dlt.read("raw_hearing_type").alias("ht"), col("cl.HearingTypeId") == col("ht.HearingTypeId"), "left_outer")
        .join(dlt.read("raw_list").alias("l"), col("cl.ListId") == col("l.ListId"), "left_outer")
        .join(dlt.read("raw_list_type").alias("lt"), col("l.ListTypeId") == col("lt.ListTypeId"), "left_outer")
        .join(dlt.read("raw_court").alias("c"), col("l.CourtId") == col("c.CourtId"), "left_outer")
        .join(dlt.read("raw_hearing_centre").alias("hc"), col("l.CentreId") == col("hc.CentreId"), "left_outer")
        .join(dlt.read("raw_list_sitting").alias("ls"), col("l.ListId") == col("ls.ListId"), "left_outer")
        .join(dlt.read("raw_adjudicator").alias("adj"), col("ls.AdjudicatorId") == col("adj.AdjudicatorId"), "left_outer")
        .select(
            # Status
            col("s.CaseNo"),
            # CaseList
            col("cl.TimeEstimate").alias("CaseListTimeEstimate"),
            col("cl.ListNumber").alias("CaseListNumber"),
            col("cl.HearingDuration").alias("CaseListHearingDuration"),
            col("cl.StartTime").alias("CaseListStartTime"),
            # HearingType
            col("ht.Description").alias("HearingTypeDesc"),
            col("ht.TimeEstimate").alias("HearingTypeEst"),
            col("ht.DoNotUse"),
            # List
            col("l.ListName"),
            col("l.StartTime").alias("ListStartTime"),
            # ListType
            col("lt.Description").alias("ListTypeDesc"),
            col("lt.ListType"),
            col("lt.DoNotUse").alias("DoNotUseListType"),
            # Court
            col("c.CourtName"),
            col("c.DoNotUse").alias("DoNotUseCourt"),
            # HearingCenter
            col("hc.Description").alias("HearingCentreDesc"),
            # ListSitting
            col("ls.Position").alias("ListSittingPosition"),
            col("ls.DateBooked").alias("ListSittingDateBooked"),
            col("ls.LetterDate").alias("ListSittingLetterDate"),
            col("ls.Cancelled").alias("ListSittingCancelled"),
            col("ls.UserId"),
            col("ls.Chairman"),
            # Adjudicator
            col("adj.Surname").alias("AdjudicatorSurname"),
            col("adj.Forenames").alias("AdjudicatorForenames"),
            col("adj.Notes").alias("AdjudicatorNote"),
            col("adj.Title").alias("AdjudicatorTitle")
        )
        )



# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_bail_ac_bfdiary_bftype
# MAGIC
# MAGIC SELECT
# MAGIC     bfd.CaseNo,
# MAGIC     bfd.BFDate,
# MAGIC     bfd.Entry AS BFDiaryEntry,
# MAGIC     bfd.EntryDate AS BFDiaryEntryDate,
# MAGIC     bfd.DateCompleted,
# MAGIC     bfd.Reason,
# MAGIC     bft.Description AS BFTypeDescription,
# MAGIC     bft.DoNotUse
# MAGIC
# MAGIC FROM [ARIAREPORTS].[dbo].[BFDiary] bfd
# MAGIC
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[BFType] bft
# MAGIC     ON bfd.BFTypeId = bft.BFTypeId;
# MAGIC

# COMMAND ----------

@dlt.table(name="bronze_bail_ac_bfdiary_bftype", comment="ARIA Migration Archive Bails cases bronze table", path=f"{silver_mnt}/bronze_bail_ac_bfdiary_bftype")
def bronze_bail_ac_bfdiary_bftype():
    return (
        dlt.read("raw_bf_diary").alias("bfd")
        .join(dlt.read("raw_bf_type").alias("bft"), col("bfd.BFTypeId") == col("bft.BFTypeId"), "left_outer")
        .select(
            col("bfd.CaseNo"),
            col("bfd.BFDate"),
            col("bfd.Entry").alias("BFDiaryEntry"),
            col("bfd.EntryDate").alias("BFDiaryEntryDate"),
            col("bfd.DateCompleted"),
            col("bfd.Reason"),
            col("bft.Description").alias("BFTypeDescription"),
            col("bft.DoNotUse")
        )
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_ bail_ac _history_users

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT
# MAGIC h.CaseNo,
# MAGIC h.HistoryId,
# MAGIC h.HistDate,
# MAGIC h.HistType,
# MAGIC h.Comment AS HistoryComment,
# MAGIC h.DeletedBy,
# MAGIC h.StatusId,
# MAGIC u.Name AS UserName,
# MAGIC u.UserType,
# MAGIC u.Fullname,
# MAGIC u.Suspended,
# MAGIC u.Extension,
# MAGIC u.DoNotUse
# MAGIC FROM [ARIAREPORTS].[dbo].[History] h
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Users] u
# MAGIC ON h.UserId = u.UserId

# COMMAND ----------

@dlt.table(name="bronze_ bail_ac _history_users", comment="ARIA Migration Archive Bails cases bronze table", path=f"{silver_mnt}/bronze_bail_ac_history_users")
def bronze_bail_ac_history_users():
    return (
        dlt.read("raw_history").alias("h")
        .join(dlt.read("raw_users").alias("u"), col("h.UserId") == col("u.UserId"), "left_outer")
        .select(
            # History table fields
            col("h.CaseNo"),
            col("h.HistoryId"),
            col("h.HistDate"),
            col("h.HistType"),
            col("h.Comment").alias("HistoryComment"),
            col("h.DeletedBy"),
            col("h.StatusId"),
            # Users table fields
            col("u.Name").alias("UserName"),
            col("u.UserType"),
            col("u.Fullname"),
            col("u.Suspended"),
            col("u.Extension"),
            col("u.DoNotUse")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_ bail_ac _link_linkdetail

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC   SELECT
# MAGIC
# MAGIC   l.CaseNo,
# MAGIC
# MAGIC   ld.Comment AS LinkDetailComment
# MAGIC
# MAGIC   FROM [ARIAREPORTS].[dbo].[Link] l
# MAGIC
# MAGIC   LEFT OUTER JOIN [ARIAREPORTS].[dbo].[LinkDetail] ld
# MAGIC   
# MAGIC   ON l.LinkNo = ld.LinkNo

# COMMAND ----------

@dlt.table(name="bronze_ bail_ac _link_linkdetail", comment="ARIA Migration Archive Bails cases bronze table", path=f"{silver_mnt}/bronze_bail_ac_link_linkdetail")
def bronze_bail_ac_link_linkdetail():
    return (
        dlt.read("raw_link").alias("l")
        .join(dlt.read("raw_link_detail").alias("ld"), col("l.LinkNo") == col("ld.LinkNo"), "left_outer")
        .select(
          col("l.CaseNo"),
          col("ld.Comment").alias("LinkDetailComment")
          )
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_bail_status_sc_ra_cs

# COMMAND ----------

@dlt.table(
    name="bronze_bail_status_sc_ra_cs",
    comment="ARIA Migration Archive Bails Status cases bronze table",
    path=f"{silver_mnt}/bronze_bail_status_sc_ra_cs"
)
def bronze_bail_status_sc_ra_cs():
    return (
        dlt.read("raw_status").alias("s")
        .join(dlt.read("raw_case_status").alias("cs"), col("s.CaseStatus") == col("cs.CaseStatusId"), "left_outer")
        .join(dlt.read("raw_status_contact").alias("sc"), col("s.StatusId") == col("sc.StatusId"), "left_outer")
        .join(dlt.read("raw_reason_adjourn").alias("ra"), col("s.ReasonAdjournId") == col("ra.ReasonAdjournId"), "left_outer")
        .join(dlt.read("raw_language").alias("l"), col("s.AdditionalLanguageId") == col("l.LanguageId"), "left_outer")
        .select(
            # Status fields
            col("s.StatusId"),
            col("s.CaseNo"),
            col("s.CaseStatus"),
            col("s.DateReceived"),
            col("s.Keydate"),
            col("s.MiscDate1"),
            col("s.Notes1").alias("StatusNotes1"),
            col("s.MiscDate2"),
            col("s.MiscDate3"),
            col("s.Chairman"),
            col("s.Recognizance"),
            col("s.Security"),
            col("s.Notes2").alias("StatusNotes2"),
            col("s.DecisionDate"),
            col("s.Outcome").alias("OutcomeStatus"),
            col("s.Promulgated"),
            col("s.Party"),
            col("s.ResidenceOrder"),
            col("s.ReportingOrder"),
            col("s.BailedTimePlace"),
            col("s.BaileddateHearing"),
            col("s.InterpreterRequired"),
            col("s.DecisionReserved"),
            col("s.BailConditions"),
            col("s.LivesAndSleepsAt"),
            col("s.AppearBefore"),
            col("s.ReportTo"),
            col("s.AdjournmentParentStatusId"),
            col("s.ListedCentre"),
            col("s.DecisionSentToHO"),
            col("s.DecisionSentToHODate"),
            col("s.VideoLink"),
            col("s.WorkAndStudyRestriction"),
            col("s.Tagging"),
            col("s.OtherCondition"),
            col("s.OutcomeReasons"),
            # CaseStatus fields
            col("cs.Description").alias("CaseStatusDescription"),
            col("cs.DoNotUse").alias("DoNotUseCaseStatus"),
            col("cs.HearingPoints").alias("CaseStatusHearingPoints"),
            # StatusContact fields
            col("sc.Contact").alias("ContactStatus"),
            col("sc.CourtName").alias("SCtContactName"),
            col("sc.Address1").alias("SCAddress1"),
            col("sc.Address2").alias("SCAddress2"),
            col("sc.Address3").alias("SCAddress3"),
            col("sc.Address4").alias("SCAddress4"),
            col("sc.Address5").alias("SCAddress5"),
            col("sc.Postcode").alias("SCPostcode"),
            col("sc.Telephone").alias("SCTelephone"),
            col("sc.Forenames").alias("SCForenames"),
            col("sc.Title").alias("SCTitle"),
            # ReasonAdjourn fields
            col("ra.Reason").alias("ReasonAdjourn"),
            col("ra.DoNotUse").alias("DoNotUseReason"),
            # Language fields
            col("l.Description").alias("LanguageDescription"),
            col("l.DoNotUse").alias("DoNotUseLanguage")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## bronze_ bail_ac _appealcatagory_catagory

# COMMAND ----------

@dlt.table(
    name="bronze_bail_ac_appealcategory_category",
    comment="ARIA Migration Archive Bails Appeal Category cases bronze table",
    path=f"{silver_mnt}/bronze_bail_ac_appealcategory_category"
)
def bronze_bail_ac_appealcategory_category():
    return (
        dlt.read("raw_appeal_category").alias("ap")
        .join(dlt.read("raw_category").alias("c"), col("ap.CategoryId") == col("c.CategoryId"), "left_outer")
        .select(
            # AppealCategory fields
            col("ap.CaseNo"),
            # Category fields
            col("c.Description").alias("CategoryDescription"),
            col("c.Flag"),
            col("c.OnScreen"),
            col("c.FileLabel"),
            col("c.InCase"),
            col("c.InVisitVisa"),
            col("c.InBail"),
            col("c.DoNotShow"),
            col("c.FeeExemption")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## CaseSurety Query

# COMMAND ----------

@dlt.table(
    name="bronze_case_surety_query",
    comment="ARIA Migration Archive Case Surety cases bronze table",
    path=f"{silver_mnt}/bronze_case_surety_query"
)
def bronze_case_surety_query():
    return (
        dlt.read("raw_case_surety").alias("cs")
        .select(
            # CaseSurety fields
            col("SuretyId"),
            col("CaseNo"),
            col("Name").alias("CaseSuretyName"),
            col("Forenames").alias("CaseSuretyForenames"),
            col("Title").alias("CaseSuretyTitle"),
            col("Address1").alias("CaseSuretyAddress1"),
            col("Address2").alias("CaseSuretyAddress2"),
            col("Address3").alias("CaseSuretyAddress3"),
            col("Address4").alias("CaseSuretyAddress4"),
            col("Address5").alias("CaseSuretyAddress5"),
            col("Postcode").alias("CaseSuretyPostcode"),
            col("Recognizance").alias("AmountOfFinancialCondition"),
            col("Security").alias("AmountOfTotalSecurity"),
            col("DateLodged").alias("CaseSuretyDateLodged"),
            col("Location"),
            col("Solicitor"),
            col("Email").alias("CaseSuretyEmail"),
            col("Telephone").alias("CaseSuretyTelephone")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Silver tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normal Bails

# COMMAND ----------

from pyspark.sql import functions as F

@dlt.table(
    name="silver_normal_bail",
    comment="Silver Normal Bail cases table",
    path=f"{silver_mnt}/silver_normal_bail"
)
def silver_normal_bail():
    # Read the necessary raw data
    appeal_case = dlt.read("raw_appeal_cases").alias("ac")
    status = dlt.read("raw_status").alias("s")
    file_location = dlt.read("raw_file_location").alias("fl")
    history = dlt.read("raw_history").alias("h")

    # Create a subquery to get the max StatusId for each CaseNo
    max_status_subquery = (
        status.filter(F.col("CaseStatus").isNull() | (F.col("CaseStatus") != 17))
        .groupBy("CaseNo")
        .agg(F.max("StatusId").alias("max_ID"))
    ).alias("max_status")

    # Join the tables and apply the necessary filters and conditions
    result = (
        appeal_case.alias("ac")
        .join(max_status_subquery, (F.col("ac.CaseNo") == F.col("max_status.CaseNo")), "left_outer")
        .join(status, (F.col("s.CaseNo") == F.col("max_status.CaseNo")) & (F.col("s.StatusId") == F.col("max_status.max_ID")), "left_outer")
        .join(file_location, F.col("ac.CaseNo") == F.col("fl.CaseNo"), "left_outer")
        .join(history, F.col("h.CaseNo") == F.col("ac.CaseNo"), "left_outer")
        .filter(
            (F.col("ac.CaseType") == '2') &
            (F.col("fl.DeptId") != 519) &
            (
                (F.col("fl.Note").isNull()) |
                (~F.col("fl.Note").like('%destroyed%')) &
                (~F.col("fl.Note").like('%detroyed%')) &
                (~F.col("fl.Note").isNull()) &
                (~F.col("fl.Note").like('%distroyed%'))
            )
        )
    )

    # Adding a new column with conditions
    result = result.withColumn(
        "RetentionStatus",
        F.when(F.col("h.Comment").like('%indefinite retention%') | F.col("h.Comment").like('%indefinate retention%'), 'Legal Hold')
        .when(F.date_add(F.col("s.DecisionDate"), 730) < F.current_date(), 'Destroy')
        .otherwise('Archive')
    )

    # Perform aggregation and order the results
    final_result = result.groupBy(F.col("ac.CaseNo")).agg(F.first("RetentionStatus").alias("RetentionStatus")).orderBy(F.col("ac.CaseNo"))

    return final_result


# COMMAND ----------

# MAGIC %md
# MAGIC ## Legal hold normal bail

# COMMAND ----------


@dlt.table(
    name="silver_legal_hold_normal_bail",
    comment="Silver table for legal hold normal bail cases",
    path=f"{silver_mnt}/silver_legal_hold_normal_bail"
)
def silver_legal_hold_normal_bail():
    # Read the necessary raw data
    appeal_case = dlt.read("raw_appeal_cases").alias("ac")
    file_location = dlt.read("raw_file_location").alias("fl")
    history = dlt.read("raw_history").alias("h")

    # Filter and join the data according to the provided SQL logic
    result = (
        appeal_case.alias("ac")
        .join(file_location.alias("fl"), F.col("ac.CaseNo") == F.col("fl.CaseNo"), "left_outer")
        .join(history.alias("h"), F.col("h.CaseNo") == F.col("ac.CaseNo"), "left_outer")
        .filter(
            (F.col("ac.CaseType") == '2') &
            (F.col("fl.DeptId") != 519) &
            (
                F.col("h.Comment").like('%indefinite retention%') |
                F.col("h.Comment").like('%indefinate retention%')
            )
        )
    )

    # Select CaseNo, group by, and aggregate the results
    final_result = (
        result.select(F.col("ac.CaseNo"))
        .groupBy(F.col("ac.CaseNo"))
        .agg(F.count("*").alias("count"))  # Example aggregation
        .orderBy(F.col("ac.CaseNo"))
    )

    return final_result.select("CaseNo")  # Select only the CaseNo for the final result


# COMMAND ----------


