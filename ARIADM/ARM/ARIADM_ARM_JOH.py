# Databricks notebook source
# MAGIC %md
# MAGIC # Judicial Office Holder Archive
# MAGIC <table style = 'float:left;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Name: </b></td>
# MAGIC          <td>ARIADM_ARM_JOH</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Description: </b></td>
# MAGIC          <td>Notebook to create a set of HTML each represenring the historicak data about judges held in ARIA</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>First Created: </b></td>
# MAGIC          <td>Sep-2024 </td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <th style='text-align: left; '><b>Changelog(JIRA ref/initials./date):</b></th>
# MAGIC          <th>Comments </th>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-95">ARIADM-95</a>/NSA/FEB-2023</td>
# MAGIC          <td>JOH :Compete Landing to Bronze Notebook</td>
# MAGIC       </tr>
# MAGIC     
# MAGIC    </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import packages

# COMMAND ----------

import dlt
import json
from pyspark.sql.functions import when, col,coalesce, current_timestamp, lit, date_format
# from pyspark.sql.functions import *
from pyspark.sql.types import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Latest landing files 

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp, lit

# Function to recursively list all files in the ADLS directory
def deep_ls(path: str, depth: int = 0, max_depth: int = 10) -> list:
    """
    Recursively list all files and directories in ADLS directory.
    Returns a list of all paths found.
    """
    output = set()  # Using a set to avoid duplicates
    if depth > max_depth:
        return output

    try:
        children = dbutils.fs.ls(path)
        for child in children:
            if child.path.endswith(".parquet"):
                output.add(child.path.strip())  # Add only .parquet files to the set

            if child.isDir:
                # Recursively explore directories
                output.update(deep_ls(child.path, depth=depth + 1, max_depth=max_depth))

    except Exception as e:
        print(f"Error accessing {path}: {e}")

    return list(output)  # Convert the set back to a list before returning

# Function to extract timestamp from the file path
def extract_timestamp(file_path):
    """
    Extracts timestamp from the parquet file name based on an assumed naming convention.
    """
    # Split the path and get the filename part
    filename = file_path.split('/')[-1]
    # Extract the timestamp part from the filename
    timestamp_str = filename.split('_')[-1].replace('.parquet', '')
    return timestamp_str

# Main function to read the latest parquet file, add audit columns, and return the DataFrame
def read_latest_parquet(folder_name: str, view_name: str, process_name: str, base_path: str = "/mnt/ingest00landingsboxlanding/") -> "DataFrame":
    """
    Reads the latest .parquet file from a specified folder, adds audit columns, creates a temporary Spark view, and returns the DataFrame.
    
    Parameters:
    - folder_name (str): The name of the folder to look for the .parquet files (e.g., "AdjudicatorRole").
    - view_name (str): The name of the temporary view to create (e.g., "tv_AdjudicatorRole").
    - process_name (str): The name of the process adding the audit information (e.g., "ARIA_ARM_JOH").
    - base_path (str): The base path for the folders in the data lake.
    
    Returns:
    - DataFrame: The DataFrame created from the latest .parquet file with added audit columns.
    """
    # Construct the full folder path
    folder_path = f"{base_path}{folder_name}/full/"
    
    # List all .parquet files in the folder
    all_files = deep_ls(folder_path)
    
    # Ensure that files were found
    if not all_files:
        print(f"No .parquet files found in {folder_path}")
        return None
    
    # Find the latest .parquet file
    latest_file = max(all_files, key=extract_timestamp)
    
    # Print the latest file being loaded for logging purposes
    print(f"Reading latest file: {latest_file}")
    
    # Read the latest .parquet file into a DataFrame
    df = spark.read.option("inferSchema", "true").parquet(latest_file)
    
    # Add audit columns
    df = df.withColumn("AdtclmnFirstCreatedDatetime", current_timestamp()) \
           .withColumn("AdtclmnModifiedDatetime", current_timestamp()) \
           .withColumn("SourceFileName", lit(latest_file)) \
           .withColumn("InsertedByProcessName", lit(process_name))
    
    # Create or replace a temporary view
    df.createOrReplaceTempView(view_name)
    
    print(f"Loaded the latest file for {folder_name} into view {view_name} with audit columns")
    
    # Return the DataFrame
    return df



# # read the data from different folders, with audit columns and process name
# df_Adjudicator = read_latest_parquet("Adjudicator", "tv_Adjudicator", "ARIA_ARM_JOH")
# df_HearingCentre = read_latest_parquet("ARIAHearingCentre", "tv_HearingCentre", "ARIA_ARM_JOH")
# df_DoNotUseReason = read_latest_parquet("ARIADoNotUseReason", "tv_DoNotUseReason", "ARIA_ARM_JOH")
# df_EmploymentTerm = read_latest_parquet("EmploymentTerm", "tv_EmploymentTerms", "ARIA_ARM_JOH")
# df_JoHistory = read_latest_parquet("JoHistory", "tv_JoHistory", "ARIA_ARM_JOH")
# df_Users = read_latest_parquet("Users", "tv_Users", "ARIA_ARM_JOH")
# df_OtherCentre = read_latest_parquet("OtherCentre", "tv_OtherCentre", "ARIA_ARM_JOH")
# df_AdjudicatorRole = read_latest_parquet("AdjudicatorRole", "tv_AdjudicatorRole", "ARIA_ARM_JOH")


# COMMAND ----------

raw_mnt = "/mnt/ingest00rawsboxraw"
landing_mnt = "/mnt/ingest00landingsboxlanding"
bronze_mnt = "/mnt/ingest00curatedsboxbronze"
silver_mnt = "/mnt/ingest00curatedsboxsilver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw DLT Tables Creation

# COMMAND ----------

@dlt.table(
    name="raw_adjudicatorrole",
    comment="Delta Live Table ARIA AdjudicatorRole.",
    path=f"{raw_mnt}/ARIA/Raw_AdjudicatorRole"
)
def Raw_AdjudicatorRole():
    return read_latest_parquet("AdjudicatorRole", "tv_AdjudicatorRole", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_adjudicator",
    comment="Delta Live Table ARIA Adjudicator.",
    path=f"{raw_mnt}/ARIA/Raw_Adjudicator"
)
def Raw_Adjudicator():
    return read_latest_parquet("Adjudicator", "tv_Adjudicator", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_employmentterm",
    comment="Delta Live Table ARIA EmploymentTerm.",
    path=f"{raw_mnt}/ARIA/Raw_EmploymentTerm"
)
def Raw_EmploymentTerm():
     return read_latest_parquet("ARIAEmploymentTerm", "tv_EmploymentTerm", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_donotusereason",
    comment="Delta Live Table ARIA DoNotUseReason.",
    path=f"{raw_mnt}/ARIA/Raw_DoNotUseReason"
)
def Raw_DoNotUseReason():
    return read_latest_parquet("DoNotUseReason", "tv_DoNotUseReason", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_johistory",
    comment="Delta Live Table ARIA JoHistory.",
    path=f"{raw_mnt}/ARIA/Raw_JoHistory"
)
def Raw_JoHistory():
    return read_latest_parquet("JoHistory", "tv_JoHistory", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_othercentre",
    comment="Delta Live Table ARIA OtherCentre.",
    path=f"{raw_mnt}/ARIA/Raw_OtherCentre"
)
def Raw_OtherCentre():
    return read_latest_parquet("OtherCentre", "tv_OtherCentre", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_hearingcentre",
    comment="Delta Live Table ARIA HearingCentre.",
    path=f"{raw_mnt}/ARIA/Raw_HearingCentre"
)
def Raw_HearingCentre():
    return read_latest_parquet("ARIAHearingCentre", "tv_HearingCentre", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_users",
    comment="Delta Live Table ARIA Users.",
    path=f"{raw_mnt}/ARIA/Raw_Users"
)
def Raw_Users():
    return read_latest_parquet("Users", "tv_Users", "ARIA_ARM_JOH_ARA")


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Bronze DLT Tables Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation bronze_adjudicator_et_hc_dnur

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC SELECT adj.AdjudicatorId, 
# MAGIC   adj.Surname, 
# MAGIC        adj.Forenames, 
# MAGIC        adj.Title, 
# MAGIC        adj.DateOfBirth, 
# MAGIC        adj.CorrespondenceAddress, 
# MAGIC        adj.ContactTelephone, 
# MAGIC        adj.ContactDetails, 
# MAGIC        adj.AvailableAtShortNotice, 
# MAGIC        hc.Description as DesignatedCentre, 
# MAGIC        et.Description as EmploymentTerms, 
# MAGIC        adj.FullTime, 
# MAGIC        adj.IdentityNumber, 
# MAGIC        adj.DateOfRetirement, 
# MAGIC        adj.ContractEndDate, 
# MAGIC        adj.ContractRenewalDate, 
# MAGIC        dnur.DoNotUse, 
# MAGIC        dnur.Description as DoNotUseReason, 
# MAGIC        adj.JudicialStatus, 
# MAGIC        adj.Address1, 
# MAGIC        adj.Address2, 
# MAGIC        adj.Address3, 
# MAGIC        adj.Address4, 
# MAGIC        adj.Address5, 
# MAGIC        adj.Postcode, 
# MAGIC        adj.Telephone, 
# MAGIC        adj.Mobile, 
# MAGIC        adj.Email, 
# MAGIC        adj.BusinessAddress1, 
# MAGIC        adj.BusinessAddress2, 
# MAGIC        adj.BusinessAddress3, 
# MAGIC        adj.BusinessAddress4, 
# MAGIC        adj.BusinessAddress5, 
# MAGIC        adj.BusinessPostcode, 
# MAGIC        adj.BusinessTelephone, 
# MAGIC        adj.BusinessFax, 
# MAGIC        adj.BusinessEmail, 
# MAGIC        adj.JudicialInstructions, 
# MAGIC        adj.JudicialInstructionsDate, 
# MAGIC        adj.Notes 
# MAGIC FROM [ARIAREPORTS].[dbo].[Adjudicator] adj 
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[HearingCentre] hc 
# MAGIC         ON adj.CentreId = hc.CentreId 
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[EmploymentTerm] et 
# MAGIC         ON adj.EmploymentTerms = et.EmploymentTermId 
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[DoNotUseReason] dnur 
# MAGIC         ON adj.DoNotUseReason = dnur.DoNotUseReasonId 
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import col

@dlt.table(
    name="bronze_adjudicator_et_hc_dnur",
    comment="Delta Live Table combining Adjudicator data with Hearing Centre, Employment Terms, and Do Not Use Reason.",
    path=f"{bronze_mnt}/ARIADM/ARM/bronze_adjudicator_et_hc_dnur"
)
def bronze_adjudicator_et_hc_dnur():
    return (
        dlt.read("raw_adjudicator").alias("adj")
            .join(
                dlt.read("raw_hearingcentre").alias("hc"),
                col("adj.CentreId") == col("hc.CentreId"),
                "left_outer",
            )
            .join(
                dlt.read("raw_employmentterm").alias("et"),
                col("adj.EmploymentTerms") == col("et.EmploymentTermId"),
                "left_outer",
            )
            .join(
                dlt.read("raw_donotusereason").alias("dnur"),
                col("adj.DoNotUseReason") == col("dnur.DoNotUseReasonId"),
                "left_outer",
            )
        .select(
            col("adj.AdjudicatorId"),
            col("adj.Surname"),
            col("adj.Forenames"),
            col("adj.Title"),
            col("adj.DateOfBirth"),
            col("adj.CorrespondenceAddress"),
            col("adj.ContactTelephone"),
            col("adj.ContactDetails"),
            col("adj.AvailableAtShortNotice"),
            col("hc.Description").alias("DesignatedCentre"),
            col("et.Description").alias("EmploymentTerm"),
            col("adj.FullTime"),
            col("adj.IdentityNumber"),
            col("adj.DateOfRetirement"),
            col("adj.ContractEndDate"),
            col("adj.ContractRenewalDate"),
            col("dnur.DoNotUse"),
            col("dnur.Description").alias("DoNotUseReason"),
            col("adj.JudicialStatus"),
            col("adj.Address1"),
            col("adj.Address2"),
            col("adj.Address3"),
            col("adj.Address4"),
            col("adj.Address5"),
            col("adj.Postcode"),
            col("adj.Telephone"),
            col("adj.Mobile"),
            col("adj.Email"),
            col("adj.BusinessAddress1"),
            col("adj.BusinessAddress2"),
            col("adj.BusinessAddress3"),
            col("adj.BusinessAddress4"),
            col("adj.BusinessAddress5"),
            col("adj.BusinessPostcode"),
            col("adj.BusinessTelephone"),
            col("adj.BusinessFax"),
            col("adj.BusinessEmail"),
            col("adj.JudicialInstructions"),
            col("adj.JudicialInstructionsDate"),
            col("adj.Notes"),
            col("adj.AdtclmnFirstCreatedDatetime"),
            col("adj.AdtclmnModifiedDatetime"),
            col("adj.SourceFileName"),
            col("adj.InsertedByProcessName")
        )
    )

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Transformation  bronze_johistory_users 

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC SELECT jh.AdjudicatorId, 
# MAGIC        jh.HistDate, 
# MAGIC        jh.HistType, 
# MAGIC        u.FullName as UserName, 
# MAGIC        jh.Comment 
# MAGIC FROM [ARIAREPORTS].[dbo].[JoHistory] jh 
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Users] u 
# MAGIC         ON jh.UserId = u.UserId 
# MAGIC ```

# COMMAND ----------

@dlt.table(
    name="bronze_johistory_users",
    comment="Delta Live Table combining JoHistory data with Users information.",
    path=f"{bronze_mnt}/ARIADM/ARM/bronze_johistory_users" 
)
def bronze_johistory_users():
    return (
        dlt.read("raw_johistory").alias("joh")
            .join(dlt.read("raw_users").alias("u"), col("joh.UserId") == col("u.UserId"), "left_outer")
            .select(
                col("joh.AdjudicatorId"),
                col("joh.HistDate"),
                col("joh.HistType"),
                col("u.FullName").alias("UserName"),
                col("joh.Comment"),
                col("joh.AdtclmnFirstCreatedDatetime"),
                col("joh.AdtclmnModifiedDatetime"),
                col("joh.SourceFileName"),
                col("joh.InsertedByProcessName")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation bronze_othercentre_hearingcentre 

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC SELECT oc.AdjudicatorId, 
# MAGIC        hc.Description As OtherCentres 
# MAGIC FROM [ARIAREPORTS].[dbo].[OtherCentre] oc 
# MAGIC     LEFT OUTER JOIN [ARIAREPORTS].[dbo].[HearingCentre] hc 
# MAGIC         ON oc.CentreId = hc.CentreId 
# MAGIC ```

# COMMAND ----------

# DLT Table 1: bronze_othercentre_hearingcentre
@dlt.table(
    name="bronze_othercentre_hearingcentre",
    comment="Delta Live Table combining OtherCentre data with HearingCentre information.",
    path=f"{bronze_mnt}/ARIADM/ARM/bronze_othercentre_hearingcentre" 
)
def bronze_othercentre_hearingcentre():
    return (
        dlt.read("raw_othercentre").alias("oc")
        .join(
            dlt.read("raw_hearingcentre").alias("hc"),
            col("hc.CentreId") == col("oc.CentreId"),
            "left_outer",
        )
        .select(
            col("oc.AdjudicatorId"),
            col("hc.Description").alias("OtherCentres"),
            col("oc.AdtclmnFirstCreatedDatetime"),
            col("oc.AdtclmnModifiedDatetime"),
            col("oc.SourceFileName"),
            col("oc.InsertedByProcessName")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation bronze_adjudicatorrole

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC SELECT AdjudicatorId, 
# MAGIC        Role, 
# MAGIC        DateOfAppointment, 
# MAGIC        EndDateOfAppointment 
# MAGIC FROM [ARIAREPORTS].[dbo].[AdjudicatorRole] 
# MAGIC ```

# COMMAND ----------


# DLT Table 2: bronze_adjudicator_role
@dlt.table(
    name="bronze_adjudicator_role",
    comment="Delta Live Table for Adjudicator Role data.",
    path=f"{bronze_mnt}/ARIADM/ARM/bronze_adjudicator_role" 
)
def bronze_adjudicator_role():
    return  (
        dlt.read("raw_adjudicatorrole").alias("adjr")
        .select(
            col("adjr.AdjudicatorId"),
            col("adjr.Role"),
            col("adjr.DateOfAppointment"),
            col("adjr.EndDateOfAppointment"),
            col("adjr.AdtclmnFirstCreatedDatetime"),
            col("adjr.AdtclmnModifiedDatetime"),
            col("adjr.SourceFileName"),
            col("adjr.InsertedByProcessName")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segmentation DLT Tables Creation - stg_joh_filtered
# MAGIC Segmentation query (to be applied in silver): ???
# MAGIC  
# MAGIC ```sql
# MAGIC SELECT a.[AdjudicatorId] 
# MAGIC FROM [ARIAREPORTS].[dbo].[Adjudicator] a 
# MAGIC     LEFT JOIN [ARIAREPORTS].[dbo].[AdjudicatorRole] jr 
# MAGIC         ON jr.AdjudicatorId = a.AdjudicatorId 
# MAGIC WHERE jr.Role NOT IN ( 7, 8 ) 
# MAGIC GROUP BY a.[AdjudicatorId]
# MAGIC ```

# COMMAND ----------

@dlt.table(
    name="stg_joh_filtered",
    comment="Delta Live silver Table segmentation with judges only using bronze_adjudicator_et_hc_dnur.",
    path=f"{silver_mnt}/ARIADM/ARM/stg_joh_filtered"
)
def stg_joh_filtered():
    return (
        dlt.read("bronze_adjudicator_et_hc_dnur").alias("a")
        .join(
            dlt.read("bronze_adjudicator_role").alias("jr"),
            col("a.AdjudicatorId") == col("jr.AdjudicatorId"), 
            "left"
        )
        .filter(~col("jr.Role").isin(7, 8))
        .groupBy(col("a.AdjudicatorId"))
        .count()
        .select(col("a.AdjudicatorId"))
    )


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Silver DLT Tables Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_adjudicator_detail
# MAGIC
# MAGIC

# COMMAND ----------

@dlt.table(
    name="silver_adjudicator_detail",
    comment="Delta Live Silver Table for Adjudicator details enhanced with Hearing Centre and DNUR information.",
    path=f"{silver_mnt}/ARIADM/ARM/silver_adjudicator_detail"
)
def silver_adjudicator_detail():
    return (
        dlt.read("bronze_adjudicator_et_hc_dnur").select(
            col("AdjudicatorId"),
            col("Surname"),
            col("Forenames"),
            col("Title"),
            col("DateOfBirth"),
            when(col("CorrespondenceAddress") == 1, "Business").otherwise("Home").alias("CorrespondenceAddress"),
            col("ContactDetails"),
            when(col("ContactTelephone") == 1, "Business")
                .when(col("ContactTelephone") == 2, "Home")
                .when(col("ContactTelephone") == 3, "Mobile")
                .otherwise(col("ContactTelephone")).alias("ContactTelephone"),
            col("AvailableAtShortNotice"),
            col("DesignatedCentre"),
            col("EmploymentTerm"),
            when(col("FullTime") == 1,'Yes').otherwise("No").alias("FullTime"),
            col("IdentityNumber"),
            col("DateOfRetirement"),
            col("ContractEndDate"),
            col("ContractRenewalDate"),
            col("DoNotUse"),
            col("DoNotUseReason"),
            when(col("JudicialStatus") == 1, "Deputy President")
                .when(col("JudicialStatus") == 2, "Designated Immigration Judge")
                .when(col("JudicialStatus") == 3, "Immigration Judge")
                .when(col("JudicialStatus") == 4, "President")
                .when(col("JudicialStatus") == 5, "Senior Immigration Judge")
                .when(col("JudicialStatus") == 6, "Deputy Senior Immigration Judge")
                .when(col("JudicialStatus") == 7, "Senior President")
                .when(col("JudicialStatus") == 21, "Vice President of the Upper Tribunal IAAC")
                .when(col("JudicialStatus") == 22, "Designated Judge of the First-tier Tribunal")
                .when(col("JudicialStatus") == 23, "Judge of the First-tier Tribunal")
                .when(col("JudicialStatus") == 25, "Upper Tribunal Judge")
                .when(col("JudicialStatus") == 26, "Deputy Judge of the Upper Tribunal")
                .when(col("JudicialStatus") == 28, "Resident Judge of the First-tier Tribunal")
                .when(col("JudicialStatus") == 29, "Principle Resident Judge of the Upper Tribunal")
                .otherwise(col("JudicialStatus")).alias("JudicialStatus"),
            col("Address1"),
            col("Address2"),
            col("Address3"),
            col("Address4"),
            col("Address5"),
            col("Postcode"),
            col("Telephone"),
            col("Mobile"),
            col("Email"),
            col("BusinessAddress1"),
            col("BusinessAddress2"),
            col("BusinessAddress3"),
            col("BusinessAddress4"),
            col("BusinessAddress5"),
            col("BusinessPostcode"),
            col("BusinessTelephone"),
            col("BusinessFax"),
            col("BusinessEmail"),
            col("JudicialInstructions"),
            col("JudicialInstructionsDate"),
            col("Notes"),
            col("AdtclmnFirstCreatedDatetime"),
            col("AdtclmnModifiedDatetime"),
            col("SourceFileName"),
            col("InsertedByProcessName")
        )
    )

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Transformation  silver_history_detail

# COMMAND ----------

@dlt.table(
    name="silver_history_detail",
    comment="Delta Live Silver Table combining JoHistory data with Users information.",
    path=f"{silver_mnt}/ARIADM/ARM/silver_history_detail"
)
def silver_history_detail():
    return (
        dlt.read("bronze_johistory_users").select(
            col('AdjudicatorId'),
            col('HistDate'),
            when(col("HistType") == 1, "Adjournment")
            .when(col("HistType") == 2, "Adjudicator Process")
            .when(col("HistType") == 3, "Bail Process")
            .when(col("HistType") == 4, "Change of Address")
            .when(col("HistType") == 5, "Decisions")
            .when(col("HistType") == 6, "File Location")
            .when(col("HistType") == 7, "Interpreters")
            .when(col("HistType") == 8, "Issue")
            .when(col("HistType") == 9, "Links")
            .when(col("HistType") == 10, "Listing")
            .when(col("HistType") == 11, "SIAC Process")
            .when(col("HistType") == 12, "Superior Court")
            .when(col("HistType") == 13, "Tribunal Process")
            .when(col("HistType") == 14, "Typing")
            .when(col("HistType") == 15, "Parties edited")
            .when(col("HistType") == 16, "Document")
            .when(col("HistType") == 17, "Document Received")
            .when(col("HistType") == 18, "Manual Entry")
            .when(col("HistType") == 19, "Interpreter")
            .when(col("HistType") == 20, "File Detail Changed")
            .when(col("HistType") == 21, "Dedicated hearing centre changed")
            .when(col("HistType") == 22, "File Linking")
            .when(col("HistType") == 23, "Details")
            .when(col("HistType") == 24, "Availability")
            .when(col("HistType") == 25, "Cancel")
            .when(col("HistType") == 26, "De-allocation")
            .when(col("HistType") == 27, "Work Pattern")
            .when(col("HistType") == 28, "Allocation")
            .when(col("HistType") == 29, "De-Listing")
            .when(col("HistType") == 30, "Statutory Closure")
            .when(col("HistType") == 31, "Provisional Destruction Date")
            .when(col("HistType") == 32, "Destruction Date")
            .when(col("HistType") == 33, "Date of Service")
            .when(col("HistType") == 34, "IND Interface")
            .when(col("HistType") == 35, "Address Changed")
            .when(col("HistType") == 36, "Contact Details")
            .when(col("HistType") == 37, "Effective Date")
            .when(col("HistType") == 38, "Centre Changed")
            .when(col("HistType") == 39, "Appraisal Added")
            .when(col("HistType") == 40, "Appraisal Removed")
            .when(col("HistType") == 41, "Costs Deleted")
            .when(col("HistType") == 42, "Credit/Debit Card Payment received")
            .when(col("HistType") == 43, "Bank Transfer Payment received")
            .when(col("HistType") == 44, "Chargeback Taken")
            .when(col("HistType") == 45, "Remission request Rejected")
            .when(col("HistType") == 46, "Refund Event Added")
            .when(col("HistType") == 47, "WriteOff, Strikeout Write-Off or Threshold Write-off Event Added")
            .when(col("HistType") == 48, "Aggregated Payment Taken")
            .when(col("HistType") == 49, "Case Created")
            .when(col("HistType") == 50, "Tracked Document")
            .otherwise(col("HistType")).alias("HistType"),
            col('UserName'),
            col('Comment'),
            col('AdtclmnFirstCreatedDatetime'),
            col('AdtclmnModifiedDatetime'),
            col('SourceFileName'),
            col('InsertedByProcessName'),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_othercentre_detail

# COMMAND ----------

@dlt.table(
    name="silver_othercentre_detail",
    comment="Delta Live silver Table combining OtherCentre data with HearingCentre information.",
    path=f"{silver_mnt}/ARIADM/ARM/silver_othercentre_detail"
)
def silver_othercentre_detail():
    return (dlt.read("bronze_othercentre_hearingcentre"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_appointment_detail

# COMMAND ----------

@dlt.table(
    name="silver_appointment_detail",
    comment="Delta Live Silver Table for Adjudicator Role data.",
    path=f"{silver_mnt}/ARIADM/ARM/silver_appointment_detail"
)
def silver_appointment_detail():
    return (
        dlt.read("bronze_adjudicator_role").select(
            col('AdjudicatorId'),
            when(col("Role") == 2, "Chairman")
            .when(col("Role") == 5, "Adjudicator")
            .when(col("Role") == 6, "Lay Member")
            .when(col("Role") == 7, "Court Clerk")
            .when(col("Role") == 8, "Usher")
            .when(col("Role") == 9, "Qualified Member")
            .when(col("Role") == 10, "Senior Immigration Judge")
            .when(col("Role") == 11, "Immigration Judge")
            .when(col("Role") == 12, "Non Legal Member")
            .when(col("Role") == 13, "Designated Immigration Judge")
            .when(col("Role") == 20, "Upper Tribunal Judge")
            .when(col("Role") == 21, "Judge of the First-tier Tribunal")
            .when(col("Role") == 23, "Designated Judge of the First-tier Tribunal")
            .when(col("Role") == 24, "Upper Tribunal Judge acting As A Judge Of The First-tier Tribunal")
            .when(col("Role") == 25, "Deputy Judge of the Upper Tribunal")
            .otherwise(col("Role")).alias("Role"),
            col('DateOfAppointment'),
            col('EndDateOfAppointment'),
            col('AdtclmnFirstCreatedDatetime'),
            col('AdtclmnModifiedDatetime'),
            col('SourceFileName'),
            col('InsertedByProcessName')
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_archive_metadata
# MAGIC <table style='float:left;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><b>Field</b></td>
# MAGIC          <td style='text-align: left;'><b>Maps to</b></td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>Client Identifier</td>
# MAGIC          <td>IdentityNumber</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>Event Date*</td>
# MAGIC          <td>DateOfRetirement or ContractEndDate, whichever is later. If NULL, use date of migration/generation.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>Record Date*</td>
# MAGIC          <td>Date of migration/generation</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>Region*</td>
# MAGIC          <td>GBR</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>Publisher*</td>
# MAGIC          <td>ARIA</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>Record Class*</td>
# MAGIC          <td>ARIA Judicial Records</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>Entitlement Tag</td>
# MAGIC          <td>IA_Judicial_Office</td>
# MAGIC       </tr>
# MAGIC    </tbody>
# MAGIC </table>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```
# MAGIC * = mandatory field. 
# MAGIC
# MAGIC The following fields will need to be configured as business metadata fields for this record class: 
# MAGIC ```
# MAGIC
# MAGIC <table style='float:left; margin-top: 20px;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left;'><b>Field</b></td>
# MAGIC          <td style='text-align: left;'><b>Type</b></td>
# MAGIC          <td style='text-align: left;'><b>Maps to</b></td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>String</td>
# MAGIC          <td>Title</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>String</td>
# MAGIC          <td>Forenames</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>String</td>
# MAGIC          <td>Surname</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>Date</td>
# MAGIC          <td>DateOfBirth</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>String</td>
# MAGIC          <td>DesignatedCentre</td>
# MAGIC       </tr>
# MAGIC    </tbody>
# MAGIC </table>
# MAGIC
# MAGIC ```
# MAGIC Please note: 
# MAGIC the bf_xxx indexes may change while being finalised with Through Technology 
# MAGIC Dates must be provided in Zulu time format ```
# MAGIC

# COMMAND ----------

@dlt.table(
    name="silver_archive_metadata",
    comment="Delta Live Silver Table for Archive Metadata data.",
    path=f"{silver_mnt}/ARIADM/ARM/silver_archive_metadata"
)
def silver_archive_metadata():
    return (
        dlt.read("silver_adjudicator_detail").select(
            col('AdjudicatorId').alias('client_identifier'),
            date_format(coalesce(col('DateOfRetirement'), col('ContractEndDate'), col('AdtclmnFirstCreatedDatetime')), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_date"),
            date_format(col('AdtclmnFirstCreatedDatetime'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("recordDate"),
            lit("GBR").alias("region"),
            lit("ARIA").alias("publisher"),
            lit("ARIA Judicial Records").alias("record_class"),
            lit('IA_Judicial_Office').alias("entitlement_tag"),
            col('Title').alias('bf_title'),
            col('Forenames').alias('bf_forename'),
            col('Surname').alias('bf_surname'),
            col('DateOfBirth').alias('bf_dateofbirth'),
            col('DesignatedCentre').alias('bf_designatedcentre')
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate HTML

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function Overview
# MAGIC This section details the function for generating HTML files and includes a sample execution.

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime

# Define the function to generate HTML for a given adjudicator
def generate_html_for_adjudicator(adjudicator_id, df_judicial_officer_details, df_other_centres, df_roles,df_history):
    # Step 1: Query the judicial officer details
    try:
        judicial_officer_details = df_judicial_officer_details.filter(col('AdjudicatorId') == adjudicator_id).collect()[0]
    except IndexError:
        print(f"No details found for Adjudicator ID: {adjudicator_id}")
        return

    # Step 2: Query the other centre details
    other_centres = df_other_centres.select('OtherCentres').filter(col('AdjudicatorId') == adjudicator_id).collect()

    # Step 3: Query the role details
    roles = df_roles.select('Role','DateOfAppointment','EndDateOfAppointment').filter(col('AdjudicatorId') == adjudicator_id).collect()

    # Step 4: Query the history details -- HistTypeDescription
    history = df_history.select('HistDate',col('HistType').alias('HistType'),'UserName','Comment').filter(col('AdjudicatorId') == adjudicator_id).collect()

    # Step 4: Read the HTML template
    html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/JOH-Details-no-js-updated-v2.html"
    with open(html_template_path, "r") as f:
        html_template = "".join([l for l in f])

    # Step 5: Date formatting helper
    def format_date(date_value):
        if date_value:
            return datetime.strftime(date_value, "%Y-%m-%d")
        return "yyyy-MM-dd"

    # Step 6: Convert the Spark Row object to a dictionary
    row_dict = judicial_officer_details.asDict()  # Convert Spark Row object to a dictionary

    # Create a dictionary with the replacements, ensuring all values are strings
    replacements = {
        "{{Surname}}": str(row_dict.get('Surname', '') or ''),
        "{{Title}}": str(row_dict.get('Title', '') or ''),
        "{{Forenames}}": str(row_dict.get('Forenames', '') or ''),
        "{{DateOfBirth}}": format_date(row_dict.get('DateOfBirth')),
        "{{CorrespondenceAddress}}": str(row_dict.get('CorrespondenceAddress', '') or ''),
        "{{Telephone}}": str(row_dict.get('ContactTelephone', '') or ''),
        "{{ContactDetails}}": str(row_dict.get('ContactDetails', '') or ''),
        "{{DesignatedCentre}}": str(row_dict.get('DesignatedCentre', '') or ''),
        "{{EmploymentTerm}}": str(row_dict.get('EmploymentTerm', '') or ''),
        "{{FullTime}}": str(row_dict.get('FullTime', '') or ''),
        "{{IdentityNumber}}": str(row_dict.get('IdentityNumber', '') or ''),
        "{{DateOfRetirement}}": format_date(row_dict.get('DateOfRetirement')),
        "{{ContractEndDate}}": format_date(row_dict.get('ContractEndDate')),
        "{{ContractRenewalDate}}": format_date(row_dict.get('ContractRenewalDate')),
        "{{DoNotUseReason}}": str(row_dict.get('DoNotUseReason', '') or ''),
        "{{JudicialStatus}}": str(row_dict.get('JudicialStatus', '') or ''),
        "{{Address1}}": str(row_dict.get('Address1', '') or ''),
        "{{Address2}}": str(row_dict.get('Address2', '') or ''),
        "{{Address3}}": str(row_dict.get('Address3', '') or ''),
        "{{Address4}}": str(row_dict.get('Address4', '') or ''),
        "{{Address5}}": str(row_dict.get('Address5', '') or ''),
        "{{Postcode}}": str(row_dict.get('Postcode', '') or ''),
        "{{Mobile}}": str(row_dict.get('Mobile', '') or ''),
        "{{Email}}": str(row_dict.get('Email', '') or ''),
        "{{BusinessAddress1}}": str(row_dict.get('BusinessAddress1', '') or ''),
        "{{BusinessAddress2}}": str(row_dict.get('BusinessAddress2', '') or ''),
        "{{BusinessAddress3}}": str(row_dict.get('BusinessAddress3', '') or ''),
        "{{BusinessAddress4}}": str(row_dict.get('BusinessAddress4', '') or ''),
        "{{BusinessAddress5}}": str(row_dict.get('BusinessAddress5', '') or ''),
        "{{BusinessPostcode}}": str(row_dict.get('BusinessPostcode', '') or ''),
        "{{BusinessTelephone}}": str(row_dict.get('BusinessTelephone', '') or ''),
        "{{BusinessFax}}": str(row_dict.get('BusinessFax', '') or ''),
        "{{BusinessEmail}}": str(row_dict.get('BusinessEmail', '') or ''),
        "{{JudicialInstructions}}": str(row_dict.get('JudicialInstructions', '') or ''),
        "{{JudicialInstructionsDate}}": format_date(row_dict.get('JudicialInstructionsDate')),
        "{{Notes}}": str(row_dict.get('Notes', '') or ''),
    }
    # Print dict
    # print(replacements)

    # Step 7: Replace placeholders using the replacements dictionary
    for key, value in replacements.items():
        html_template = html_template.replace(key, value)

    # Step 8: Handle multiple rows for Other Centres
    other_centres_code = ""
    for i, centre in enumerate(other_centres, start=1):
        line = f"<tr><td id=\"midpadding\">{i}</td><td id=\"midpadding\">{centre['OtherCentres']}</td></tr>"
        other_centres_code += line + '\n'
    html_template = html_template.replace("{{OtherCentre}}", other_centres_code)

    # Handle roles
    roles_code = ""
    for i, role in enumerate(roles, start=1):
        line = f"<tr><td id=\"midpadding\">{i}</td><td id=\"midpadding\">{role['Role']}</td><td id=\"midpadding\">{format_date(role['DateOfAppointment'])}</td><td id=\"midpadding\">{format_date(role['EndDateOfAppointment'])}</td></tr>"
        roles_code += line + '\n'
    html_template = html_template.replace("{{AppointmentPlaceHolder}}", roles_code)


    # History Details
    History_Code = ''
    for index,row in enumerate(history,start=1):
        line = f"<tr><td id=\"midpadding\">{row['HistDate']}</td><td id=\"midpadding\">{row['HistType']}</td><td id=\"midpadding\">{row['UserName']}</td><td id=\"midpadding\">{row['Comment']}</td></tr>"
        History_Code += line + '\n'
    html_template = html_template.replace(f"{{{{HistoryPlaceHolder}}}}",History_Code)

    # Step 9: Write transformed HTML to lake 
    file_name = f"{silver_mnt}/HTML/judicial_officer_{adjudicator_id}.html"
    dbutils.fs.put(file_name, html_template, overwrite=True)

    print(f"HTML file created for Adjudicator with ID: {adjudicator_id} at {file_name}")

    # # Display the generated HTML
    # if adjudicator_id == 1660:
    #     displayHTML(html_template)

    # Step 10: Return the transformed HTML
    return file_name, "Success"

# @dlt.table
# def generate_html_files():
#     # Example usage
#     adjudicator_id = 1660
#     df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_et_hc_dnur")
#     df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_hearingcentre")
#     df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_role")
#     df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_johistory_users")

#     html_output = generate_html_for_adjudicator(adjudicator_id, df_judicial_officer_details, df_other_centres, df_roles,df_history)

#     # Return an empty DataFrame with the specified schema
#     return dlt.read("silver_adjudicator_et_hc_dnur")

#Example usage
adjudicator_id = 1660
# Load the necessary dataframes from Hive metastore
df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")

generate_html_for_adjudicator(adjudicator_id, df_judicial_officer_details, df_other_centres, df_roles,df_history)



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create silver_adjudicator_html_generation_status & Processing Adjudicator HTML's
# MAGIC This section is to prallel process the HTML and create atracker table with log information.

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.types import LongType

@dlt.table(
    name="gold_joh_html_generation_status",
    comment="Delta Live Silver Table for Silver Adjudicator HTML Generation Status.",
    path=f"{silver_mnt}/ARIADM/ARM/gold_joh_html_generation_status"
)
def gold_joh_html_generation_status():
    
    dlt.read("stg_joh_filtered")
    dlt.read("silver_adjudicator_detail")
    dlt.read("silver_othercentre_detail")
    dlt.read("silver_appointment_detail")
    dlt.read("silver_history_detail")

    # Load the necessary dataframes from Hive metastore
    df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
    df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
    df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
    df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
    df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")
   

    # Fetch the list of Adjudicator IDs from the table
    adjudicator_ids_list = df_stg_joh_filtered.select('AdjudicatorId').distinct().rdd.flatMap(lambda x: x).collect()
    # adjudicator_ids_list = df_judicial_officer_details.select('AdjudicatorId').filter(col('AdjudicatorId') == 1660).distinct().rdd.flatMap(lambda x: x).collect()

    # Create an empty list to store results for the Delta Live Table
    result_list = []

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(generate_html_for_adjudicator, adjudicator_id, 
                             df_judicial_officer_details, df_other_centres, df_roles, df_history): adjudicator_id
            for adjudicator_id in adjudicator_ids_list
        }
        
        for future in as_completed(futures):
            adjudicator_id = futures[future]
            try:
                # Call the function to generate the HTML and file name, along with the status
                file_name, status = future.result()
                
                # Append the result (with status) to the list
                result_list.append((adjudicator_id, file_name, status))
            except Exception as e:
                print(f"Error generating HTML for Adjudicator ID {adjudicator_id}: {str(e)}")
                result_list.append((adjudicator_id, None, f"Error: {str(e)}"))

    # Check if results were generated
    if not result_list:
        print("No results generated. Returning an empty DataFrame.")
        # Return an empty DataFrame with the defined schema
        empty_schema = StructType([
            StructField("AdjudicatorId", LongType(), True),
            StructField("GeneratedFilePath", StringType(), True),
            StructField("Status", StringType(), True)
        ])
        return spark.createDataFrame([], empty_schema)

    # Convert the results list to a DataFrame and return as a Delta Live Table
    result_df = spark.createDataFrame(result_list, ["AdjudicatorId", "GeneratedFilePath", "Status"])

    return result_df

# COMMAND ----------

# %sql
# SELECT * from hive_metastore.ariadm_arm_joh.gold_joh_html_generation_status
# -- where Status != 'Success'



# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate JSON

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function Overview
# MAGIC This section details the function for generating Json files and includes a sample execution.

# COMMAND ----------

import json
from pyspark.sql.functions import col
from datetime import datetime

# Define the function to generate JSON for a given adjudicator
def generate_json_for_adjudicator(adjudicator_id, df_judicial_officer_details, df_other_centres, df_roles, df_history):
    # Step 1: Query the judicial officer details
    try:
        judicial_officer_details = df_judicial_officer_details.filter(col('AdjudicatorId') == adjudicator_id).collect()[0]
    except IndexError:
        print(f"No details found for Adjudicator ID: {adjudicator_id}")
        return

    # Step 2: Query the other centre details
    other_centres = df_other_centres.filter(col('AdjudicatorId') == adjudicator_id).select('OtherCentres').collect()

    # Step 3: Query the role details
    roles = df_roles.filter(col('AdjudicatorId') == adjudicator_id).select('Role', 'DateOfAppointment', 'EndDateOfAppointment').collect()

    # Step 4: Query the history details
    history = df_history.filter(col('AdjudicatorId') == adjudicator_id).select('HistDate', col('HistType').alias('HistType'), 'UserName', 'Comment').collect()

    # Step 5: Date formatting helper
    def format_date(date_value):
        if date_value:
            return datetime.strftime(date_value, "%Y-%m-%d")
        return "yyyy-MM-dd"

    # Step 6: Convert the Spark Row object to a dictionary
    row_dict = judicial_officer_details.asDict()  # Convert Spark Row object to a dictionary

    # Create a dictionary for the adjudicator details
    # Create a dictionary for the adjudicator details
    adjudicator_data = {
        "Surname": row_dict.get('Surname', ''),
        "Forenames": row_dict.get('Forenames', ''),
        "Title": row_dict.get('Title', ''),


        "DateOfBirth": format_date(row_dict.get('DateOfBirth')),
        "CorrespondenceAddress": row_dict.get('CorrespondenceAddress', ''),
        "Telephone": row_dict.get('ContactTelephone', ''),
        "ContactDetails": row_dict.get('ContactDetails', ''),
        "DesignatedCentre": row_dict.get('DesignatedCentre', ''),

        "OtherCentres": [centre['OtherCentres'] for centre in other_centres],


        "EmploymentTerm": row_dict.get('EmploymentTerm', ''),
        "FullTime": "Yes" if row_dict.get('FullTime', False) else "No",
        "IdentityNumber": row_dict.get('IdentityNumber', ''),
        "DateOfRetirement": format_date(row_dict.get('DateOfRetirement')),
        "ContractEndDate": format_date(row_dict.get('ContractEndDate')),
        "ContractRenewalDate": format_date(row_dict.get('ContractRenewalDate')),
        "DoNotUseReason": row_dict.get('DoNotUseReason', ''),
        "JudicialStatus": row_dict.get('JudicialStatus', ''),

        "Roles": [
            {
                "Role": role['Role'],
                "DateOfAppointment": format_date(role['DateOfAppointment']),
                "EndDateOfAppointment": format_date(role['EndDateOfAppointment'])
            }
            for role in roles
        ],


        "Address1": row_dict.get('Address1', ''),
        "Address2": row_dict.get('Address2', ''),
        "Address3": row_dict.get('Address3', ''),
        "Address4": row_dict.get('Address4', ''),
        "Address5": row_dict.get('Address5', ''),
        "Postcode": row_dict.get('Postcode', ''),
        "Mobile": row_dict.get('Mobile', ''),
        "Email": row_dict.get('Email', ''),
        "BusinessAddress1": row_dict.get('BusinessAddress1', ''),
        "BusinessAddress2": row_dict.get('BusinessAddress2', ''),
        "BusinessAddress3": row_dict.get('BusinessAddress3', ''),
        "BusinessAddress4": row_dict.get('BusinessAddress4', ''),
        "BusinessAddress5": row_dict.get('BusinessAddress5', ''),
        "BusinessPostcode": row_dict.get('BusinessPostcode', ''),
        "BusinessTelephone": row_dict.get('BusinessTelephone', ''),
        "BusinessFax": row_dict.get('BusinessFax', ''),
        "BusinessEmail": row_dict.get('BusinessEmail', ''),
        
        "JudicialInstructions": row_dict.get('JudicialInstructions', ''),
        "JudicialInstructionsDate": format_date(row_dict.get('JudicialInstructionsDate')),
        "Notes": row_dict.get('Notes', ''),
        
        
        "History": [
            {
                "HistDate": format_date(row['HistDate']),
                "HistType": row['HistType'],
                "UserName": row['UserName'],
                "Comment": row['Comment']
            }
            for row in history
        ]
    }

    # Step 9: Write transformed JSON to lake 
    json_dir = "/mnt/ingest00curatedsboxsilver/JSON"
    file_name = f"{json_dir}/judicial_officer_{adjudicator_id}.json"
    
    # Convert the dictionary to a JSON string
    json_content = json.dumps(adjudicator_data, indent=4)
    
    # Use dbutils.fs.put to write the JSON to DBFS
    dbutils.fs.put(file_name, json_content, overwrite=True)

    print(f"JSON file created for Adjudicator with ID: {adjudicator_id} at {file_name}")

    # Step 10: Return the transformed JSON file path
    return file_name, "Success"

# Example usage
adjudicator_id = 1660
# Load the necessary dataframes from Hive metastore
df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")

generate_json_for_adjudicator(adjudicator_id, df_judicial_officer_details, df_other_centres, df_roles, df_history)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create silver_adjudicator_Json_generation_status & Processing Adjudicator Json's
# MAGIC This section is to prallel process the json and create atracker table with log information.

# COMMAND ----------

# from concurrent.futures import ThreadPoolExecutor, as_completed
# from pyspark.sql.types import LongType

@dlt.table(
    name="gold_joh_json_generation_status",
    comment="Delta Live Silver Table for Silver Adjudicator json Generation Status.",
    path=f"{silver_mnt}/ARIADM/ARM/gold_joh_json_generation_status"
)
def gold_joh_json_generation_status():
    
    dlt.read("stg_joh_filtered")
    dlt.read("silver_adjudicator_detail")
    dlt.read("silver_othercentre_detail")
    dlt.read("silver_appointment_detail")
    dlt.read("silver_history_detail")

    # Load the necessary dataframes from Hive metastore
    df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
    df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
    df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
    df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
    df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")
   

    # Fetch the list of Adjudicator IDs from the table
    adjudicator_ids_list = df_stg_joh_filtered.select('AdjudicatorId').distinct().rdd.flatMap(lambda x: x).collect()
    # adjudicator_ids_list = df_judicial_officer_details.select('AdjudicatorId').filter(col('AdjudicatorId') == 1660).distinct().rdd.flatMap(lambda x: x).collect()

    # Create an empty list to store results for the Delta Live Table
    result_list = []

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(generate_json_for_adjudicator, adjudicator_id, 
                             df_judicial_officer_details, df_other_centres, df_roles, df_history): adjudicator_id
            for adjudicator_id in adjudicator_ids_list
        }
        
        for future in as_completed(futures):
            adjudicator_id = futures[future]
            try:
                # Call the function to generate the json and file name, along with the status
                file_name, status = future.result()
                
                # Append the result (with status) to the list
                result_list.append((adjudicator_id, file_name, status))
            except Exception as e:
                print(f"Error generating json for Adjudicator ID {adjudicator_id}: {str(e)}")
                result_list.append((adjudicator_id, None, f"Error: {str(e)}"))

    # Check if results were generated
    if not result_list:
        print("No results generated. Returning an empty DataFrame.")
        # Return an empty DataFrame with the defined schema
        empty_schema = StructType([
            StructField("AdjudicatorId", LongType(), True),
            StructField("GeneratedFilePath", StringType(), True),
            StructField("Status", StringType(), True)
        ])
        return spark.createDataFrame([], empty_schema)

    # Convert the results list to a DataFrame and return as a Delta Live Table
    result_df = spark.createDataFrame(result_list, ["AdjudicatorId", "GeneratedFilePath", "Status"])

    return result_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate a360 files

# COMMAND ----------

#Sample frin td
# {"operation": "create_record","relation_id":"152820","record_metadata":{"publisher":"IADEMO","record_class":"IADEMO","region":"GBR","recordDate":"2023-04-12T00:00:00Z","event_date":"2023-04-12T00:00:00Z","client_identifier":"HU/02287/2021","bf_001":"Orgest","bf_002":"Hoxha","bf_003":"A1234567/001","bf_004":"1990-06-09T00:00:00Z","bf_005":"ABC/12345","bf_010":"2024-01-01T00:00:00Z"}}

# {"operation": "upload_new_file","relation_id":"152820","file_metadata":{"publisher":"IADEMO","dz_file_name":"HU_02287_2021.json","file_tag":"json"}}

# {"operation": "upload_new_file","relation_id":"152820","file_metadata":{"publisher":"IADEMO","dz_file_name":"HU022872021.pdf","file_tag":"pdf"}}
 

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime
import json

adjudicator_id = 1660

# Load the necessary dataframes from Hive metastore
df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
df_joh_metadata = spark.read.table("hive_metastore.ariadm_arm_joh.silver_archive_metadata")

def format_date(date_value):
    if isinstance(date_value, str):  # If the date is already a string, return as is
        return date_value
    elif isinstance(date_value, (datetime,)):
        return datetime.strftime(date_value, "%Y-%m-%dT%H:%M:%SZ")
    return None  # Return None if the date is invalid or not provided

try:
    df_metadata = df_joh_metadata.filter(col('client_identifier') == adjudicator_id).collect()
    
    if not df_metadata:  # Check if any rows are returned
        print(f"No details found for Adjudicator ID: {adjudicator_id}")
    else:
        # Convert the first Spark Row object to a dictionary
        row_dict = df_metadata[0].asDict()

        # Create a dictionary for the adjudicator details
        metadata_data = {
            "operation": "create_record",
            "relation_id": row_dict.get('client_identifier', ''),
            "record_metadata": {
                "publisher": row_dict.get('publisher', ''),
                "record_class": row_dict.get('record_class', ''),
                "region": row_dict.get('region', ''),
                "recordDate": format_date(row_dict.get('recordDate')),
                "event_date": format_date(row_dict.get('event_date')),
                "client_identifier": row_dict.get('client_identifier', ''),
                "bf_001": row_dict.get('bf_title', ''),
                "bf_002": row_dict.get('bf_forename', ''),
                "bf_003": row_dict.get('bf_surname', ''),
                "bf_004": format_date(row_dict.get('bf_dateofbirth')),
                "bf_005": row_dict.get('bf_designatedcentre', '')
            }
        }

        html_data = {
            "operation": "upload_new_file",
            "relation_id": row_dict.get('client_identifier', ''),
            "file_metadata": {
                "publisher": row_dict.get('publisher', ''),
                "dz_file_name": f"judicial_officer_{adjudicator_id}.html",
                "file_tag": "html"
            }
        }

        json_data = {
            "operation": "upload_new_file",
            "relation_id": row_dict.get('client_identifier', ''),
            "file_metadata": {
                "publisher": row_dict.get('publisher', ''),
                "dz_file_name": f"judicial_officer_{adjudicator_id}.json",
                "file_tag": "json"
            }
        }

        # Convert dictionaries to compact single-line strings
        metadata_data_str = json.dumps(metadata_data, separators=(',', ':'))
        html_data_str = json.dumps(html_data, separators=(',', ':'))
        json_data_str = json.dumps(json_data, separators=(',', ':'))

        # Concatenate the three lines into a single variable
        all_data_str = f"{metadata_data_str}\n{html_data_str}\n{json_data_str}"

        # Now you have all three lines stored in `all_data_str`
        print(all_data_str)  # Or use `all_data_str` as needed

except IndexError:
    print(f"No details found for Adjudicator ID: {adjudicator_id}")
except Exception as e:
    print(f"An error occurred: {e}")

