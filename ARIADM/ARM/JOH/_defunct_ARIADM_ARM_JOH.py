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
# MAGIC          <td>Notebook to generate a set of HTML, JSON, and A360 files, each representing the data about judges stored in ARIA.</td>
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
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-95">ARIADM-95</a>/NSA/SEP-2024</td>
# MAGIC          <td>JOH :Compete Landing to Bronze Notebook</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-97">ARIADM-97</a>/NSA/SEP-2024</td>
# MAGIC          <td>JOH: Create Silver and gold HTML outputs: </td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-123">ARIADM-123</a>/NSA/SEP-2024</td>
# MAGIC          <td>JOH: Create Gold outputs- Json and A360</td>
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
# MAGIC ## Set Variables

# COMMAND ----------

# MAGIC %md
# MAGIC Please note that running the DLT pipeline with the parameter `initial_load = true` will ensure the creation of the corresponding Hive tables. However, during this stage, none of the gold outputs (HTML, JSON, and A360) are processed. To generate the gold outputs, a secondary run with `initial_load = true` is required.

# COMMAND ----------


initial_Load = False

# Setting variables for use in subsequent cells
raw_mnt = "/mnt/ingest00rawsboxraw/ARIADM/ARM/JOH"
landing_mnt = "/mnt/ingest00landingsboxlanding/"
bronze_mnt = "/mnt/ingest00curatedsboxbronze/ARIADM/ARM/JOH"
silver_mnt = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/JOH"
gold_mnt = "/mnt/ingest00curatedsboxgold/ARIADM/ARM/JOH"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions to Read Latest Landing Files

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

# MAGIC %md
# MAGIC ## Raw DLT Tables Creation

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

@dlt.table(
    name="raw_adjudicatorrole",
    comment="Delta Live Table ARIA AdjudicatorRole.",
    path=f"{raw_mnt}/Raw_AdjudicatorRole"
)
def Raw_AdjudicatorRole():
    return read_latest_parquet("AdjudicatorRole", "tv_AdjudicatorRole", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_adjudicator",
    comment="Delta Live Table ARIA Adjudicator.",
    path=f"{raw_mnt}/Raw_Adjudicator"
)
def Raw_Adjudicator():
    return read_latest_parquet("Adjudicator", "tv_Adjudicator", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_employmentterm",
    comment="Delta Live Table ARIA EmploymentTerm.",
    path=f"{raw_mnt}/Raw_EmploymentTerm"
)
def Raw_EmploymentTerm():
     return read_latest_parquet("ARIAEmploymentTerm", "tv_EmploymentTerm", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_donotusereason",
    comment="Delta Live Table ARIA DoNotUseReason.",
    path=f"{raw_mnt}/Raw_DoNotUseReason"
)
def Raw_DoNotUseReason():
    return read_latest_parquet("DoNotUseReason", "tv_DoNotUseReason", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_johistory",
    comment="Delta Live Table ARIA JoHistory.",
    path=f"{raw_mnt}/Raw_JoHistory"
)
def Raw_JoHistory():
    return read_latest_parquet("JoHistory", "tv_JoHistory", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_othercentre",
    comment="Delta Live Table ARIA OtherCentre.",
    path=f"{raw_mnt}/Raw_OtherCentre"
)
def Raw_OtherCentre():
    return read_latest_parquet("OtherCentre", "tv_OtherCentre", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_hearingcentre",
    comment="Delta Live Table ARIA HearingCentre.",
    path=f"{raw_mnt}/Raw_HearingCentre"
)
def Raw_HearingCentre():
    return read_latest_parquet("ARIAHearingCentre", "tv_HearingCentre", "ARIA_ARM_JOH_ARA")


@dlt.table(
    name="raw_users",
    comment="Delta Live Table ARIA Users.",
    path=f"{raw_mnt}/Raw_Users"
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
    path=f"{bronze_mnt}/bronze_adjudicator_et_hc_dnur"
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
    path=f"{bronze_mnt}/bronze_johistory_users" 
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
    path=f"{bronze_mnt}/bronze_othercentre_hearingcentre" 
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
    path=f"{bronze_mnt}/bronze_adjudicator_role" 
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
# MAGIC The below staging table is joined with other silver table to esure the Role NOT IN ( 7, 8 ) 

# COMMAND ----------

@dlt.table(
    name="stg_joh_filtered",
    comment="Delta Live silver Table segmentation with judges only using bronze_adjudicator_et_hc_dnur.",
    path=f"{silver_mnt}/stg_joh_filtered"
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
    path=f"{silver_mnt}/silver_adjudicator_detail"
)
def silver_adjudicator_detail():
    return (
        dlt.read("bronze_adjudicator_et_hc_dnur").alias("adj").join(dlt.read("stg_joh_filtered").alias('flt'), col("adj.AdjudicatorId") == col("flt.AdjudicatorId"), "inner").select(
            col("adj.AdjudicatorId"),
            col("adj.Surname"),
            col("adj.Forenames"),
            col("adj.Title"),
            col("adj.DateOfBirth"),
            when(col("adj.CorrespondenceAddress") == 1, "Business").otherwise("Home").alias("CorrespondenceAddress"),
            col("adj.ContactDetails"),
            when(col("adj.ContactTelephone") == 1, "Business")
                .when(col("adj.ContactTelephone") == 2, "Home")
                .when(col("adj.ContactTelephone") == 3, "Mobile")
                .otherwise(col("adj.ContactTelephone")).alias("ContactTelephone"),
            col("adj.AvailableAtShortNotice"),
            col("adj.DesignatedCentre"),
            col("adj.EmploymentTerm"),
            when(col("adj.FullTime") == 1,'Yes').otherwise("No").alias("FullTime"),
            col("adj.IdentityNumber"),
            col("adj.DateOfRetirement"),
            col("adj.ContractEndDate"),
            col("adj.ContractRenewalDate"),
            col("adj.DoNotUse"),
            col("adj.DoNotUseReason"),
            when(col("adj.JudicialStatus") == 1, "Deputy President")
                .when(col("adj.JudicialStatus") == 2, "Designated Immigration Judge")
                .when(col("adj.JudicialStatus") == 3, "Immigration Judge")
                .when(col("adj.JudicialStatus") == 4, "President")
                .when(col("adj.JudicialStatus") == 5, "Senior Immigration Judge")
                .when(col("adj.JudicialStatus") == 6, "Deputy Senior Immigration Judge")
                .when(col("adj.JudicialStatus") == 7, "Senior President")
                .when(col("adj.JudicialStatus") == 21, "Vice President of the Upper Tribunal IAAC")
                .when(col("adj.JudicialStatus") == 22, "Designated Judge of the First-tier Tribunal")
                .when(col("adj.JudicialStatus") == 23, "Judge of the First-tier Tribunal")
                .when(col("adj.JudicialStatus") == 25, "Upper Tribunal Judge")
                .when(col("adj.JudicialStatus") == 26, "Deputy Judge of the Upper Tribunal")
                .when(col("adj.JudicialStatus") == 28, "Resident Judge of the First-tier Tribunal")
                .when(col("adj.JudicialStatus") == 29, "Principle Resident Judge of the Upper Tribunal")
                .otherwise(col("adj.JudicialStatus")).alias("JudicialStatus"),
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
# MAGIC ### Transformation  silver_history_detail

# COMMAND ----------

@dlt.table(
    name="silver_history_detail",
    comment="Delta Live Silver Table combining JoHistory data with Users information.",
    path=f"{silver_mnt}/silver_history_detail"
)
def silver_history_detail():
    return (
        dlt.read("bronze_johistory_users").alias("his").join(dlt.read("stg_joh_filtered").alias('flt'), col("his.AdjudicatorId") == col("flt.AdjudicatorId"), "inner").select(
            col('his.AdjudicatorId'),
            col('his.HistDate'),
            when(col("his.HistType") == 1, "Adjournment")
            .when(col("his.HistType") == 2, "Adjudicator Process")
            .when(col("his.HistType") == 3, "Bail Process")
            .when(col("his.HistType") == 4, "Change of Address")
            .when(col("his.HistType") == 5, "Decisions")
            .when(col("his.HistType") == 6, "File Location")
            .when(col("his.HistType") == 7, "Interpreters")
            .when(col("his.HistType") == 8, "Issue")
            .when(col("his.HistType") == 9, "Links")
            .when(col("his.HistType") == 10, "Listing")
            .when(col("his.HistType") == 11, "SIAC Process")
            .when(col("his.HistType") == 12, "Superior Court")
            .when(col("his.HistType") == 13, "Tribunal Process")
            .when(col("his.HistType") == 14, "Typing")
            .when(col("his.HistType") == 15, "Parties edited")
            .when(col("his.HistType") == 16, "Document")
            .when(col("his.HistType") == 17, "Document Received")
            .when(col("his.HistType") == 18, "Manual Entry")
            .when(col("his.HistType") == 19, "Interpreter")
            .when(col("his.HistType") == 20, "File Detail Changed")
            .when(col("his.HistType") == 21, "Dedicated hearing centre changed")
            .when(col("his.HistType") == 22, "File Linking")
            .when(col("his.HistType") == 23, "Details")
            .when(col("his.HistType") == 24, "Availability")
            .when(col("his.HistType") == 25, "Cancel")
            .when(col("his.HistType") == 26, "De-allocation")
            .when(col("his.HistType") == 27, "Work Pattern")
            .when(col("his.HistType") == 28, "Allocation")
            .when(col("his.HistType") == 29, "De-Listing")
            .when(col("his.HistType") == 30, "Statutory Closure")
            .when(col("his.HistType") == 31, "Provisional Destruction Date")
            .when(col("his.HistType") == 32, "Destruction Date")
            .when(col("his.HistType") == 33, "Date of Service")
            .when(col("his.HistType") == 34, "IND Interface")
            .when(col("his.HistType") == 35, "Address Changed")
            .when(col("his.HistType") == 36, "Contact Details")
            .when(col("his.HistType") == 37, "Effective Date")
            .when(col("his.HistType") == 38, "Centre Changed")
            .when(col("his.HistType") == 39, "Appraisal Added")
            .when(col("his.HistType") == 40, "Appraisal Removed")
            .when(col("his.HistType") == 41, "Costs Deleted")
            .when(col("his.HistType") == 42, "Credit/Debit Card Payment received")
            .when(col("his.HistType") == 43, "Bank Transfer Payment received")
            .when(col("his.HistType") == 44, "Chargeback Taken")
            .when(col("his.HistType") == 45, "Remission request Rejected")
            .when(col("his.HistType") == 46, "Refund Event Added")
            .when(col("his.HistType") == 47, "WriteOff, Strikeout Write-Off or Threshold Write-off Event Added")
            .when(col("his.HistType") == 48, "Aggregated Payment Taken")
            .when(col("his.HistType") == 49, "Case Created")
            .when(col("his.HistType") == 50, "Tracked Document")
            .otherwise(col("his.HistType")).alias("HistType"),
            col('his.UserName'),
            col('his.Comment'),
            col('his.AdtclmnFirstCreatedDatetime'),
            col('his.AdtclmnModifiedDatetime'),
            col('his.SourceFileName'),
            col('his.InsertedByProcessName'),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_othercentre_detail

# COMMAND ----------

@dlt.table(
    name="silver_othercentre_detail",
    comment="Delta Live silver Table combining OtherCentre data with HearingCentre information.",
    path=f"{silver_mnt}/silver_othercentre_detail"
)
def silver_othercentre_detail():
    return (dlt.read("bronze_othercentre_hearingcentre").alias("hc").join(dlt.read("stg_joh_filtered").alias('flt'), col("hc.AdjudicatorId") == col("flt.AdjudicatorId"), "inner").select("hc.*"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_appointment_detail

# COMMAND ----------

@dlt.table(
    name="silver_appointment_detail",
    comment="Delta Live Silver Table for Adjudicator Role data.",
    path=f"{silver_mnt}/silver_appointment_detail"
)
def silver_appointment_detail():
    return (
        dlt.read("bronze_adjudicator_role").alias("rol").join(dlt.read("stg_joh_filtered").alias('flt'), col("rol.AdjudicatorId") == col("flt.AdjudicatorId"), "inner").select(
            col('rol.AdjudicatorId'),
            when(col("rol.Role") == 2, "Chairman")
            .when(col("rol.Role") == 5, "Adjudicator")
            .when(col("rol.Role") == 6, "Lay Member")
            .when(col("rol.Role") == 7, "Court Clerk")
            .when(col("rol.Role") == 8, "Usher")
            .when(col("rol.Role") == 9, "Qualified Member")
            .when(col("rol.Role") == 10, "Senior Immigration Judge")
            .when(col("rol.Role") == 11, "Immigration Judge")
            .when(col("rol.Role") == 12, "Non Legal Member")
            .when(col("rol.Role") == 13, "Designated Immigration Judge")
            .when(col("rol.Role") == 20, "Upper Tribunal Judge")
            .when(col("rol.Role") == 21, "Judge of the First-tier Tribunal")
            .when(col("rol.Role") == 23, "Designated Judge of the First-tier Tribunal")
            .when(col("rol.Role") == 24, "Upper Tribunal Judge acting As A Judge Of The First-tier Tribunal")
            .when(col("rol.Role") == 25, "Deputy Judge of the Upper Tribunal")
            .otherwise(col("rol.Role")).alias("Role"),
            col('rol.DateOfAppointment'),
            col('rol.EndDateOfAppointment'),
            col('rol.AdtclmnFirstCreatedDatetime'),
            col('rol.AdtclmnModifiedDatetime'),
            col('rol.SourceFileName'),
            col('rol.InsertedByProcessName')
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
    path=f"{silver_mnt}/silver_archive_metadata"
)
def silver_archive_metadata():
    return (
        dlt.read("silver_adjudicator_detail").alias("adj").join(dlt.read("stg_joh_filtered").alias('flt'), col("adj.AdjudicatorId") == col("flt.AdjudicatorId"), "inner").select(
            col('adj.AdjudicatorId').alias('client_identifier'),
            date_format(coalesce(col('adj.DateOfRetirement'), col('adj.ContractEndDate'), col('adj.AdtclmnFirstCreatedDatetime')), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_date"),
            date_format(col('adj.AdtclmnFirstCreatedDatetime'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("recordDate"),
            lit("GBR").alias("region"),
            lit("ARIA").alias("publisher"),
            lit("ARIA Judicial Records").alias("record_class"),
            lit('IA_Judicial_Office').alias("entitlement_tag"),
            col('adj.Title').alias('bf_001'),
            col('adj.Forenames').alias('bf_002'),
            col('adj.Surname').alias('bf_003'),
            col('adj.DateOfBirth').alias('bf_004'),
            col('adj.DesignatedCentre').alias('bf_005')
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Outputs and Tracking DLT Table Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate HTML

# COMMAND ----------

# MAGIC %md
# MAGIC #### Function Overview
# MAGIC This section details the function for generating HTML files and includes a sample execution.

# COMMAND ----------

# DBTITLE 1,TEST
import json
from pyspark.sql.functions import col
from datetime import datetime
from pyspark.sql.types import StructType, StructField, LongType, StringType

# Step 5: Date formatting helper (Reuse from HTML generation)
def format_date(date_value):
    if date_value:
        return datetime.strftime(date_value, "%Y-%m-%d")
    return "yyyy-MM-dd"

# Function to generate JSON for each partition
def generate_json_for_partition(partition_data):
    results = []
    
    for row in partition_data:
        adjudicator_id = row['AdjudicatorId']
        
        try:
            # Query the judicial officer details
            judicial_officer_details = row.asDict()

            # Get related records from other dataframes (broadcasted for efficiency)
            other_centres = other_centres_broadcast.value.filter(col('AdjudicatorId') == adjudicator_id).select('OtherCentres').collect()
            roles = roles_broadcast.value.filter(col('AdjudicatorId') == adjudicator_id).select('Role', 'DateOfAppointment', 'EndDateOfAppointment').collect()
            history = history_broadcast.value.filter(col('AdjudicatorId') == adjudicator_id).select('HistDate', col('HistType').alias('HistType'), 'UserName', 'Comment').collect()

            # Step 6: Create a dictionary for the adjudicator details
            adjudicator_data = {
                "Surname": judicial_officer_details.get('Surname', ''),
                "Forenames": judicial_officer_details.get('Forenames', ''),
                "Title": judicial_officer_details.get('Title', ''),
                "DateOfBirth": format_date(judicial_officer_details.get('DateOfBirth')),
                "CorrespondenceAddress": judicial_officer_details.get('CorrespondenceAddress', ''),
                "Telephone": judicial_officer_details.get('ContactTelephone', ''),
                "DesignatedCentre": judicial_officer_details.get('DesignatedCentre', ''),
                "OtherCentres": [centre['OtherCentres'] for centre in other_centres],
                "EmploymentTerm": judicial_officer_details.get('EmploymentTerm', ''),
                "FullTime": "Yes" if judicial_officer_details.get('FullTime', False) else "No",
                "IdentityNumber": judicial_officer_details.get('IdentityNumber', ''),
                "DateOfRetirement": format_date(judicial_officer_details.get('DateOfRetirement')),
                "ContractEndDate": format_date(judicial_officer_details.get('ContractEndDate')),
                "Roles": [
                    {
                        "Role": role['Role'],
                        "DateOfAppointment": format_date(role['DateOfAppointment']),
                        "EndDateOfAppointment": format_date(role['EndDateOfAppointment'])
                    }
                    for role in roles
                ],
                "History": [
                    {
                        "HistDate": format_date(hist['HistDate']),
                        "HistType": hist['HistType'],
                        "UserName": hist['UserName'],
                        "Comment": hist['Comment']
                    }
                    for hist in history
                ]
            }

            # Step 9: Write the JSON to DBFS
            file_name = f"{gold_mnt}/JSON/judicial_officer_{adjudicator_id}.json"
            json_content = json.dumps(adjudicator_data, indent=4)
            dbutils.fs.put(file_name, json_content, overwrite=True)

            # Step 10: Append success result
            results.append((adjudicator_id, file_name, "Success"))

        except Exception as e:
            print(f"Error generating JSON for Adjudicator ID {adjudicator_id}: {str(e)}")
            results.append((adjudicator_id, None, f"Error: {str(e)}"))

    return iter(results)

# Main function to process JSON generation
@dlt.table(
    name="gold_joh_json_generation_status",
    comment="Delta Live Table for JSON Generation Status",
    path=f"{gold_mnt}/gold_joh_json_generation_status"
)
def gold_joh_json_generation_status():
    # Load dataframes
    if not initial_Load:
        df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
        df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
        df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
        df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
        df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")
    else:
        df_stg_joh_filtered = dlt.read("stg_joh_filtered")
        df_judicial_officer_details = dlt.read("silver_adjudicator_detail")
        df_other_centres = dlt.read("silver_othercentre_detail")
        df_roles = dlt.read("silver_appointment_detail")
        df_history = dlt.read("silver_history_detail")

    # Broadcast related dataframes for efficiency
    global other_centres_broadcast, roles_broadcast, history_broadcast
    other_centres_broadcast = spark.sparkContext.broadcast(df_other_centres)
    roles_broadcast = spark.sparkContext.broadcast(df_roles)
    history_broadcast = spark.sparkContext.broadcast(df_history)

    # Repartition by AdjudicatorId for parallel processing
    df_judicial_officer_details_repart = df_judicial_officer_details.repartition(col('AdjudicatorId'))

    # Use mapPartitions for parallel processing
    result_rdd = df_judicial_officer_details_repart.rdd.mapPartitions(generate_json_for_partition)

    # Convert the results back to a DataFrame
    result_schema = StructType([
        StructField("AdjudicatorId", LongType(), True),
        StructField("GeneratedFilePath", StringType(), True),
        StructField("Status", StringType(), True)
    ])
    
    result_df = spark.createDataFrame(result_rdd, result_schema)

    return result_df


# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime


# Step 5: Date formatting helper
def format_date_iso(date_value):
    if date_value:
        return datetime.strftime(date_value, "%Y-%m-%d")
    return ""
    # return "dd-MM-yyyy"

def format_date(date_value):
    if date_value:
        return datetime.strftime(date_value, "%d/%m/%Y")
    return ""

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


    # Step 6: Convert the Spark Row object to a dictionary
    row_dict = judicial_officer_details.asDict()  # Convert Spark Row object to a dictionary

    # Create a dictionary with the replacements, ensuring all values are strings
    replacements = {
        "{{Surname}}": str(row_dict.get('Surname', '') or ''),
        "{{Title}}": str(row_dict.get('Title', '') or ''),
        "{{Forenames}}": str(row_dict.get('Forenames', '') or ''),
        "{{DateOfBirth}}": format_date_iso(row_dict.get('DateOfBirth')),
        "{{CorrespondenceAddress}}": str(row_dict.get('CorrespondenceAddress', '') or ''),
        "{{Telephone}}": str(row_dict.get('ContactTelephone', '') or ''),
        "{{ContactDetails}}": str(row_dict.get('ContactDetails', '') or ''),
        "{{DesignatedCentre}}": str(row_dict.get('DesignatedCentre', '') or ''),
        "{{EmploymentTerm}}": str(row_dict.get('EmploymentTerm', '') or ''),
        "{{FullTime}}": str(row_dict.get('FullTime', '') or ''),
        "{{IdentityNumber}}": str(row_dict.get('IdentityNumber', '') or ''),
        "{{DateOfRetirement}}": format_date_iso(row_dict.get('DateOfRetirement')),
        "{{ContractEndDate}}": format_date_iso(row_dict.get('ContractEndDate')),
        "{{ContractRenewalDate}}": format_date_iso(row_dict.get('ContractRenewalDate')),
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
        "{{JudicialInstructionsDate}}": format_date_iso(row_dict.get('JudicialInstructionsDate')),
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
        line = f"<tr><td id=\"midpadding\">{format_date(row['HistDate'])}</td><td id=\"midpadding\">{row['HistType']}</td><td id=\"midpadding\">{row['UserName']}</td><td id=\"midpadding\">{row['Comment']}</td></tr>"
        History_Code += line + '\n'
    html_template = html_template.replace(f"{{{{HistoryPlaceHolder}}}}",History_Code)

    # Step 9: Write transformed HTML to lake 
    file_name = f"{gold_mnt}/HTML/judicial_officer_{adjudicator_id}.html"
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

# Example usage
# adjudicator_id = 1660
# # Load the necessary dataframes from Hive metastore
# df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
# df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
# df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
# df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
# df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")

# generate_html_for_adjudicator(adjudicator_id, df_judicial_officer_details, df_other_centres, df_roles,df_history)



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create gold_adjudicator_html_generation_status & Processing Adjudicator HTML's
# MAGIC This section is to prallel process the HTML and create atracker table with log information.

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.types import LongType

@dlt.table(
    name="gold_joh_html_generation_status",
    comment="Delta Live Silver Table for Silver Adjudicator HTML Generation Status.",
    path=f"{gold_mnt}/gold_joh_html_generation_status"
)
def gold_joh_html_generation_status():
    
    df_stg_joh_filtered = dlt.read("stg_joh_filtered")
    df_judicial_officer_details =dlt.read("silver_adjudicator_detail")
    df_other_centres = dlt.read("silver_othercentre_detail")
    df_roles = dlt.read("silver_appointment_detail")
    df_history =dlt.read("silver_history_detail")

    # Load the necessary dataframes from Hive metastore
    if initial_Load == False:
        print("Running non initial load")
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
    with ThreadPoolExecutor(max_workers=10) as executor:
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
# MAGIC ### Generate JSON

# COMMAND ----------

# MAGIC %md
# MAGIC #### Function Overview
# MAGIC This section details the function for generating Json files and includes a sample execution.

# COMMAND ----------

# DBTITLE 1,HMTL Output Processing
import json
from pyspark.sql.functions import col
from datetime import datetime

# Step 5: Date formatting helper
def format_date(date_value):
    if date_value:
        return datetime.strftime(date_value, "%Y-%m-%d")
    return "yyyy-MM-dd"

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
    # json_dir = "{gold_mnt}/ARIADM/ARM/JSON"
    file_name = f"{gold_mnt}/JSON/judicial_officer_{adjudicator_id}.json"
    
    # Convert the dictionary to a JSON string
    json_content = json.dumps(adjudicator_data, indent=4)
    
    # Use dbutils.fs.put to write the JSON to DBFS
    dbutils.fs.put(file_name, json_content, overwrite=True)

    print(f"JSON file created for Adjudicator with ID: {adjudicator_id} at {file_name}")

    # Step 10: Return the transformed JSON file path
    return file_name, "Success"

# Example usage
# adjudicator_id = 1660
# # Load the necessary dataframes from Hive metastore
# df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
# df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
# df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
# df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")

# generate_json_for_adjudicator(adjudicator_id, df_judicial_officer_details, df_other_centres, df_roles, df_history)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create gold_adjudicator_Json_generation_status & Processing Adjudicator Json's
# MAGIC This section is to prallel process the json and create atracker table with log information.

# COMMAND ----------

# from concurrent.futures import ThreadPoolExecutor, as_completed
# from pyspark.sql.types import LongType

@dlt.table(
    name="gold_joh_json_generation_status",
    comment="Delta Live Silver Table for Silver Adjudicator json Generation Status.",
    path=f"{gold_mnt}/gold_joh_json_generation_status"
)
def gold_joh_json_generation_status():
    
    df_stg_joh_filtered = dlt.read("stg_joh_filtered")
    df_judicial_officer_details =dlt.read("silver_adjudicator_detail")
    df_other_centres = dlt.read("silver_othercentre_detail")
    df_roles = dlt.read("silver_appointment_detail")
    df_history =dlt.read("silver_history_detail")

    # Load the necessary dataframes from Hive metastore
    if initial_Load == False:
        print("Running non initial load")
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
    with ThreadPoolExecutor(max_workers=10) as executor:
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
# MAGIC ### Generate a360 files

# COMMAND ----------

# MAGIC %md
# MAGIC #### Function Overview
# MAGIC This section details the function for generating A360 files and includes a sample execution.

# COMMAND ----------

#Sample frin td
# {"operation": "create_record","relation_id":"152820","record_metadata":{"publisher":"IADEMO","record_class":"IADEMO","region":"GBR","recordDate":"2023-04-12T00:00:00Z","event_date":"2023-04-12T00:00:00Z","client_identifier":"HU/02287/2021","bf_001":"Orgest","bf_002":"Hoxha","bf_003":"A1234567/001","bf_004":"1990-06-09T00:00:00Z","bf_005":"ABC/12345","bf_010":"2024-01-01T00:00:00Z"}}

# {"operation": "upload_new_file","relation_id":"152820","file_metadata":{"publisher":"IADEMO","dz_file_name":"HU_02287_2021.json","file_tag":"json"}}

# {"operation": "upload_new_file","relation_id":"152820","file_metadata":{"publisher":"IADEMO","dz_file_name":"HU022872021.pdf","file_tag":"pdf"}}
 

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime
import json
from pyspark.sql.types import LongType, StringType, StructType, StructField

# # Function to format dates
def format_date(date_value):
    if isinstance(date_value, str):  # If the date is already a string, return as is
        return date_value
    elif isinstance(date_value, (datetime,)):
        return datetime.strftime(date_value, "%Y-%m-%dT%H:%M:%SZ")
    return None  # Return None if the date is invalid or not provided

# Function to generate an .a360 file for a given adjudicator
def generate_a360_file_for_adjudicator(adjudicator_id, df_joh_metadata):
    try:
        # Query the judicial officer details
        df_metadata  = df_joh_metadata.filter(col('client_identifier') == adjudicator_id).collect()
    except Exception as e:
        print(f"Error querying Adjudicator ID: {adjudicator_id}, Error: {str(e)}")
        return None, f"Error querying Adjudicator ID: {adjudicator_id}: {str(e)}"

    if not df_metadata:  # Check if any rows are returned
        print(f"No details found for Adjudicator ID: {adjudicator_id}")
        return None, f"No details for Adjudicator ID: {adjudicator_id}"

    # Convert the first Spark Row object to a dictionary
    row_dict = df_metadata[0].asDict()

    # Create metadata, HTML, and JSON strings
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
            "bf_001": row_dict.get('bf_001', ''),
            "bf_002": row_dict.get('bf_002', ''),
            "bf_003": row_dict.get('bf_003', ''),
            "bf_004": format_date(row_dict.get('bf_004')),
            "bf_005": row_dict.get('bf_005', '')
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

    # Convert dictionaries to JSON strings
    metadata_data_str = json.dumps(metadata_data, separators=(',', ':'))
    html_data_str = json.dumps(html_data, separators=(',', ':'))
    json_data_str = json.dumps(json_data, separators=(',', ':'))

    # Combine the data
    all_data_str = f"{metadata_data_str}\n{html_data_str}\n{json_data_str}"

    # # Now you have all three lines stored in `all_data_str`
    # print(all_data_str)  # Or use `all_data_str` as needed

    # Write to A360 file
    file_name = f"{gold_mnt}/A360/judicial_officer_{adjudicator_id}.a360"
    try:
        dbutils.fs.put(file_name, all_data_str, overwrite=True)
        print(f"A360 file created for Adjudicator with ID: {adjudicator_id} at {file_name}")
        return file_name, "Success"
    except Exception as e:
        print(f"Error writing file for Adjudicator ID: {adjudicator_id}: {str(e)}")
        return None, f"Error writing file: {str(e)}"
    
# Example usage
# adjudicator_id = 1660

# # Load necessary data from Hive tables
# df_joh_metadata = spark.read.table("hive_metastore.ariadm_arm_joh.silver_archive_metadata")

# generate_a360_file_for_adjudicator(adjudicator_id, df_joh_metadata)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create gold_adjudicator_Json_generation_status & Processing Adjudicator A360's
# MAGIC This section is to prallel process the json and create atracker table with log information.

# COMMAND ----------

# Delta Live Table
@dlt.table(
    name="gold_joh_a360_generation_status",
    comment="Delta Live Table for Silver Adjudicator .a360 File Generation Status.",
    path=f"{gold_mnt}/gold_joh_a360_generation_status"
)
def gold_joh_a360_generation_status():

    df_stg_joh_filtered = dlt.read("stg_joh_filtered")
    df_joh_metadata = dlt.read("silver_archive_metadata")

    
    # Load necessary data from Hive tables
    if initial_Load == False:
        print("Running non initial load")
        df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
        df_joh_metadata = spark.read.table("hive_metastore.ariadm_arm_joh.silver_archive_metadata")

    # Fetch the list of Adjudicator IDs
    adjudicator_ids_list = df_stg_joh_filtered.select('AdjudicatorId').distinct().rdd.flatMap(lambda x: x).collect()

    result_list = []

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(generate_a360_file_for_adjudicator, adjudicator_id, df_joh_metadata): adjudicator_id
            for adjudicator_id in adjudicator_ids_list
        }

        for future in as_completed(futures):
            adjudicator_id = futures[future]
            try:
                file_name, status = future.result()
                result_list.append((int(adjudicator_id), str(file_name), str(status)))
            except Exception as e:
                print(f"Error generating .a360 for Adjudicator ID {adjudicator_id}: {str(e)}")
                result_list.append((int(adjudicator_id), None, f"Error: {str(e)}"))

    if result_list:
        result_df = spark.createDataFrame(result_list, ["AdjudicatorId", "GeneratedFilePath", "Status"])
    else:
        empty_schema = StructType([
            StructField("AdjudicatorId", LongType(), True),
            StructField("GeneratedFilePath", StringType(), True),
            StructField("Status", StringType(), True)
        ])
        result_df = spark.createDataFrame([], empty_schema)

    return result_df


# COMMAND ----------

# %sql
# drop schema hive_metastore.ariadm_arm_joh cascade;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix

# COMMAND ----------

df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")

# Collect the necessary data to the driver
judicial_officer_details_list = df_judicial_officer_details.collect()
other_centres_list = df_other_centres.collect()
roles_list = df_roles.collect()
history_list = df_history.collect()

# COMMAND ----------

 # Step 4: Query the history details
history = df_history.select('HistDate', col('HistType').alias('HistType'), 'UserName', 'Comment').filter(col('AdjudicatorId') == 1406).collect()
print(history)

# COMMAND ----------

filtered_history_list = [row for row in history_list if row['AdjudicatorId'] == 1406]
print(filtered_history_list)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import LongType, StringType, StructType, StructField
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Step 5: Date formatting helper
def format_date_iso(date_value):
    if date_value:
        return datetime.strftime(date_value, "%Y-%m-%d")
    return ""

def format_date(date_value):
    if date_value:
        return datetime.strftime(date_value, "%d/%m/%Y")
    return ""

# Helper function to find data from a list by AdjudicatorId
def find_data_in_list(data_list, adjudicator_id):
    for row in data_list:
        if row['AdjudicatorId'] == adjudicator_id:
            return row
    return None

# Define the function to generate HTML content for a given adjudicator
# Define the function to generate HTML content for a given adjudicator
def generate_html_content(adjudicator_id, judicial_officer_details_list, other_centres_list, roles_list, history_list):
    # Step 1: Find judicial officer details
    judicial_officer_details = find_data_in_list(judicial_officer_details_list, adjudicator_id)
    if not judicial_officer_details:
        print(f"No details found for Adjudicator ID: {adjudicator_id}")
        return None, "No details found"

    # Step 2: Find other centres
    other_centres = [centre for centre in other_centres_list if centre['AdjudicatorId'] == adjudicator_id]

    # Step 3: Find roles
    roles = [role for role in roles_list if role['AdjudicatorId'] == adjudicator_id]

    # Step 4: Find history
    history = [hist for hist in history_list if hist['AdjudicatorId'] == adjudicator_id]

    # Step 4: Read the HTML template
    html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/JOH-Details-no-js-updated-v2.html"
    with open(html_template_path, "r") as f:
        html_template = "".join([l for l in f])

    # Step 6: Convert the Spark Row object to a dictionary
    row_dict = judicial_officer_details.asDict()  # Convert Spark Row object to a dictionary

    # Create a dictionary with the replacements, ensuring all values are strings
    replacements = {
        "{{Surname}}": str(row_dict.get('Surname', '') or ''),
        "{{Title}}": str(row_dict.get('Title', '') or ''),
        "{{Forenames}}": str(row_dict.get('Forenames', '') or ''),
        "{{DateOfBirth}}": format_date_iso(row_dict.get('DateOfBirth')),
        "{{CorrespondenceAddress}}": str(row_dict.get('CorrespondenceAddress', '') or ''),
        "{{Telephone}}": str(row_dict.get('ContactTelephone', '') or ''),
        "{{ContactDetails}}": str(row_dict.get('ContactDetails', '') or ''),
        "{{DesignatedCentre}}": str(row_dict.get('DesignatedCentre', '') or ''),
        "{{EmploymentTerm}}": str(row_dict.get('EmploymentTerm', '') or ''),
        "{{FullTime}}": str(row_dict.get('FullTime', '') or ''),
        "{{IdentityNumber}}": str(row_dict.get('IdentityNumber', '') or ''),
        "{{DateOfRetirement}}": format_date_iso(row_dict.get('DateOfRetirement')),
        "{{ContractEndDate}}": format_date_iso(row_dict.get('ContractEndDate')),
        "{{ContractRenewalDate}}": format_date_iso(row_dict.get('ContractRenewalDate')),
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
        "{{JudicialInstructionsDate}}": format_date_iso(row_dict.get('JudicialInstructionsDate')),
        "{{Notes}}": str(row_dict.get('Notes', '') or ''),
    }

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
    for index, row in enumerate(history, start=1):
        line = f"<tr><td id=\"midpadding\">{format_date(row['HistDate'])}</td><td id=\"midpadding\">{row['HistType']}</td><td id=\"midpadding\">{row['UserName']}</td><td id=\"midpadding\">{row['Comment']}</td></tr>"
        History_Code += line + '\n'
    html_template = html_template.replace(f"{{{{HistoryPlaceHolder}}}}", History_Code)

    # Step 9: Return the transformed HTML content
    return html_template, "Success"

# Delta Live Table for HTML generation status
@dlt.table(
    name="gold_joh_html_generation_status",
    comment="Delta Live Table for Adjudicator HTML Generation Status.",
    path=f"{gold_mnt}/gold_joh_html_generation_status"
)
def gold_joh_html_generation_status():
    df_stg_joh_filtered = dlt.read("stg_joh_filtered")
    df_judicial_officer_details = dlt.read("silver_adjudicator_detail")
    df_other_centres = dlt.read("silver_othercentre_detail")
    df_roles = dlt.read("silver_appointment_detail")
    df_history = dlt.read("silver_history_detail")

    # Load the necessary dataframes from Hive metastore if not initial load
    if initial_Load == False:
        print("Running non-initial load")
        df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
        df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
        df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
        df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
        df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")

    # Collect the necessary data to the driver
    judicial_officer_details_list = df_judicial_officer_details.collect()
    other_centres_list = df_other_centres.collect()
    roles_list = df_roles.collect()
    history_list = df_history.collect()

    # Broadcast the collected data
    judicial_officer_details_bc = spark.sparkContext.broadcast(judicial_officer_details_list)
    other_centres_bc = spark.sparkContext.broadcast(other_centres_list)
    roles_bc = spark.sparkContext.broadcast(roles_list)
    history_bc = spark.sparkContext.broadcast(history_list)

        # Fetch the list of Adjudicator IDs from the table, partitioned by AdjudicatorId
    adjudicator_ids_list = df_stg_joh_filtered.select('AdjudicatorId').distinct().rdd.flatMap(lambda x: x).collect()

    # Repartition the list into 4 parts for parallel processing
    adjudicator_ids_rdd = spark.sparkContext.parallelize(adjudicator_ids_list, 8 )

    # Process each partition (subset of Adjudicator IDs) in parallel using mapPartitions
    def process_partition(partition):
        result_list = []
        for adjudicator_id in partition:
            html_content, status = generate_html_content(
                adjudicator_id, 
                judicial_officer_details_bc.value, 
                other_centres_bc.value, 
                roles_bc.value, 
                history_bc.value
            )
            result_list.append((adjudicator_id, html_content, status))
        return result_list

    # Use mapPartitions for parallel processing
    result_rdd = adjudicator_ids_rdd.mapPartitions(process_partition)

    # Collect the results from all partitions
    result_list = result_rdd.collect()

    # Unpersist the broadcast variables after processing is complete
    judicial_officer_details_bc.unpersist()
    other_centres_bc.unpersist()
    roles_bc.unpersist()
    history_bc.unpersist()

    # Write the HTML files outside of the Spark job
    for adjudicator_id, html_content, status in result_list:
        if status == "Success":
            file_name = f"{gold_mnt}/HTML/judicial_officer_{adjudicator_id}.html"
            dbutils.fs.put(file_name, html_content, overwrite=True)
            print(f"HTML file created for Adjudicator with ID: {adjudicator_id} at {file_name}")

    # Convert the results list to a DataFrame
    result_df = spark.createDataFrame([(adjudicator_id, f"{gold_mnt}/HTML/judicial_officer_{adjudicator_id}.html", status) for adjudicator_id, _, status in result_list], schema=["AdjudicatorId", "FileName", "Status"])

    return result_df

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import LongType, StringType, StructType, StructField
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Step 5: Date formatting helper
def format_date_iso(date_value):
    if date_value:
        return datetime.strftime(date_value, "%Y-%m-%d")
    return ""

def format_date(date_value):
    if date_value:
        return datetime.strftime(date_value, "%d/%m/%Y")
    return ""

# Define the function to generate HTML content for a given adjudicator
def generate_html_content(adjudicator_id, df_judicial_officer_details, df_other_centres, df_roles, df_history):
    # Step 1: Query the judicial officer details
    try:
        judicial_officer_details = df_judicial_officer_details.filter(col('AdjudicatorId') == adjudicator_id).collect()[0]
    except IndexError:
        print(f"No details found for Adjudicator ID: {adjudicator_id}")
        return None, "No details found"

    # Step 2: Query the other centre details
    other_centres = df_other_centres.select('OtherCentres').filter(col('AdjudicatorId') == adjudicator_id).collect()

    # Step 3: Query the role details
    roles = df_roles.select('Role', 'DateOfAppointment', 'EndDateOfAppointment').filter(col('AdjudicatorId') == adjudicator_id).collect()

    # Step 4: Query the history details
    history = df_history.select('HistDate', col('HistType').alias('HistType'), 'UserName', 'Comment').filter(col('AdjudicatorId') == adjudicator_id).collect()

    # Step 4: Read the HTML template
    html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/JOH-Details-no-js-updated-v2.html"
    with open(html_template_path, "r") as f:
        html_template = "".join([l for l in f])

    # Step 6: Convert the Spark Row object to a dictionary
    row_dict = judicial_officer_details.asDict()  # Convert Spark Row object to a dictionary

    # Create a dictionary with the replacements, ensuring all values are strings
    replacements = {
        "{{Surname}}": str(row_dict.get('Surname', '') or ''),
        "{{Title}}": str(row_dict.get('Title', '') or ''),
        "{{Forenames}}": str(row_dict.get('Forenames', '') or ''),
        "{{DateOfBirth}}": format_date_iso(row_dict.get('DateOfBirth')),
        "{{CorrespondenceAddress}}": str(row_dict.get('CorrespondenceAddress', '') or ''),
        "{{Telephone}}": str(row_dict.get('ContactTelephone', '') or ''),
        "{{ContactDetails}}": str(row_dict.get('ContactDetails', '') or ''),
        "{{DesignatedCentre}}": str(row_dict.get('DesignatedCentre', '') or ''),
        "{{EmploymentTerm}}": str(row_dict.get('EmploymentTerm', '') or ''),
        "{{FullTime}}": str(row_dict.get('FullTime', '') or ''),
        "{{IdentityNumber}}": str(row_dict.get('IdentityNumber', '') or ''),
        "{{DateOfRetirement}}": format_date_iso(row_dict.get('DateOfRetirement')),
        "{{ContractEndDate}}": format_date_iso(row_dict.get('ContractEndDate')),
        "{{ContractRenewalDate}}": format_date_iso(row_dict.get('ContractRenewalDate')),
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
        "{{JudicialInstructionsDate}}": format_date_iso(row_dict.get('JudicialInstructionsDate')),
        "{{Notes}}": str(row_dict.get('Notes', '') or ''),
    }

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
    for index, row in enumerate(history, start=1):
        line = f"<tr><td id=\"midpadding\">{format_date(row['HistDate'])}</td><td id=\"midpadding\">{row['HistType']}</td><td id=\"midpadding\">{row['UserName']}</td><td id=\"midpadding\">{row['Comment']}</td></tr>"
        History_Code += line + '\n'
    html_template = html_template.replace(f"{{{{HistoryPlaceHolder}}}}", History_Code)

    # Step 9: Return the transformed HTML content
    return html_template, "Success"

# Delta Live Table for HTML generation status
@dlt.table(
    name="gold_joh_html_generation_status",
    comment="Delta Live Table for Adjudicator HTML Generation Status.",
    path=f"{gold_mnt}/gold_joh_html_generation_status"
)
def gold_joh_html_generation_status():
    df_stg_joh_filtered = dlt.read("stg_joh_filtered")
    df_judicial_officer_details = dlt.read("silver_adjudicator_detail")
    df_other_centres = dlt.read("silver_othercentre_detail")
    df_roles = dlt.read("silver_appointment_detail")
    df_history = dlt.read("silver_history_detail")

    # Load the necessary dataframes from Hive metastore if not initial load
    if initial_Load == False:
        print("Running non-initial load")
        df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
        df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
        df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
        df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
        df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")

    # Collect the necessary data to the driver
    judicial_officer_details_list = df_judicial_officer_details.collect()
    other_centres_list = df_other_centres.collect()
    roles_list = df_roles.collect()
    history_list = df_history.collect()

    # Broadcast the collected data
    judicial_officer_details_bc = spark.sparkContext.broadcast(judicial_officer_details_list)
    other_centres_bc = spark.sparkContext.broadcast(other_centres_list)
    roles_bc = spark.sparkContext.broadcast(roles_list)
    history_bc = spark.sparkContext.broadcast(history_list)

       # Fetch the list of Adjudicator IDs from the table, partitioned by AdjudicatorId
    adjudicator_ids_list = df_stg_joh_filtered.select('AdjudicatorId').distinct().rdd.flatMap(lambda x: x).collect()

    # Repartition the list into 4 parts for parallel processing
    adjudicator_ids_rdd = spark.sparkContext.parallelize(adjudicator_ids_list, 4)

    # Process each partition (subset of Adjudicator IDs) in parallel using mapPartitions
    def process_partition(partition):
        result_list = []
        for adjudicator_id in partition:
            html_content, status = generate_html_content(
                adjudicator_id, 
                judicial_officer_details_bc.value, 
                other_centres_bc.value, 
                roles_bc.value, 
                history_bc.value
            )
            result_list.append((adjudicator_id, html_content, status))
        return result_list

    # Use mapPartitions for parallel processing
    result_rdd = adjudicator_ids_rdd.mapPartitions(process_partition)

    # Collect the results from all partitions
    result_list = result_rdd.collect()

    # Unpersist the broadcast variables after processing is complete
    judicial_officer_details_bc.unpersist()
    other_centres_bc.unpersist()
    roles_bc.unpersist()
    history_bc.unpersist()

    # Create a DataFrame from the results list
    result_df = spark.createDataFrame(result_list, schema=["AdjudicatorId", "HTMLContent", "Status"])

    # Write the HTML content to the data lake using Spark's DataFrame write method
    for row in result_df.collect():
        if row["Status"] == "Success":
            file_name = f"{gold_mnt}/HTML/judicial_officer_{row['AdjudicatorId']}.html"
            html_df = spark.createDataFrame([(row["HTMLContent"],)], ["HTMLContent"])
            html_df.write.mode("overwrite").text(file_name)
            print(f"HTML file created for Adjudicator with ID: {row['AdjudicatorId']} at {file_name}")

    # Return the result DataFrame
    return result_df.select("AdjudicatorId", "Status")


# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import LongType, StringType, StructType, StructField
from datetime import datetime

# Step 5: Date formatting helper
def format_date_iso(date_value):
    if date_value:
        return datetime.strftime(date_value, "%Y-%m-%d")
    return ""

def format_date(date_value):
    if date_value:
        return datetime.strftime(date_value, "%d/%m/%Y")
    return ""

# Helper function to find data from a list by AdjudicatorId
def find_data_in_list(data_list, adjudicator_id):
    for row in data_list:
        if row['AdjudicatorId'] == adjudicator_id:
            return row
    return None

# Define the function to generate HTML content for a given adjudicator
def generate_html_content(adjudicator_id, judicial_officer_details_list, other_centres_list, roles_list, history_list):
    # Step 1: Find judicial officer details
    judicial_officer_details = find_data_in_list(judicial_officer_details_list, adjudicator_id)
    if not judicial_officer_details:
        print(f"No details found for Adjudicator ID: {adjudicator_id}")
        return None, "No details found"

    # Step 2: Find other centres
    other_centres = [centre for centre in other_centres_list if centre['AdjudicatorId'] == adjudicator_id]

    # Step 3: Find roles
    roles = [role for role in roles_list if role['AdjudicatorId'] == adjudicator_id]

    # Step 4: Find history
    history = [hist for hist in history_list if hist['AdjudicatorId'] == adjudicator_id]

    # Step 4: Read the HTML template
    html_template_path = f"{gold_mnt}/HTML/judicial_officer_{adjudicator_id}.html"
    with open(html_template_path, "r") as f:
        html_template = "".join([l for l in f])

    # Step 6: Convert the Spark Row object to a dictionary
    row_dict = judicial_officer_details.asDict()  # Convert Spark Row object to a dictionary

    # Create a dictionary with the replacements, ensuring all values are strings
    replacements = {
        "{{Surname}}": str(row_dict.get('Surname', '') or ''),
        "{{Title}}": str(row_dict.get('Title', '') or ''),
        "{{Forenames}}": str(row_dict.get('Forenames', '') or ''),
        "{{DateOfBirth}}": format_date_iso(row_dict.get('DateOfBirth')),
        "{{CorrespondenceAddress}}": str(row_dict.get('CorrespondenceAddress', '') or ''),
        "{{Telephone}}": str(row_dict.get('ContactTelephone', '') or ''),
        "{{ContactDetails}}": str(row_dict.get('ContactDetails', '') or ''),
        "{{DesignatedCentre}}": str(row_dict.get('DesignatedCentre', '') or ''),
        "{{EmploymentTerm}}": str(row_dict.get('EmploymentTerm', '') or ''),
        "{{FullTime}}": str(row_dict.get('FullTime', '') or ''),
        "{{IdentityNumber}}": str(row_dict.get('IdentityNumber', '') or ''),
        "{{DateOfRetirement}}": format_date_iso(row_dict.get('DateOfRetirement')),
        "{{ContractEndDate}}": format_date_iso(row_dict.get('ContractEndDate')),
        "{{ContractRenewalDate}}": format_date_iso(row_dict.get('ContractRenewalDate')),
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
        "{{JudicialInstructionsDate}}": format_date_iso(row_dict.get('JudicialInstructionsDate')),
        "{{Notes}}": str(row_dict.get('Notes', '') or ''),
    }

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
    for index, row in enumerate(history, start=1):
        line = f"<tr><td id=\"midpadding\">{format_date(row['HistDate'])}</td><td id=\"midpadding\">{row['HistType']}</td><td id=\"midpadding\">{row['UserName']}</td><td id=\"midpadding\">{row['Comment']}</td></tr>"
        History_Code += line + '\n'
    html_template = html_template.replace(f"{{{{HistoryPlaceHolder}}}}", History_Code)

    # Step 9: Return the transformed HTML content
    return html_template, "Success"

# Delta Live Table for HTML generation status
@dlt.table(
    name="gold_joh_html_generation_status",
    comment="Delta Live Table for Adjudicator HTML Generation Status.",
    path=f"{gold_mnt}/gold_joh_html_generation_status"
)
def gold_joh_html_generation_status():
    df_stg_joh_filtered = dlt.read("stg_joh_filtered")
    df_judicial_officer_details = dlt.read("silver_adjudicator_detail")
    df_other_centres = dlt.read("silver_othercentre_detail")
    df_roles = dlt.read("silver_appointment_detail")
    df_history = dlt.read("silver_history_detail")

    # Load the necessary dataframes from Hive metastore if not initial load
    if initial_Load == False:
        print("Running non-initial load")
        df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
        df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
        df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
        df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
        df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")

    # Collect the necessary data to the driver
    judicial_officer_details_list = df_judicial_officer_details.collect()
    other_centres_list = df_other_centres.collect()
    roles_list = df_roles.collect()
    history_list = df_history.collect()

    # Broadcast the collected data
    judicial_officer_details_bc = spark.sparkContext.broadcast(judicial_officer_details_list)
    other_centres_bc = spark.sparkContext.broadcast(other_centres_list)
    roles_bc = spark.sparkContext.broadcast(roles_list)
    history_bc = spark.sparkContext.broadcast(history_list)

    # Fetch the list of Adjudicator IDs from the table, partitioned by AdjudicatorId
    adjudicator_ids_list = df_stg_joh_filtered.select('AdjudicatorId').distinct().rdd.flatMap(lambda x: x).collect()

    # Repartition by AdjudicatorId to optimize parallel processing
    repartitioned_df = df_stg_joh_filtered.repartition(4, "AdjudicatorId")

    # Define a function to run the HTML generation on each partition
    def process_partition(partition):
        html_results = []
        for row in partition:
            adjudicator_id = row['AdjudicatorId']
            html_content, status = generate_html_content(
                adjudicator_id,
                judicial_officer_details_bc.value,
                other_centres_bc.value,
                roles_bc.value,
                history_bc.value
            )
            html_results.append((adjudicator_id, html_content, status))
        return html_results

    # Parallelize the HTML generation using mapPartitions
    result_rdd = repartitioned_df.rdd.mapPartitions(process_partition)

    # # Collect results
    # results = result_rdd.collect()

    #  # Write results to Azure Data Lake
    # for adjudicator_id, html_content, status in results:
    #     # Define the target path for each Adjudicator's HTML file
    #     target_path = f"{gold_mnt}/HTML/judicial_officer_{adjudicator_id}.html"
    #     dbutils.fs.put(target_path, html_content, overwrite=True)


    # Convert the RDD back to a DataFrame
    schema = StructType([
        StructField("AdjudicatorId", LongType(), True),
        StructField("HTML_Content", StringType(), True),
        StructField("Status", StringType(), True)
    ])
    html_result_df = spark.createDataFrame(result_rdd, schema=schema)

    return html_result_df


# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import LongType, StringType, StructType, StructField
from datetime import datetime

# Step 5: Date formatting helper
def format_date_iso(date_value):
    if date_value:
        return datetime.strftime(date_value, "%Y-%m-%d")
    return ""

def format_date(date_value):
    if date_value:
        return datetime.strftime(date_value, "%d/%m/%Y")
    return ""

# Helper function to find data from a list by AdjudicatorId
def find_data_in_list(data_list, adjudicator_id):
    for row in data_list:
        if row['AdjudicatorId'] == adjudicator_id:
            return row
    return None


# Define the function to generate HTML content for a given adjudicator
def generate_html_content(adjudicator_id, judicial_officer_details_list, other_centres_list, roles_list, history_list):
    # Step 1: Find judicial officer details
    judicial_officer_details = find_data_in_list(judicial_officer_details_list, adjudicator_id)
    if not judicial_officer_details:
        print(f"No details found for Adjudicator ID: {adjudicator_id}")
        return None, "No details found"

    # Step 2: Find other centres
    other_centres = [centre for centre in other_centres_list if centre['AdjudicatorId'] == adjudicator_id]

    # Step 3: Find roles
    roles = [role for role in roles_list if role['AdjudicatorId'] == adjudicator_id]

    # Step 4: Find history
    history = [hist for hist in history_list if hist['AdjudicatorId'] == adjudicator_id]

    # Step 4: Read the HTML template
    html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/JOH-Details-no-js-updated-v2.html"
    with open(html_template_path, "r") as f:
        html_template = "".join([l for l in f])

    # Step 6: Convert the Spark Row object to a dictionary
    row_dict = judicial_officer_details.asDict()  # Convert Spark Row object to a dictionary

    # Create a dictionary with the replacements, ensuring all values are strings
    replacements = {
        "{{Surname}}": str(row_dict.get('Surname', '') or ''),
        "{{Title}}": str(row_dict.get('Title', '') or ''),
        "{{Forenames}}": str(row_dict.get('Forenames', '') or ''),
        "{{DateOfBirth}}": format_date_iso(row_dict.get('DateOfBirth')),
        "{{CorrespondenceAddress}}": str(row_dict.get('CorrespondenceAddress', '') or ''),
        "{{Telephone}}": str(row_dict.get('ContactTelephone', '') or ''),
        "{{ContactDetails}}": str(row_dict.get('ContactDetails', '') or ''),
        "{{DesignatedCentre}}": str(row_dict.get('DesignatedCentre', '') or ''),
        "{{EmploymentTerm}}": str(row_dict.get('EmploymentTerm', '') or ''),
        "{{FullTime}}": str(row_dict.get('FullTime', '') or ''),
        "{{IdentityNumber}}": str(row_dict.get('IdentityNumber', '') or ''),
        "{{DateOfRetirement}}": format_date_iso(row_dict.get('DateOfRetirement')),
        "{{ContractEndDate}}": format_date_iso(row_dict.get('ContractEndDate')),
        "{{ContractRenewalDate}}": format_date_iso(row_dict.get('ContractRenewalDate')),
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
        "{{JudicialInstructionsDate}}": format_date_iso(row_dict.get('JudicialInstructionsDate')),
        "{{Notes}}": str(row_dict.get('Notes', '') or ''),
    }

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
    for index, row in enumerate(history, start=1):
        line = f"<tr><td id=\"midpadding\">{format_date(row['HistDate'])}</td><td id=\"midpadding\">{row['HistType']}</td><td id=\"midpadding\">{row['UserName']}</td><td id=\"midpadding\">{row['Comment']}</td></tr>"
        History_Code += line + '\n'
    html_template = html_template.replace(f"{{{{HistoryPlaceHolder}}}}", History_Code)

    # Step 9: Return the transformed HTML content
    return html_template, "Success"

# Define a function to run the HTML generation for each partition
def process_partition(partition, judicial_officer_details_bc, other_centres_bc, roles_bc, history_bc):
    results = []
    for row in partition:
        adjudicator_id = row['AdjudicatorId']
        html_content, status = generate_html_content(
            adjudicator_id,
            judicial_officer_details_bc.value,
            other_centres_bc.value,
            roles_bc.value,
            history_bc.value
        )
        
        if html_content is None:
            continue
        
        # Instead of writing here, append the result
        results.append((adjudicator_id, html_content, status))
    return results

# Delta Live Table for HTML generation status
@dlt.table(
    name="gold_joh_html_generation_status",
    comment="Delta Live Table for Adjudicator HTML Generation Status.",
    path=f"{gold_mnt}/gold_joh_html_generation_status"
)
def gold_joh_html_generation_status():
    df_stg_joh_filtered = dlt.read("stg_joh_filtered")
    df_judicial_officer_details = dlt.read("silver_adjudicator_detail")
    df_other_centres = dlt.read("silver_othercentre_detail")
    df_roles = dlt.read("silver_appointment_detail")
    df_history = dlt.read("silver_history_detail")

    # Load the necessary dataframes from Hive metastore if not initial load
    if not initial_Load:
        df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
        df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
        df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
        df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
        df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")

    # Broadcast the dataframes to all workers
    judicial_officer_details_bc = sc.broadcast(df_judicial_officer_details.collect())
    other_centres_bc = sc.broadcast(df_other_centres.collect())
    roles_bc = sc.broadcast(df_roles.collect())
    history_bc = sc.broadcast(df_history.collect())

    # Repartition the DataFrame by AdjudicatorId to optimize parallel processing
    repartitioned_df = df_stg_joh_filtered.repartition(4, "AdjudicatorId")

    # Run the HTML generation on each partition
    result_rdd = repartitioned_df.rdd.mapPartitions(lambda partition: process_partition(partition, judicial_officer_details_bc, other_centres_bc, roles_bc, history_bc))

    # Collect results
    results = result_rdd.collect()

    # Write results to Azure Data Lake
    for adjudicator_id, html_content, status in results:
        # Define the target path for each Adjudicator's HTML file
        target_path = f"{gold_mnt}/HTML/judicial_officer_{adjudicator_id}.html"
        dbutils.fs.put(target_path, html_content, overwrite=True)

    # Create DataFrame for output
    schema = StructType([
        StructField("AdjudicatorId", LongType(), True),
        StructField("HTML_Path", StringType(), True),
        StructField("Status", StringType(), True)
    ])
    html_result_df = spark.createDataFrame(results, schema=schema)

    return html_result_df

# COMMAND ----------

# DBTITLE 1,Installing Azure Blob Storage Python Package
pip install azure-storage-blob


# COMMAND ----------

# from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
# import os

# # Set up the BlobServiceClient with your connection string
# connection_string = "your_connection_string"
# blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# # Specify the container name
# container_name = "your_container_name"
# container_client = blob_service_client.get_container_client(container_name)


# COMMAND ----------

# DBTITLE 1,Setting Up Azure Blob Storage in Python
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

# Set up the BlobServiceClient with your connection string
connection_string = "BlobEndpoint=https://ingest00curatedsbox.blob.core.windows.net/;QueueEndpoint=https://ingest00curatedsbox.queue.core.windows.net/;FileEndpoint=https://ingest00curatedsbox.file.core.windows.net/;TableEndpoint=https://ingest00curatedsbox.table.core.windows.net/;SharedAccessSignature=sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-04-01T07:29:30Z&st=2024-10-01T23:29:30Z&spr=https&sig=Mm2wSFM7obaVtMuY5LomoJbrvlgrrdxgAT1nBQemkds%3D"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Specify the container name
container_name = "gold"
container_client = blob_service_client.get_container_client(container_name)


# COMMAND ----------

# DBTITLE 1,Optimized- Generating Judicial Officer Profiles with Python
from pyspark.sql.functions import col
from pyspark.sql.types import LongType, StringType, StructType, StructField
from datetime import datetime

# Step 5: Date formatting helper
def format_date_iso(date_value):
    if date_value:
        return datetime.strftime(date_value, "%Y-%m-%d")
    return ""

def format_date(date_value):
    if date_value:
        return datetime.strftime(date_value, "%d/%m/%Y")
    return ""

# Helper function to find data from a list by AdjudicatorId
def find_data_in_list(data_list, adjudicator_id):
    for row in data_list:
        if row['AdjudicatorId'] == adjudicator_id:
            return row
    return None

# Define the function to generate HTML content for a given adjudicator
def generate_html_content(adjudicator_id, judicial_officer_details_list, other_centres_list, roles_list, history_list):
    # Step 1: Find judicial officer details
    judicial_officer_details = find_data_in_list(judicial_officer_details_list, adjudicator_id)
    if not judicial_officer_details:
        print(f"No details found for Adjudicator ID: {adjudicator_id}")
        return None, "No details found"

    # Step 2: Find other centres
    other_centres = [centre for centre in other_centres_list if centre['AdjudicatorId'] == adjudicator_id]

    # Step 3: Find roles
    roles = [role for role in roles_list if role['AdjudicatorId'] == adjudicator_id]

    # Step 4: Find history
    history = [hist for hist in history_list if hist['AdjudicatorId'] == adjudicator_id]

    # Step 4: Read the HTML template
    html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/JOH-Details-no-js-updated-v2.html"
    with open(html_template_path, "r") as f:
        html_template = "".join([l for l in f])

    # Step 6: Convert the Spark Row object to a dictionary
    row_dict = judicial_officer_details.asDict()  # Convert Spark Row object to a dictionary

    # Create a dictionary with the replacements, ensuring all values are strings
    replacements = {
        "{{Surname}}": str(row_dict.get('Surname', '') or ''),
        "{{Title}}": str(row_dict.get('Title', '') or ''),
        "{{Forenames}}": str(row_dict.get('Forenames', '') or ''),
        "{{DateOfBirth}}": format_date_iso(row_dict.get('DateOfBirth')),
        "{{CorrespondenceAddress}}": str(row_dict.get('CorrespondenceAddress', '') or ''),
        "{{Telephone}}": str(row_dict.get('ContactTelephone', '') or ''),
        "{{ContactDetails}}": str(row_dict.get('ContactDetails', '') or ''),
        "{{DesignatedCentre}}": str(row_dict.get('DesignatedCentre', '') or ''),
        "{{EmploymentTerm}}": str(row_dict.get('EmploymentTerm', '') or ''),
        "{{FullTime}}": str(row_dict.get('FullTime', '') or ''),
        "{{IdentityNumber}}": str(row_dict.get('IdentityNumber', '') or ''),
        "{{DateOfRetirement}}": format_date_iso(row_dict.get('DateOfRetirement')),
        "{{ContractEndDate}}": format_date_iso(row_dict.get('ContractEndDate')),
        "{{ContractRenewalDate}}": format_date_iso(row_dict.get('ContractRenewalDate')),
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
        "{{JudicialInstructionsDate}}": format_date_iso(row_dict.get('JudicialInstructionsDate')),
        "{{Notes}}": str(row_dict.get('Notes', '') or ''),
    }

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
    for index, row in enumerate(history, start=1):
        line = f"<tr><td id=\"midpadding\">{format_date(row['HistDate'])}</td><td id=\"midpadding\">{row['HistType']}</td><td id=\"midpadding\">{row['UserName']}</td><td id=\"midpadding\">{row['Comment']}</td></tr>"
        History_Code += line + '\n'
    html_template = html_template.replace(f"{{{{HistoryPlaceHolder}}}}", History_Code)

    # Step 9: Return the transformed HTML content
    return html_template, "Success"

# Define a function to run the HTML generation for each partition
def process_partition(partition, judicial_officer_details_bc, other_centres_bc, roles_bc, history_bc):
    results = []
    for row in partition:
        adjudicator_id = row['AdjudicatorId']
        html_content, status = generate_html_content(
            adjudicator_id,
            judicial_officer_details_bc.value,
            other_centres_bc.value,
            roles_bc.value,
            history_bc.value
        )
        
        if html_content is None:
            continue
        
        # Define the target path for each Adjudicator's HTML file
        target_path = f"ARIADM/ARM/JOH/HTML/judicial_officer_{adjudicator_id}.html"
        
        # Upload the HTML content to Azure Blob Storage
        blob_client = container_client.get_blob_client(target_path)
        blob_client.upload_blob(html_content, overwrite=True)
        
        # Append the result
        results.append((adjudicator_id, target_path, status))
    return results



@dlt.table(
    name="gold_joh_html_generation_status",
    comment="Delta Live Table for Adjudicator HTML Generation Status.",
    path=f"{gold_mnt}/gold_joh_html_generation_status"
)
def gold_joh_html_generation_status():
    df_stg_joh_filtered = dlt.read("stg_joh_filtered")
    df_judicial_officer_details = dlt.read("silver_adjudicator_detail")
    df_other_centres = dlt.read("silver_othercentre_detail")
    df_roles = dlt.read("silver_appointment_detail")
    df_history = dlt.read("silver_history_detail")

    # Load the necessary dataframes from Hive metastore if not initial load
    if not initial_Load:
        df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
        df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
        df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
        df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
        df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")

    # Broadcast the dataframes to all workers
    judicial_officer_details_bc = sc.broadcast(df_judicial_officer_details.collect())
    other_centres_bc = sc.broadcast(df_other_centres.collect())
    roles_bc = sc.broadcast(df_roles.collect())
    history_bc = sc.broadcast(df_history.collect())

    # Repartition the DataFrame by AdjudicatorId to optimize parallel processing
    repartitioned_df = df_stg_joh_filtered.repartition(31, "AdjudicatorId")

    # Run the HTML generation on each partition
    result_rdd = repartitioned_df.rdd.mapPartitions(lambda partition: process_partition(partition, judicial_officer_details_bc, other_centres_bc, roles_bc, history_bc))

    # Collect results
    results = result_rdd.collect()

    # Create DataFrame for output
    schema = StructType([
        StructField("AdjudicatorId", LongType(), True),
        StructField("HTML_Path", StringType(), True),
        StructField("Status", StringType(), True)
    ])
    html_result_df = spark.createDataFrame(results, schema=schema)

    # Log the HTML paths in the Delta Live Table
    return html_result_df.select("AdjudicatorId", "HTML_Path", "Status")


# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 32)  # Set this to 32 for your 8-worker cluster


# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import LongType, StringType, StructType, StructField
from datetime import datetime

# Step 5: Date formatting helper
def format_date_iso(date_value):
    if date_value:
        return datetime.strftime(date_value, "%Y-%m-%d")
    return ""

def format_date(date_value):
    if date_value:
        return datetime.strftime(date_value, "%d/%m/%Y")
    return ""

# Helper function to find data from a list by AdjudicatorId
def find_data_in_list(data_list, adjudicator_id):
    for row in data_list:
        if row['AdjudicatorId'] == adjudicator_id:
            return row
    return None

# Define the function to generate HTML content for a given adjudicator
def generate_html_content(adjudicator_id, judicial_officer_details_list, other_centres_list, roles_list, history_list):
    # Step 1: Find judicial officer details
    judicial_officer_details = find_data_in_list(judicial_officer_details_list, adjudicator_id)
    if not judicial_officer_details:
        print(f"No details found for Adjudicator ID: {adjudicator_id}")
        return None, "No details found"

    # Step 2: Find other centres
    other_centres = [centre for centre in other_centres_list if centre['AdjudicatorId'] == adjudicator_id]

    # Step 3: Find roles
    roles = [role for role in roles_list if role['AdjudicatorId'] == adjudicator_id]

    # Step 4: Find history
    history = [hist for hist in history_list if hist['AdjudicatorId'] == adjudicator_id]

    # Step 4: Read the HTML template
    html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/JOH-Details-no-js-updated-v2.html"
    with open(html_template_path, "r") as f:
        html_template = "".join([l for l in f])

    # Step 6: Convert the Spark Row object to a dictionary
    row_dict = judicial_officer_details.asDict()  # Convert Spark Row object to a dictionary

    # Create a dictionary with the replacements, ensuring all values are strings
    replacements = {
        "{{Surname}}": str(row_dict.get('Surname', '') or ''),
        "{{Title}}": str(row_dict.get('Title', '') or ''),
        "{{Forenames}}": str(row_dict.get('Forenames', '') or ''),
        "{{DateOfBirth}}": format_date_iso(row_dict.get('DateOfBirth')),
        "{{CorrespondenceAddress}}": str(row_dict.get('CorrespondenceAddress', '') or ''),
        "{{Telephone}}": str(row_dict.get('ContactTelephone', '') or ''),
        "{{ContactDetails}}": str(row_dict.get('ContactDetails', '') or ''),
        "{{DesignatedCentre}}": str(row_dict.get('DesignatedCentre', '') or ''),
        "{{EmploymentTerm}}": str(row_dict.get('EmploymentTerm', '') or ''),
        "{{FullTime}}": str(row_dict.get('FullTime', '') or ''),
        "{{IdentityNumber}}": str(row_dict.get('IdentityNumber', '') or ''),
        "{{DateOfRetirement}}": format_date_iso(row_dict.get('DateOfRetirement')),
        "{{ContractEndDate}}": format_date_iso(row_dict.get('ContractEndDate')),
        "{{ContractRenewalDate}}": format_date_iso(row_dict.get('ContractRenewalDate')),
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
        "{{JudicialInstructionsDate}}": format_date_iso(row_dict.get('JudicialInstructionsDate')),
        "{{Notes}}": str(row_dict.get('Notes', '') or ''),
    }

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
    for index, row in enumerate(history, start=1):
        line = f"<tr><td id=\"midpadding\">{format_date(row['HistDate'])}</td><td id=\"midpadding\">{row['HistType']}</td><td id=\"midpadding\">{row['UserName']}</td><td id=\"midpadding\">{row['Comment']}</td></tr>"
        History_Code += line + '\n'
    html_template = html_template.replace(f"{{{{HistoryPlaceHolder}}}}", History_Code)

    # Step 9: Return the transformed HTML content
    return html_template, "Success"

# Define a function to run the HTML generation for each partition
def process_partition(partition, judicial_officer_details_bc, other_centres_bc, roles_bc, history_bc):
    results = []
    for row in partition:
        adjudicator_id = row['AdjudicatorId']
        html_content, status = generate_html_content(
            adjudicator_id,
            judicial_officer_details_bc.value,
            other_centres_bc.value,
            roles_bc.value,
            history_bc.value
        )
        
        if html_content is None:
            continue
        
        # Define the target path for each Adjudicator's HTML file 
        # Containers specfied in azure-storage-blob connection configuration
        target_path = f"ARIADM/ARM/JOH/HTML/judicial_officer_{adjudicator_id}.html"
        
        # Upload the HTML content to Azure Blob Storage
        blob_client = container_client.get_blob_client(target_path)
        blob_client.upload_blob(html_content, overwrite=True)
        
        # Append the result
        results.append((adjudicator_id, target_path, status))
    return results

@dlt.table(
    name="gold_joh_html_generation_status",
    comment="Delta Live Table for Adjudicator HTML Generation Status.",
    path=f"{gold_mnt}/gold_joh_html_generation_status"
)
def gold_joh_html_generation_status():
    df_stg_joh_filtered = dlt.read("stg_joh_filtered")
    df_judicial_officer_details = dlt.read("silver_adjudicator_detail")
    df_other_centres = dlt.read("silver_othercentre_detail")
    df_roles = dlt.read("silver_appointment_detail")
    df_history = dlt.read("silver_history_detail")

    # Load the necessary dataframes from Hive metastore if not initial load
    if not initial_Load:
        df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
        df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
        df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
        df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
        df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")

    # Step 1: Broadcast necessary lookup DataFrames to optimize performance
    judicial_officer_details_bc = spark.sparkContext.broadcast(df_judicial_officer_details.collect())
    other_centres_bc = spark.sparkContext.broadcast(df_other_centres.collect())
    roles_bc = spark.sparkContext.broadcast(df_roles.collect())
    history_bc = spark.sparkContext.broadcast(df_history.collect())

    # Step 2: Repartition the primary DataFrame based on AdjudicatorId
    num_partitions = 32  # Setting this to optimize for your 8-worker cluster
    repartitioned_df = df_stg_joh_filtered.repartition(num_partitions, "AdjudicatorId")

    # Step 3: Use mapPartitions for parallel HTML generation
    result_rdd = repartitioned_df.rdd.mapPartitions(lambda partition: process_partition(
        partition,
        judicial_officer_details_bc, other_centres_bc, roles_bc, history_bc
    ))

    # Step 4: Convert the RDD results into a DataFrame and return
    result_df = result_rdd.toDF(["AdjudicatorId", "TargetPath", "Status"])

    return result_df


# COMMAND ----------

gold_mnt

# COMMAND ----------

display(dbutils.fs.ls('/mnt/ingest00curatedsboxgold/ARIADM/ARM/JOH/HTML'))

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime

# Date formatting helpers
def format_date_iso(date_value):
    return datetime.strftime(date_value, "%Y-%m-%d") if date_value else ""

def format_date(date_value):
    return datetime.strftime(date_value, "%d/%m/%Y") if date_value else ""

# Function to get data from a list by AdjudicatorId
def find_data_in_list(data_list, adjudicator_id):
    for row in data_list:
        if row['AdjudicatorId'] == adjudicator_id:
            return row
    return None

# Function to generate HTML content for an adjudicator
def generate_html_content(adjudicator_id, judicial_officer_details_list, other_centres_list, roles_list, history_list):
    judicial_officer_details = find_data_in_list(judicial_officer_details_list, adjudicator_id)
    if not judicial_officer_details:
        print(f"No details found for Adjudicator ID: {adjudicator_id}")
        return None, "No details found"

    other_centres = [centre for centre in other_centres_list if centre['AdjudicatorId'] == adjudicator_id]
    roles = [role for role in roles_list if role['AdjudicatorId'] == adjudicator_id]
    history = [hist for hist in history_list if hist['AdjudicatorId'] == adjudicator_id]

    # Reading HTML template
    html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/JOH-Details-no-js-updated-v2.html"
    with open(html_template_path, "r") as f:
        html_template = "".join([l for l in f])

    # Setting up replacements based on the data
    row_dict = judicial_officer_details.asDict()
    replacements = {
        "{{Surname}}": str(row_dict.get('Surname', '') or ''),
        "{{Title}}": str(row_dict.get('Title', '') or ''),
        "{{Forenames}}": str(row_dict.get('Forenames', '') or ''),
        "{{DateOfBirth}}": format_date_iso(row_dict.get('DateOfBirth')),
        # Add more replacements as needed
    }

    # Replacing placeholders in HTML template with actual data
    for key, value in replacements.items():
        html_template = html_template.replace(key, value)

    # Processing other centres
    other_centres_code = ""
    for i, centre in enumerate(other_centres, start=1):
        other_centres_code += f"<tr><td>{i}</td><td>{centre['OtherCentres']}</td></tr>\n"
    html_template = html_template.replace("{{OtherCentre}}", other_centres_code)

    # Processing roles
    roles_code = ""
    for i, role in enumerate(roles, start=1):
        roles_code += f"<tr><td>{i}</td><td>{role['Role']}</td></tr>\n"
    html_template = html_template.replace("{{AppointmentPlaceHolder}}", roles_code)

    # Processing history
    history_code = ""
    for index, row in enumerate(history, start=1):
        history_code += f"<tr><td>{format_date(row['HistDate'])}</td><td>{row['HistType']}</td></tr>\n"
    html_template = html_template.replace("{{HistoryPlaceHolder}}", history_code)

    return html_template, "Success"

# Function to process a partition
def process_partition(partition, judicial_officer_details_bc, other_centres_bc, roles_bc, history_bc):
    results = []
    partition_data = list(partition)

    # Check if partition is empty
    if not partition_data:
        print("Empty partition found")
        return []

    print(f"Processing partition with {len(partition_data)} records")

    for row in partition_data:
        adjudicator_id = row['AdjudicatorId']
        html_content, status = generate_html_content(
            adjudicator_id,
            judicial_officer_details_bc.value,
            other_centres_bc.value,
            roles_bc.value,
            history_bc.value
        )

        if html_content:
            target_path = f"ARIADM/ARM/JOH/HTML/judicial_officer_{adjudicator_id}.html"
            blob_client = container_client.get_blob_client(target_path)
            blob_client.upload_blob(html_content, overwrite=True)
            results.append((adjudicator_id, target_path, status))

    # Return results or placeholder if partition is empty
    return results if results else [(None, None, "No data processed")]

# Function to handle the HTML generation process
def process_html_generation():
    df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
    df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_detail")
    df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_detail")
    df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_appointment_detail")
    df_history = spark.read.table("hive_metastore.ariadm_arm_joh.silver_history_detail")

    # Broadcasting datasets to all workers
    judicial_officer_details_bc = spark.sparkContext.broadcast(df_judicial_officer_details.collect())
    other_centres_bc = spark.sparkContext.broadcast(df_other_centres.collect())
    roles_bc = spark.sparkContext.broadcast(df_roles.collect())
    history_bc = spark.sparkContext.broadcast(df_history.collect())

    # Adjusting partitioning to optimize processing
    num_partitions = 64  # Increase partitions to distribute the workload better
    repartitioned_df = df_stg_joh_filtered.repartition(num_partitions, "AdjudicatorId")

    print(f"Number of partitions after repartitioning: {repartitioned_df.rdd.getNumPartitions()}")

    # Mapping partitions and processing in parallel
    result_rdd = repartitioned_df.rdd.mapPartitions(lambda partition: process_partition(
        partition,
        judicial_officer_details_bc,
        other_centres_bc,
        roles_bc,
        history_bc
    ))

    # Convert the resulting RDD back to a DataFrame
    result_df = result_rdd.toDF(["AdjudicatorId", "TargetPath", "Status"])
    return result_df

# Call the function to start the process
df_result = process_html_generation()
# df_result.count()


# COMMAND ----------

display(df_result)
