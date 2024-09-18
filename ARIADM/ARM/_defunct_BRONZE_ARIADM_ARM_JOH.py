# Databricks notebook source
from pyspark.sql.functions import when, col
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
import dlt

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Latest landing files 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

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
    path="/mnt/ingest00curatedsboxbronze/ARIADM/ARM/bronze_adjudicator_et_hc_dnur"
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
    path="/mnt/ingest00curatedsboxbronze/ARIADM/ARM/bronze_johistory_users" 
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
    path="/mnt/ingest00curatedsboxbronze/ARIADM/ARM/bronze_othercentre_hearingcentre" 
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
    path="/mnt/ingest00curatedsboxbronze/ARIADM/ARM/bronze_adjudicator_role" 
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

# MAGIC
# MAGIC %md
# MAGIC ## Silver DLT Tables Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_adjudicator_et_hc_dnur

# COMMAND ----------

@dlt.table(
    name="silver_adjudicator_et_hc_dnur",
    comment="Delta Live Silver Table for Adjudicator details enhanced with Hearing Centre and DNUR information.",
    path="/mnt/ingest00curatedsboxsilver/ARIADM/ARM/silver_adjudicator_et_hc_dnur"
)
def silver_adjudicator_et_hc_dnur():
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
            col("FullTime"),
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
# MAGIC ### Transformation  silver_johistory_users 

# COMMAND ----------

@dlt.table(
    name="silver_johistory_users",
    comment="Delta Live Silver Table combining JoHistory data with Users information.",
    path="/mnt/ingest00curatedsboxsilver/ARIADM/ARM/silver_johistory_users"
)
def silver_johistory_users():
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
            .otherwise(col("HistType")).alias("HistTypeDescription"),
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
# MAGIC ### Transformation silver_othercentre_hearingcentre 

# COMMAND ----------

@dlt.table(
    name="silver_othercentre_hearingcentre",
    comment="Delta Live silver Table combining OtherCentre data with HearingCentre information.",
    path=f"/mnt/ingest00curatedsboxsilver/ARIADM/ARM/silver_othercentre_hearingcentre"
)
def silver_othercentre_hearingcentre():
    return (dlt.read("bronze_othercentre_hearingcentre"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_adjudicator_role

# COMMAND ----------

@dlt.table(
    name="silver_adjudicator_role",
    comment="Delta Live Silver Table for Adjudicator Role data.",
    path="/mnt/ingest00curatedsboxsilver/ARIADM/ARM/silver_adjudicator_role"
)
def silver_adjudicator_role():
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
# MAGIC ## Generate HTML

# COMMAND ----------

# DBTITLE 1,HTML Content from a Template File
# html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/judicial-info-no-js.html"

# with open(html_template_path, "r") as f:
#   html_template = "".join([l for l in f])

# displayHTML(html=html_template)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Sample Execution

# COMMAND ----------

# from datetime import datetime

# # Define the function to generate HTML for a given adjudicator
# def generate_html_for_adjudicator(adjudicator_id, df_judicial_officer_details, df_other_centres, df_roles):
#     # Step 1: Query the judicial officer details
#     judicial_officer_details = df_judicial_officer_details.filter(col('AdjudicatorId') == adjudicator_id).collect()[0]

#     # Step 2: Query the other centre details
#     other_centres = df_other_centres.select('OtherCentres').filter(col('AdjudicatorId') == adjudicator_id).collect()

#     # Step 3: Query the role details
#     roles = df_roles.select('Role','DateOfAppointment','EndDateOfAppointment').filter(col('AdjudicatorId') == adjudicator_id).collect()

#     # Step 4: Read the HTML template
#     html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/judicial-info-no-js.html"
#     with open(html_template_path, "r") as f:
#         html_template = "".join([l for l in f])

#     # Step 5: Date formatting helper
#     def format_date(date_value):
#         if date_value:
#             return datetime.strftime(date_value, "%Y-%m-%d")
#         return "yyyy-MM-dd"

#     # Step 6: Convert the Spark Row object to a dictionary
#     row_dict = judicial_officer_details.asDict()  # Convert Spark Row object to a dictionary

#     # Create a dictionary with the replacements
#     replacements = {
#         "{{Surname}}": row_dict.get('Surname', ''),
#         "{{Title}}": row_dict.get('Title', ''),
#         "{{Forenames}}": row_dict.get('Forenames', ''),
#         "{{DateOfBirth}}": format_date(row_dict['DateOfBirth']),
#         "{{CorrespondenceAddress}}": str(row_dict.get('CorrespondenceAddress', '')),
#         "{{Telephone}}": str(row_dict.get('ContactTelephone', '')),
#         "{{ContactDetails}}": row_dict.get('ContactDetails', ''),
#         "{{DesignatedCentre}}": row_dict.get('DesignatedCentre', ''),
#         "{{EmploymentTerm}}": row_dict.get('EmploymentTerm', ''),
#         "{{FullTime}}": "Yes" if row_dict.get('FullTime', False) else "No",
#         "{{IdentityNumber}}": str(row_dict.get('IdentityNumber', '')),
#         "{{DoNotUseReason}}": str(row_dict.get('DoNotUseReason', '')),
#         "{{JudicialStatus}}": str(row_dict.get('JudicialStatus', '')),
#         "{{Address1}}": str(row_dict.get('Address1', '')),
#         "{{Address2}}": str(row_dict.get('Address2', '')),
#         "{{Address3}}": str(row_dict.get('Address3', '')),
#         "{{Address4}}": str(row_dict.get('Address4', '')),
#         "{{Address5}}": str(row_dict.get('Address5', '')),
#         "{{Postcode}}": str(row_dict.get('Postcode', '')),
#         "{{Mobile}}": str(row_dict.get('Mobile', '')),
#         "{{Email}}": str(row_dict.get('Email', '')),
#         "{{BusinessAddress1}}": str(row_dict.get('BusinessAddress1', '')),
#         "{{BusinessAddress2}}": str(row_dict.get('BusinessAddress2', '')),
#         "{{BusinessAddress3}}": str(row_dict.get('BusinessAddress3', '')),
#         "{{BusinessAddress4}}": str(row_dict.get('BusinessAddress4', '')),
#         "{{BusinessAddress5}}": str(row_dict.get('BusinessAddress5', '')),
#         "{{BusinessPostcode}}": str(row_dict.get('BusinessPostcode', '')),
#         "{{BusinessTelephone}}": str(row_dict.get('BusinessTelephone', '')),
#         "{{BusinessFax}}": str(row_dict.get('BusinessFax', '')),
#         "{{BusinessEmail}}": str(row_dict.get('BusinessEmail', '')),
#         "{{JudicialInstructions}}": str(row_dict.get('JudicialInstructions', '')),
#         "{{Notes}}": str(row_dict.get('Notes', '')),
#     }

#     # Step 7: Replace placeholders using the replacements dictionary
#     for key, value in replacements.items():
#         html_template = html_template.replace(key, value)

#     # Step 8: Handle multiple rows for Other Centres
#     for i, centre in enumerate(other_centres, start=1):
#         html_template = html_template.replace(f"{{{{OtherCentre{i}}}}}", str(centre['OtherCentres']) if centre['OtherCentres'] else "")

#     for i in range(len(other_centres) + 1, 6):
#         html_template = html_template.replace(f"{{{{OtherCentre{i}}}}}", "")

#     # Handle roles
#     for i, role in enumerate(roles, start=1):
#         html_template = html_template.replace(f"{{{{Role{i}}}}}", str(role['Role']) if role['Role'] else "")
#         html_template = html_template.replace(f"{{{{Appointment{i}}}}}", format_date(role['DateOfAppointment']) if role['DateOfAppointment'] else "")
#         html_template = html_template.replace(f"{{{{EndDate{i}}}}}", format_date(role['EndDateOfAppointment']) if role['EndDateOfAppointment'] else "")

#     for i in range(len(roles) + 1, 5):
#         html_template = html_template.replace(f"{{{{Role{i}}}}}", "")
#         html_template = html_template.replace(f"{{{{Appointment{i}}}}}", "")
#         html_template = html_template.replace(f"{{{{EndDate{i}}}}}", "")

#     # Step 9: Write transformed HTML to lake 
#     file_name = f"/mnt/ingest00curatedsboxsilver/HTML/judicial_officer_{adjudicator_id}.html"
#     dbutils.fs.put(file_name, html_template, overwrite=True)

#     print(f"HTML file created for Adjudicator with ID: {'adjudicator_id'} at {file_name}")

#     # displayHTML(html_template)


#     # Step 10: Return the transformed HTML
#     return html_template

# @dlt.table
# def generate_html_files():
#     # Example usage
#     adjudicator_id = 1660
#     df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_et_hc_dnur")
#     df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_hearingcentre")
#     df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_role")

#     # df_judicial_officer_details = dlt.read("silver_adjudicator_et_hc_dnur")
#     # df_other_centres = dlt.read("silver_othercentre_hearingcentre")
#     # df_roles = dlt.read("silver_adjudicator_role")

#     html_output = generate_html_for_adjudicator(adjudicator_id, df_judicial_officer_details, df_other_centres, df_roles)

#      # Define schema with a single column
#     sample_schema = StructType([
#         StructField("dummyforHTMLexport", StringType(), True)
#     ])
    
#     # Return an empty DataFrame with the specified schema
#     return spark.createDataFrame([], sample_schema)




# COMMAND ----------

# MAGIC %md
# MAGIC ## Function Overview
# MAGIC This section details the function for generating HTML files and includes a sample execution.

# COMMAND ----------



# Define the function to generate HTML for a given adjudicator
def generate_html_for_adjudicator(adjudicator_id, df_judicial_officer_details, df_other_centres, df_roles):
    # Step 1: Query the judicial officer details
    judicial_officer_details = df_judicial_officer_details.filter(col('AdjudicatorId') == adjudicator_id).collect()[0]

    # Step 2: Query the other centre details
    other_centres = df_other_centres.select('OtherCentres').filter(col('AdjudicatorId') == adjudicator_id).collect()

    # Step 3: Query the role details
    roles = df_roles.select('Role','DateOfAppointment','EndDateOfAppointment').filter(col('AdjudicatorId') == adjudicator_id).collect()

    # Step 4: Read the HTML template
    html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/judicial-info-no-js.html"
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
        "{{FullTime}}": "Yes" if row_dict.get('FullTime', False) else "No",
        "{{IdentityNumber}}": str(row_dict.get('IdentityNumber', '') or ''),
        
        "{{DateOfRetirement}}": format_date(row_dict.get('DateOfRetirement')),
        # html_transformed = html_transformed.replace("{{ContractEndDate}}", format_date(judicial_officer_details['ContractEndDate']))
        # html_transformed = html_transformed.replace("{{ContractRenewalDate}}", format_date(judicial_officer_details['ContractRenewalDate']))

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
        # html_transformed = html_transformed.replace("{{JudicialInstructionsDate}}", format_date(judicial_officer_details['JudicialInstructionsDate']))
        "{{Notes}}": str(row_dict.get('Notes', '') or ''),
    }

    # Step 7: Replace placeholders using the replacements dictionary
    for key, value in replacements.items():
        html_template = html_template.replace(key, value)

    # Step 8: Handle multiple rows for Other Centres
    for i, centre in enumerate(other_centres, start=1):
        html_template = html_template.replace(f"{{{{OtherCentre{i}}}}}", str(centre['OtherCentres']) if centre['OtherCentres'] else "")

    for i in range(len(other_centres) + 1, 6):
        html_template = html_template.replace(f"{{{{OtherCentre{i}}}}}", "")

    # Handle roles
    for i, role in enumerate(roles, start=1):
        html_template = html_template.replace(f"{{{{Role{i}}}}}", str(role['Role']) if role['Role'] else "")
        html_template = html_template.replace(f"{{{{Appointment{i}}}}}", format_date(role['DateOfAppointment']) if role['DateOfAppointment'] else "")
        html_template = html_template.replace(f"{{{{EndDate{i}}}}}", format_date(role['EndDateOfAppointment']) if role['EndDateOfAppointment'] else "")

    for i in range(len(roles) + 1, 5):
        html_template = html_template.replace(f"{{{{Role{i}}}}}", "")
        html_template = html_template.replace(f"{{{{Appointment{i}}}}}", "")
        html_template = html_template.replace(f"{{{{EndDate{i}}}}}", "")

    # Step 9: Write transformed HTML to lake 
    file_name = f"/mnt/ingest00curatedsboxsilver/HTML/judicial_officer_{adjudicator_id}.html"
    dbutils.fs.put(file_name, html_template, overwrite=True)

    print(f"HTML file created for Adjudicator with ID: {adjudicator_id} at {file_name}")
     
    displayHTML(html_template)


    # Step 10: Return the transformed HTML
    return html_template

@dlt.table
def generate_html_files():
    # Example usage
    adjudicator_id = 1660
    df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_et_hc_dnur")
    df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_hearingcentre")
    df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_role")

    # df_judicial_officer_details = dlt.read("silver_adjudicator_et_hc_dnur")
    # df_other_centres = dlt.read("silver_othercentre_hearingcentre")
    # df_roles = dlt.read("silver_adjudicator_role")

    html_output = generate_html_for_adjudicator(adjudicator_id, df_judicial_officer_details, df_other_centres, df_roles)

    
    # Return an empty DataFrame with the specified schema
    return dlt.read("silver_adjudicator_et_hc_dnur")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Processing HMTL in Batchs

# COMMAND ----------

# # Step 1: Get the list of distinct adjudicator IDs
# adjudicator_ids_list = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_et_hc_dnur") \
#     .select('AdjudicatorId') \
#     .distinct() \
#     .rdd.flatMap(lambda x: x) \
#     .collect()
# len(adjudicator_ids_list)


# # Step 2: Read necessary DataFrames
# df_judicial_officer_details = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_et_hc_dnur")
# df_other_centres = spark.read.table("hive_metastore.ariadm_arm_joh.silver_othercentre_hearingcentre")
# df_roles = spark.read.table("hive_metastore.ariadm_arm_joh.silver_adjudicator_role")

# # Define a function to process a batch of adjudicator IDs
# def process_batch(adjudicator_ids):
#     for adjudicator_id in adjudicator_ids:
#         try:
#             html_output = generate_html_for_adjudicator(adjudicator_id, df_judicial_officer_details, df_other_centres, df_roles)
#             print(f"Successfully generated HTML for Adjudicator ID: {adjudicator_id}")
#         except Exception as e:
#             print(f"Error processing Adjudicator ID {adjudicator_id}: {e}")

# # Step 3: Define batch size and process in batches
# batch_size = 10  # Define your batch size
# for i in range(0, len(adjudicator_ids_list), batch_size):
#     batch_ids = adjudicator_ids_list[i:i + batch_size]
#     process_batch(batch_ids)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Extracting Table Folder Names from Directory

# COMMAND ----------

# # List the files and folders in the directory
# file_info = dbutils.fs.ls('/mnt/ingest00landingsboxlanding/')

# # Extract folder names without the trailing slash and exclude those containing '$'
# table_folders = [f.name.rstrip('/') for f in file_info if '$' not in f.name]

# # Display the list of table folder names
# display(spark.createDataFrame([(f,) for f in table_folders], ["Folder Names"]))
# display(spark.createDataFrame([("No of folders", len(table_folders))], ["Description", "Count"]))

# COMMAND ----------

# %sql
# drop schema hive_metastore.ariadm_arm_joh cascade
