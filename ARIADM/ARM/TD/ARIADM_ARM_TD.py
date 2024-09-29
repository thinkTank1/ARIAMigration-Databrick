# Databricks notebook source
# MAGIC %md
# MAGIC # Tribunal Decision Archive
# MAGIC <table style = 'float:left;'>
# MAGIC    <tbody>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Name: </b></td>
# MAGIC          <td>ARIADM_ARM_TD</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><b>Description: </b></td>
# MAGIC          <td>Notebook to generate a set of HTML, JSON, and A360 files, each representing the data about Tribunal Decision stored in ARIA.</td>
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
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-124">ARIADM-124</a>/NSA/SEP-2024</td>
# MAGIC          <td>Tribunal Decision : Compete Landing to Bronze Notebook</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-65">ARIADM-65</a>/NSA/SEP-2024</td>
# MAGIC          <td>Tribunal Decision : Compete Bronze silver Notebook</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-125">ARIADM-125</a>/NSA/SEP-2024</td>
# MAGIC          <td>Tribunal Decision : Compete Gold Outputs </td>
# MAGIC       </tr>
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
raw_mnt = "/mnt/ingest00rawsboxraw/ARIADM/ARM/TD"
landing_mnt = "/mnt/ingest00landingsboxlanding/"
bronze_mnt = "/mnt/ingest00curatedsboxbronze/ARIADM/ARM/TD"
silver_mnt = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/TD"
gold_mnt = "/mnt/ingest00curatedsboxgold/ARIADM/ARM/TD"

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
# MAGIC
# MAGIC ```
# MAGIC AppealCase
# MAGIC CaseAppellant
# MAGIC Appellant
# MAGIC FileLocation
# MAGIC Department
# MAGIC HearingCentre
# MAGIC Status
# MAGIC ```

# COMMAND ----------

@dlt.table(
    name="raw_appealcase",
    comment="Delta Live Table ARIA AppealCase.",
    path=f"{raw_mnt}/Raw_AppealCase"
)
def Raw_AppealCase():
    return read_latest_parquet("AppealCase", "tv_AppealCase", "ARIA_ARM_TD")

@dlt.table(
    name="raw_caseappellant",
    comment="Delta Live Table ARIA CaseAppellant.",
    path=f"{raw_mnt}/Raw_CaseAppellant"
)
def Raw_CaseAppellant():
    return read_latest_parquet("CaseAppellant", "tv_CaseAppellant", "ARIA_ARM_JOH")

@dlt.table(
    name="raw_appellant",
    comment="Delta Live Table ARIA Appellant.",
    path=f"{raw_mnt}/Raw_Appellant"
)
def raw_Appellant():
     return read_latest_parquet("Appellant", "tv_Appellant", "ARIA_ARM_JOH")

@dlt.table(
    name="raw_filelocation",
    comment="Delta Live Table ARIA FileLocation.",
    path=f"{raw_mnt}/Raw_FileLocation"
)
def Raw_FileLocation():
    return read_latest_parquet("FileLocation", "tv_FileLocation", "ARIA_ARM_JOH")

@dlt.table(
    name="raw_department",
    comment="Delta Live Table ARIA Department.",
    path=f"{raw_mnt}/Raw_Department"
)
def Raw_Department():
    return read_latest_parquet("Department", "tv_Department", "ARIA_ARM_JOH")

@dlt.table(
    name="raw_hearingcentre",
    comment="Delta Live Table ARIA HearingCentre.",
    path=f"{raw_mnt}/Raw_HearingCentre"
)
def Raw_HearingCentre():
    return read_latest_parquet("ARIAHearingCentre", "tv_HearingCentre", "ARIA_ARM_JOH")

@dlt.table(
    name="raw_status",
    comment="Delta Live Table ARIA Status.",
    path=f"{raw_mnt}/Raw_Status"
)
def Raw_Status():
    return read_latest_parquet("Status", "tv_Status", "ARIA_ARM_JOH_ARA")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Bronze DLT Tables Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation bronze_ac_ca_ant_fl_dt_hc 

# COMMAND ----------

df_raw_appealcase = spark.read.table("hive_metastore.ariadm_arm_td.raw_appealcase")
df_raw_appealcase.createOrReplaceTempView("tv_raw_appealcase")


df_raw_status = spark.read.table("hive_metastore.ariadm_arm_td.raw_status")
df_raw_status.createOrReplaceTempView("tv_raw_status")

df_raw_filelocation = spark.read.table("hive_metastore.ariadm_arm_td.raw_filelocation")
df_raw_filelocation.createOrReplaceTempView("tv_raw_filelocation")

df_raw_appealcase = spark.read.table("hive_metastore.ariadm_arm_td.raw_appealcase")
df_raw_appealcase.createOrReplaceTempView("tv_raw_appealcase")

df_raw_caseappellant = spark.read.table("hive_metastore.ariadm_arm_td.raw_caseappellant")
df_raw_caseappellant.createOrReplaceTempView("tv_raw_caseappellant")

df_raw_appelant = spark.read.table("hive_metastore.ariadm_arm_td.raw_appellant")
df_raw_appelant.createOrReplaceTempView("tv_raw_appelant")

df_raw_department = spark.read.table("hive_metastore.ariadm_arm_td.raw_department")
df_raw_department.createOrReplaceTempView("tv_raw_department")


df_raw_hearingcentre = spark.read.table("hive_metastore.ariadm_arm_td.raw_hearingcentre")
df_raw_hearingcentre.createOrReplaceTempView("tv_raw_hearingcentre")


# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC SELECT ac.CaseNo, 
# MAGIC        a.Forenames, 
# MAGIC        a.Name, 
# MAGIC        a.BirthDate, 
# MAGIC        ac.DestructionDate, -- for those not already destroyed, which date to use here? 
# MAGIC        ac.HORef, 
# MAGIC        a.PortReference, 
# MAGIC        hc.Description, 
# MAGIC        d.Description, 
# MAGIC        fl.Note 
# MAGIC FROM [dbo].[AppealCase] ac 
# MAGIC LEFT OUTER JOIN [dbo].[CaseAppellant] ca 
# MAGIC     ON ac.CaseNo = ca.CaseNo 
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Appellant] a 
# MAGIC     ON ca.AppellantId = a.AppellantId 
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[FileLocation] fl 
# MAGIC     ON ac.CaseNo = fl.CaseNo 
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[Department] d 
# MAGIC     ON fl.DeptId = d.DeptId 
# MAGIC LEFT OUTER JOIN [ARIAREPORTS].[dbo].[HearingCentre] hc 
# MAGIC     ON d.CentreId = hc.CentreId
# MAGIC ```

# COMMAND ----------

@dlt.table(
    name="bronze_ac_ca_ant_fl_dt_hc",
    comment="Delta Live Table combining Appeal Case data with Case Appellant, Appellant, File Location, Department, and Hearing Centre.",
    path=f"{bronze_mnt}/bronze_ac_ca_ant_fl_dt_hc"
)
def bronze_ac_ca_ant_fl_dt_hc():
    return (
        dlt.read("raw_appealcase").alias("ac")
            .join(
                dlt.read("raw_caseappellant").alias("ca"),
                col("ac.CaseNo") == col("ca.CaseNo"),
                "left_outer"
            )
            .join(
                dlt.read("raw_appellant").alias("a"),
                col("ca.AppellantId") == col("a.AppellantId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_filelocation").alias("fl"),
                col("ac.CaseNo") == col("fl.CaseNo"),
                "left_outer"
            )
            .join(
                dlt.read("raw_department").alias("d"),
                col("fl.DeptId") == col("d.DeptId"),
                "left_outer"
            )
            .join(
                dlt.read("raw_hearingcentre").alias("hc"),
                col("d.CentreId") == col("hc.CentreId"),
                "left_outer"
            )
            .select(
                col("ac.CaseNo"),
                col("a.Forenames"),
                col("a.Name"),
                col("a.BirthDate"),
                col("ac.DestructionDate"),
                col("ac.HORef"),
                col("a.PortReference"),
                col("hc.Description").alias("HearingCentreDescription"),
                col("d.Description").alias("DepartmentDescription"),
                col("fl.Note"),
                col("ac.AdtclmnFirstCreatedDatetime"),
                col("ac.AdtclmnModifiedDatetime"),
                col("ac.SourceFileName"),
                col("ac.InsertedByProcessName")
            )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Segmentation query (to be applied in silver):  - stg_td_filtered 
# MAGIC
# MAGIC ```sql
# MAGIC /* 
# MAGIC ARIA Data Segmentation 
# MAGIC Archive 
# MAGIC Tribunal Decisions 
# MAGIC 16/09/2024 
# MAGIC */ 
# MAGIC
# MAGIC SELECT  
# MAGIC     ac.CaseNo 
# MAGIC FROM dbo.AppealCase ac 
# MAGIC LEFT OUTER JOIN ( 
# MAGIC     SELECT MAX(StatusId) max_ID, Caseno 
# MAGIC     FROM dbo.Status 
# MAGIC     WHERE ISNULL(outcome, -1) NOT IN (38,111)  
# MAGIC     AND ISNULL(casestatus, -1) != 17 
# MAGIC     GROUP BY Caseno 
# MAGIC ) AS s ON ac.caseno = s.caseno 
# MAGIC LEFT OUTER JOIN dbo.Status t ON t.caseno = s.caseno AND t.statusID = s.max_ID 
# MAGIC LEFT OUTER JOIN (
# MAGIC     SELECT MAX(StatusID) as Prev_ID, CaseNo  
# MAGIC     FROM dbo.Status WHERE ISNULL(casestatus, -1) NOT IN (52,36) 
# MAGIC     GROUP BY CaseNo
# MAGIC ) AS Prev ON ac.CaseNo = prev.caseNo 
# MAGIC LEFT OUTER JOIN dbo.Status st ON st.caseno = prev.caseno AND st.StatusId = prev.Prev_ID  
# MAGIC LEFT OUTER JOIN dbo.FileLocation fl ON ac.caseNo = fl.caseNo 
# MAGIC WHERE	 
# MAGIC     ac.CaseType = 1 
# MAGIC     AND  
# MAGIC     CASE  
# MAGIC         WHEN ac.CasePrefix IN ('LP','LR', 'LD', 'LH', 'LE' ,'IA') AND ac.HOANRef IS NOT NULL THEN 'Skeleton Case' -- Excluding Skeleton cases 
# MAGIC         WHEN (  t.CaseStatus IN ('40','41','42','43','44','45','53','27','28','29','34','32','33') 
# MAGIC         AND t.Outcome IN ('0','86') ) THEN 'UT Active/Remitted Case' -- Excluding UT Active Cases & UT Remitted cases 
# MAGIC         WHEN fl.DeptId = 519 THEN 'Tribunal Decision' -- All National Archive, File Destroyed Cases 
# MAGIC         WHEN (	t.CaseStatus IS NULL 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 10 AND t.Outcome IN ('0','109','104','82','99','121','27','39') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 46 AND t.Outcome IN ('1','86') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 26 AND t.Outcome IN ('0','27','39','50','40','52','89') 
# MAGIC         OR 
# MAGIC         t.CaseStatus IN ('37','38') AND t.Outcome IN ('39','40','37','50','27','0','5') 
# MAGIC         OR  
# MAGIC         t.CaseStatus = 39 AND t.Outcome IN ('0','86') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 50 AND t.Outcome = 0 
# MAGIC         OR  
# MAGIC         t.CaseStatus IN ('52','36') AND t.Outcome = 0 AND st.DecisionDate IS NULL 
# MAGIC         ) THEN 'Active - CCD' -- Excluding FT Active Appeals 
# MAGIC         WHEN (	t.CaseStatus = 10 AND t.Outcome IN ('13','80','122','25','120','2','105','119') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 46 AND t.Outcome IN ('31','2','50') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 26 AND t.Outcome IN ('80','13','25','1','2') 
# MAGIC         OR 
# MAGIC         t.CaseStatus IN ('37','38') AND t.Outcome IN ('1','2','80','13','25','72','14') 
# MAGIC         OR  
# MAGIC         t.CaseStatus = 39 AND t.Outcome IN ('30','31','25','14') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 51 AND t.Outcome IN ('94','93') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 36 AND t.Outcome = 25 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 52 AND t.Outcome IN ('91','95') AND (st.CaseStatus NOT IN ('37','38','39','17') OR st.CaseStatus IS NULL) 
# MAGIC         ) AND DATEADD(MONTH,6,t.decisiondate) > GETDATE() THEN 'Retain - CCD' 	-- Excluding FT Retained Appeals | Using decision date from final/substantive decision where most recent status is the final/substantive decision 
# MAGIC         WHEN (	t.CaseStatus IN (52, 36) AND t.Outcome = 0 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 36 AND t.Outcome IN ('1','2','50','108') 
# MAGIC         OR 
# MAGIC         t.CaseStatus = 52 AND t.Outcome IN ('91','95') AND st.CaseStatus IN ('37','38','39','17') 
# MAGIC         ) AND DATEADD(MONTH,6,st.decisiondate) > GETDATE() THEN 'Retain - CCD' 	-- Excluding FT Retained Appeals | Using decision date from the Substantive decision where most recent status isn't the final/substantive decision	 
# MAGIC         ELSE 'Tribunal Decision' -- Every other appeal case needs a tribunal decision 
# MAGIC     END = 'Tribunal Decision' -- Filtering for just cases requiring a tribunal decision 
# MAGIC ORDER BY ac.CaseNo
# MAGIC ```

# COMMAND ----------

# import dlt
# from pyspark.sql.functions import col, when, coalesce

@dlt.table(
    name="stg_td_filtered",
    comment="Delta Live Table for appeal cases requiring tribunal decisions.",
    path=f"{bronze_mnt}/stg_td_filtered"
)
def bronze_appeal_case_tribunal_decision():
    # Subquery for the max StatusId and Caseno filtering by outcome and casestatus
    status_subquery = (
        dlt.read("raw_status")
        .filter(
            (col("outcome").isNotNull() & (~col("outcome").isin(38,111))) &
            (col("casestatus").isNotNull() & (col("casestatus") != 17))
        )
        .groupBy("CaseNo")
        .agg({"StatusId": "max"})
        .withColumnRenamed("max(StatusId)", "max_ID")
    )

    # Subquery for the previous status excluding certain casestatus
    prev_subquery = (
        dlt.read("raw_status")
        .filter(
            (col("casestatus").isNotNull() & (col("casestatus").isin(52,36)))
        )
        .groupBy("CaseNo")
        .agg({"StatusId": "max"})
        .withColumnRenamed("max(StatusId)", "Prev_ID")
    )

    # Joining the tables
    result_df = (
        dlt.read("raw_appealcase").alias("ac")
        .join(status_subquery.alias("s"), col("ac.CaseNo") == col("s.CaseNo"), "left_outer")
        .join(dlt.read("raw_status").alias("t"), (col("t.CaseNo") == col("s.CaseNo")) & (col("t.StatusId") == col("s.max_ID")), "left_outer")
        .join(prev_subquery.alias("prev"), col("ac.CaseNo") == col("prev.CaseNo"), "left_outer")
        .join(dlt.read("raw_status").alias("st"), (col("st.CaseNo") == col("prev.CaseNo")) & (col("st.StatusId") == col("prev.Prev_ID")), "left_outer")
        .join(dlt.read("raw_filelocation").alias("fl"), col("ac.CaseNo") == col("fl.CaseNo"), "left_outer")
        .filter(
            (col("ac.CaseType") == 1) &
            (when(
                (col("ac.CasePrefix").isin("LP", "LR", "LD", "LH", "LE", "IA")) & (col("ac.HOANRef").isNotNull()),
                "Skeleton Case"
            )
            .when(
                (col("t.CaseStatus").isin("40", "41", "42", "43", "44", "45", "53", "27", "28", "29", "34", "32", "33")) &
                (col("t.Outcome").isin("0", "86")),
                "UT Active/Remitted Case"
            )
            .when(
                col("fl.DeptId") == 519,
                "Tribunal Decision"
            )
            .when(
                (col("t.CaseStatus").isNull()) |
                ((col("t.CaseStatus") == 10) & (col("t.Outcome").isin("0", "109", "104", "82", "99", "121", "27", "39"))) |
                ((col("t.CaseStatus") == 46) & (col("t.Outcome").isin("1", "86"))) |
                ((col("t.CaseStatus") == 26) & (col("t.Outcome").isin("0", "27", "39", "50", "40", "52", "89"))) |
                ((col("t.CaseStatus").isin("37", "38")) & (col("t.Outcome").isin("39", "40", "37", "50", "27", "0", "5"))) |
                ((col("t.CaseStatus") == 39) & (col("t.Outcome") == "0")) |
                ((col("t.CaseStatus") == 50) & (col("t.Outcome") == "0")) |
                ((col("t.CaseStatus").isin("52", "36")) & (col("t.Outcome") == "0") & col("st.DecisionDate").isNull()),
                "Active - CCD"
            )
            .when(
                (col("t.CaseStatus") == 10) & (col("t.Outcome").isin("13", "80", "122", "25", "120", "2", "105", "119")) |
                (col("t.CaseStatus") == 46) & (col("t.Outcome").isin("31", "2", "50")) |
                (col("t.CaseStatus") == 26) & (col("t.Outcome").isin("80", "13", "25", "1", "2")) |
                (col("t.CaseStatus").isin("37", "38")) & (col("t.Outcome").isin("1", "2", "80", "13", "25", "72", "14")) |
                (col("t.CaseStatus") == 39) & (col("t.Outcome").isin("30", "31", "25", "14")) |
                (col("t.CaseStatus") == 51) & (col("t.Outcome").isin("94", "93")) |
                (col("t.CaseStatus") == 36) & (col("t.Outcome") == 25) |
                (col("t.CaseStatus") == 52) & (col("t.Outcome").isin("91", "95")) &
                (col("st.CaseStatus").isNull() | ~col("st.CaseStatus").isin("37", "38", "39", "17")),
                "Retain - CCD"
            )
            .when(
                (col("t.CaseStatus").isin(52, 36)) & (col("t.Outcome") == 0) |
                (col("t.CaseStatus") == 36) & (col("t.Outcome").isin("1", "2", "50", "108")) |
                (col("t.CaseStatus") == 52) & (col("t.Outcome").isin("91", "95")) & col("st.CaseStatus").isin("37", "38", "39", "17"),
                "Retain - CCD"
            )
            .otherwise("Tribunal Decision") == "Tribunal Decision")
        )
        .select("ac.CaseNo")
        .orderBy("ac.CaseNo")
    )

    return result_df


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Silver DLT Tables Creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation silver_tribunaldecision_detail
# MAGIC
# MAGIC

# COMMAND ----------

@dlt.table(
    name="silver_tribunaldecision_detail",
    comment="Delta Live silver Table for Tribunal Decision information.",
    path=f"{silver_mnt}/silver_tribunaldecision_detail"
)
def silver_tribunaldecision_detail():
    return (dlt.read("bronze_ac_ca_ant_fl_dt_hc").alias("td").join(dlt.read("stg_td_filtered").alias('flt'), col("td.CaseNo") == col("flt.CaseNo"), "inner").select("td.*"))

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
# MAGIC          <td>client_identifier</td>
# MAGIC          <td>CaseNo</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>event_date*</td>
# MAGIC          <td>Date of migration/generation.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>recordDate*</td>
# MAGIC          <td>Date of migration/generation.</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>region*</td>
# MAGIC          <td>GBR</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>publisher*</td>
# MAGIC          <td>ARIA</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>record_class*</td>
# MAGIC          <td>ARIA Tribunal Decision</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>entitlement_tag/td>
# MAGIC          <td>IA_Tribunal</td>
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
# MAGIC          <td>Forename</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>String</td>
# MAGIC          <td>name</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>String</td>
# MAGIC          <td>Birth Date</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>Date</td>
# MAGIC          <td>HO Reference</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC          <td>bf_xxx</td>
# MAGIC          <td>String</td>
# MAGIC          <td>Port Reference</td>
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
        dlt.read("bronze_ac_ca_ant_fl_dt_hc").alias("td").join(dlt.read("stg_td_filtered").alias('flt'), col("td.CaseNo") == col("flt.CaseNo"), "inner").select(
            col('td.CaseNo').alias('client_identifier'),
            date_format(col('td.AdtclmnFirstCreatedDatetime'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_date"),
            date_format(col('td.AdtclmnFirstCreatedDatetime'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("recordDate"),
            lit("GBR").alias("region"),
            lit("ARIA").alias("publisher"),
            lit("ARIA Tribunal Decision").alias("record_class"),
            lit('IA_Tribunal').alias("entitlement_tag"),
            col('td.Forenames').alias('bf_001'),
            col('td.Name').alias('bf_002'),
            col('td.BirthDate').alias('bf_003'),
            col('td.HORef').alias('bf_004'),
            col('td.PortReference').alias('bf_005')
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

from pyspark.sql.functions import col
from datetime import datetime

# Define the function to generate HTML for a given adjudicator
def generate_html_for_tribunaldecision(CaseNo, Forenames, Name,  df_tribunaldecision_detail):
    # Step 1: Query the judicial officer details
    try:
        tribunaldecision_detail = df_tribunaldecision_detail.filter((col('CaseNo') == CaseNo) & (col('Forenames') == Forenames) & (col('Name') == Name) ).collect()[0]
    except IndexError:
        print(f"No details found for CaseNo: {CaseNo}")
        return
    
    # Step 4: Read the HTML template
    html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/TD-Details-no-js-v1.html"
    with open(html_template_path, "r") as f:
        html_template = "".join([l for l in f])

    # Step 5: Date formatting helper
    def format_date(date_value):
        if date_value:
            return datetime.strftime(date_value, "%d/%m/%Y")
        return ""  # Return empty string if date_value is None

    # Step 6: Convert the Spark Row object to a dictionary
    row_dict = tribunaldecision_detail.asDict()  # Convert Spark Row object to a dictionary

    # Create a dictionary with the replacements, ensuring all values are strings
    replacements = {
        "{{Archivedate}}":  format_date(row_dict.get('AdtclmnFirstCreatedDatetime')), 
        "{{CaseNo}}": str(row_dict.get('CaseNo', '') or ''),
        "{{Forenames}}": str(row_dict.get('Forenames', '') or ''),
        "{{Name}}": str(row_dict.get('Name', '') or ''),
        "{{BirthDate}}": format_date(row_dict.get('BirthDate')),
        "{{DestructionDate}}": format_date(row_dict.get('DestructionDate', '') or ''),
        "{{HORef}}": str(row_dict.get('HORef', '') or ''),
        "{{PortReference}}": str(row_dict.get('PortReference', '') or ''),
        "{{HearingCentreDescription}}": str(row_dict.get('HearingCentreDescription', '') or ''),
        "{{DepartmentDescription}}": str(row_dict.get('DepartmentDescription', '') or ''),
        "{{Note}}": str(row_dict.get('Note', '') or '')
    }
    # Print dict
    # print(replacements)


    # Step 7: Replace placeholders using the replacements dictionary
    for key, value in replacements.items():
        html_template = html_template.replace(key, value)

    # Step 9: Write transformed HTML to lake 
    file_name = f"{gold_mnt}/HTML/tribunal_decision_{CaseNo.replace('/', '_')}_{Forenames}_{Name}.html"
    dbutils.fs.put(file_name, html_template, overwrite=True)

    print(f"HTML file created for Adjudicator with ID: {CaseNo} at {file_name}")

    # # # Display the generated HTML
    # if CaseNo == 'NS/00001/2003' and Forenames == 'Marcia' and Name == 'Lenon' :
    #     displayHTML(html_template)

    # Step 10: Return the transformed HTML
    return file_name, "Success"

# Example usage
# CaseNo = 'NS/00001/2003'
# Forenames = 'Marcia'
# Name = 'Lenon'
# # Load the necessary dataframes from Hive metastore
# # df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
# df_tribunaldecision_detail = spark.read.table("hive_metastore.ariadm_arm_td.silver_tribunaldecision_detail")


# generate_html_for_tribunaldecision(CaseNo,Forenames, Name, df_tribunaldecision_detail)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create gold_adjudicator_html_generation_status & Processing TribunalDecision HTML's
# MAGIC This section is to prallel process the HTML and create atracker table with log information.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import col

@dlt.table(
    name="gold_td_html_generation_status",
    comment="Delta Live Table for Gold Tribunal Decision HTML Generation Status.",
    path=f"{gold_mnt}/gold_td_html_generation_status"
)
def gold_td_html_generation_status(initial_Load=False):
    
    # Load the necessary dataframes from Hive metastore
    df_archive_metadata = dlt.read("silver_archive_metadata")
    df_tribunaldecision_detail = dlt.read("silver_tribunaldecision_detail")

    if initial_Load == False:
        print("Running non-initial load")
        df_archive_metadata = spark.read.table("hive_metastore.ariadm_arm_td.silver_archive_metadata")
        df_tribunaldecision_detail = spark.read.table("hive_metastore.ariadm_arm_td.silver_tribunaldecision_detail")

    # Fetch the list of CaseNo, Forenames, and Name from the table
    CaseNo_list = df_archive_metadata.select(col('client_identifier').alias('CaseNo'), 
                                             col('bf_001').alias('Forenames'), 
                                             col('bf_002').alias('Name')).distinct().collect()

    # Create an empty list to store results for the Delta Live Table
    result_list = []

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(generate_html_for_tribunaldecision, row.CaseNo, row.Forenames, row.Name, df_tribunaldecision_detail)
            for row in CaseNo_list
        ]
        
        for future, row in zip(as_completed(futures), CaseNo_list):
            try:
                # Retrieve the result from the future
                result = future.result()
                # Ensure the result contains 2 fields (GeneratedFilePath, Status)
                if result and len(result) == 2:
                    # Append the CaseNo, Forenames, Name from CaseNo_list and the result from generate_html_for_tribunaldecision
                    result_list.append((row.CaseNo, row.Forenames, row.Name, result[0], result[1]))
                else:
                    print(f"Skipping result with incorrect structure: {result}")
            except Exception as e:
                print(f"Error processing future: {str(e)}")

    # Check if results were generated
    if not result_list:
        print("No results generated. Returning an empty DataFrame.")
        # Return an empty DataFrame with the defined schema
        empty_schema = StructType([
            StructField("CaseNo", StringType(), True),
            StructField("Forenames", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("GeneratedFilePath", StringType(), True),
            StructField("Status", StringType(), True)
        ])
        return spark.createDataFrame([], empty_schema)

    # Convert the results list to a DataFrame and return as a Delta Live Table
    result_df = spark.createDataFrame(result_list, ["CaseNo", "Forenames", "Name", "GeneratedFilePath", "Status"])

    return result_df


# COMMAND ----------

# %sql
# select  * from hive_metastore.ariadm_arm_td.gold_td_html_generation_status
# where Status = 'Success'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate JSON

# COMMAND ----------

# MAGIC %md
# MAGIC #### Function Overview
# MAGIC This section details the function for generating Json files and includes a sample execution.

# COMMAND ----------

import json
from pyspark.sql.functions import col
from datetime import datetime

# Define the function to generate JSON for a given adjudicator
def generate_json_for_tribunaldecision(CaseNo,Forenames, Name,  df_tribunaldecision_detail):
     # Step 1: Query the judicial officer details
    try:
        tribunaldecision_detail = df_tribunaldecision_detail.filter((col('CaseNo') == CaseNo) & (col('Forenames') == Forenames) & (col('Name') == Name) ).collect()[0]
    except IndexError:
        print(f"No details found for CaseNo: {CaseNo}")
        return
    

    # Step 5: Date formatting helper
    def format_date(date_value):
        if date_value:
            return datetime.strftime(date_value, "%d/%m/%Y")
        return ""  # Return empty string if date_value is None

    # Step 6: Convert the Spark Row object to a dictionary
    row_dict = tribunaldecision_detail.asDict()  # Convert Spark Row object to a dictionary

    # Create a dictionary for the adjudicator details
    # Create a dictionary for the adjudicator details
    tribunal_decision_data = {
        "Archivedate": format_date(row_dict.get('AdtclmnFirstCreatedDatetime', '')),
        "CaseNo": row_dict.get('CaseNo', ''),
        "Forenames": row_dict.get('Forenames', ''),
        "Name": row_dict.get('Name', ''),
        "BirthDate": format_date(row_dict.get('BirthDate', '')),
        "DestructionDate": format_date(row_dict.get('DestructionDate', '')),
        "HORef": row_dict.get('HORef', ''),
        "PortReference": row_dict.get('PortReference', ''),
        "HearingCentreDescription": row_dict.get('HearingCentreDescription', ''),
        "DepartmentDescription": row_dict.get('DepartmentDescription', ''),
        "Note": row_dict.get('Note', '')
    }

    # Step 9: Write transformed JSON to lake 
    # json_dir = "{gold_mnt}/ARIADM/ARM/JSON"
    file_name = f"{gold_mnt}/JSON/tribunal_decision_{CaseNo.replace('/', '_')}_{Forenames}_{Name}.json"
    
    # Convert the dictionary to a JSON string
    json_content = json.dumps(tribunal_decision_data, indent=4)

    # print(json_content)
    
    # Use dbutils.fs.put to write the JSON to DBFS
    dbutils.fs.put(file_name, json_content, overwrite=True)

    print(f"JSON file created for Adjudicator with ID: {CaseNo} at {file_name}")

    # Step 10: Return the transformed JSON file path
    return file_name, "Success"

# Example usage
CaseNo = 'NS/00001/2003'
Forenames = 'Marcia'
Name = 'Lenon'
# Load the necessary dataframes from Hive metastore
# df_stg_joh_filtered = spark.read.table("hive_metastore.ariadm_arm_joh.stg_joh_filtered")
df_tribunaldecision_detail = spark.read.table("hive_metastore.ariadm_arm_td.silver_tribunaldecision_detail")

generate_json_for_tribunaldecision(CaseNo, Forenames,Name, df_tribunaldecision_detail)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create gold_td_Json_generation_status & Processing TribunalDecision Json's
# MAGIC This section is to prallel process the json and create atracker table with log information.

# COMMAND ----------

# from concurrent.futures import ThreadPoolExecutor, as_completed
# from pyspark.sql.types import LongType

@dlt.table(
    name="gold_td_json_generation_status",
    comment="Delta Live Table for Gold Tribunal Decision json Generation Status.",
    path=f"{gold_mnt}/gold_td_json_generation_status"
)
def gold_td_json_generation_status():
    
    # Load the necessary dataframes from Hive metastore
    df_archive_metadata = dlt.read("silver_archive_metadata")
    df_tribunaldecision_detail = dlt.read("silver_tribunaldecision_detail")

    if initial_Load == False:
        print("Running non-initial load")
        df_archive_metadata = spark.read.table("hive_metastore.ariadm_arm_td.silver_archive_metadata")
        df_tribunaldecision_detail = spark.read.table("hive_metastore.ariadm_arm_td.silver_tribunaldecision_detail")

    # Fetch the list of CaseNo, Forenames, and Name from the table
    CaseNo_list = df_archive_metadata.select(col('client_identifier').alias('CaseNo'), 
                                             col('bf_001').alias('Forenames'), 
                                             col('bf_002').alias('Name')).distinct().collect()

    # Create an empty list to store results for the Delta Live Table
    result_list = []

     # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(generate_json_for_tribunaldecision, row.CaseNo, row.Forenames, row.Name, df_tribunaldecision_detail)
            for row in CaseNo_list
        ]
        
        for future, row in zip(as_completed(futures), CaseNo_list):
            try:
                # Retrieve the result from the future
                result = future.result()
                # Ensure the result contains 2 fields (GeneratedFilePath, Status)
                if result and len(result) == 2:
                    # Append the CaseNo, Forenames, Name from CaseNo_list and the result from generate_html_for_tribunaldecision
                    result_list.append((row.CaseNo, row.Forenames, row.Name, result[0], result[1]))
                else:
                    print(f"Skipping result with incorrect structure: {result}")
            except Exception as e:
                print(f"Error processing future: {str(e)}")

    # Check if results were generated
    if not result_list:
        print("No results generated. Returning an empty DataFrame.")
        # Return an empty DataFrame with the defined schema
        empty_schema = StructType([
            StructField("CaseNo", StringType(), True),
            StructField("Forenames", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("GeneratedFilePath", StringType(), True),
            StructField("Status", StringType(), True)
        ])
        return spark.createDataFrame([], empty_schema)

    # Convert the results list to a DataFrame and return as a Delta Live Table
    result_df = spark.createDataFrame(result_list, ["CaseNo", "Forenames", "Name", "GeneratedFilePath", "Status"])

    return result_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate a360 files

# COMMAND ----------

# MAGIC %md
# MAGIC #### Function Overview
# MAGIC This section details the function for generating A360 files and includes a sample execution.

# COMMAND ----------

#Sample
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
def generate_a360_file_for_tribunaldecision(CaseNo, Forenames, Name, df_archive_metadata):
    try:
        # Query the judicial officer details
        df_metadata  = df_archive_metadata.filter((col('client_identifier') == CaseNo) & (col('bf_001') == Forenames) & (col('bf_002') == Name) ).collect()
    except Exception as e:
        print(f"Error querying Adjudicator ID: {CaseNo}, Error: {str(e)}")
        return None, f"Error querying Adjudicator ID: {CaseNo}: {str(e)}"

    if not df_metadata:  # Check if any rows are returned
        print(f"No details found for Adjudicator ID: {CaseNo}")
        return None, f"No details for Adjudicator ID: {CaseNo}"

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
            "bf_003": format_date(row_dict.get('bf_003', '')),
            "bf_004": format_date(row_dict.get('bf_003')),
            "bf_005": row_dict.get('bf_004', '')
        }
    }

    html_data = {
        "operation": "upload_new_file",
        "relation_id": row_dict.get('client_identifier', ''),
        "file_metadata": {
            "publisher": row_dict.get('publisher', ''),
            "dz_file_name": f"tribunal_decision_{CaseNo.replace('/', '_')}_{Forenames}_{Name}.html",
            "file_tag": "html"
        }
    }

    json_data = {
        "operation": "upload_new_file",
        "relation_id": row_dict.get('client_identifier', ''),
        "file_metadata": {
            "publisher": row_dict.get('publisher', ''),
            "dz_file_name": f"tribunal_decision_{CaseNo.replace('/', '_')}_{Forenames}_{Name}.json",
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
    file_name = f"{gold_mnt}/A360/tribunal_decision_{CaseNo.replace('/', '_')}_{Forenames}_{Name}.a360"
    try:
        dbutils.fs.put(file_name, all_data_str, overwrite=True)
        print(f"A360 file created for Adjudicator with ID: {CaseNo} at {file_name}")
        return file_name, "Success"
    except Exception as e:
        print(f"Error writing file for Adjudicator ID: {CaseNo}: {str(e)}")
        return None, f"Error writing file: {str(e)}"
    
# Example usage
CaseNo = 'NS/00001/2003'
Forenames = 'Marcia'
Name = 'Lenon'

# Load necessary data from Hive tables
df_archive_metadata = spark.read.table("hive_metastore.ariadm_arm_td.silver_archive_metadata")

generate_a360_file_for_tribunaldecision(CaseNo, Forenames, Name, df_archive_metadata)




# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create gold_td_Json_generation_status & Processing TribunalDecision A360's
# MAGIC This section is to prallel process the json and create atracker table with log information.

# COMMAND ----------

# Delta Live Table
@dlt.table(
    name="gold_td_a360_generation_status",
    comment="Delta Live Table for Gold Tribunal Decision .a360 File Generation Status.",
    path=f"{gold_mnt}/gold_td_a360_generation_status"
)
def gold_joh_a360_generation_status():

    df_archive_metadata = dlt.read("silver_archive_metadata")

    # Load necessary data from Hive tables
    if initial_Load == False:
        print("Running non initial load")
        df_archive_metadata = spark.read.table("hive_metastore.ariadm_arm_td.silver_archive_metadata")

     # Fetch the list of CaseNo, Forenames, and Name from the table
    CaseNo_list = df_archive_metadata.select(col('client_identifier').alias('CaseNo'), 
                                             col('bf_001').alias('Forenames'), 
                                             col('bf_002').alias('Name')).distinct().collect()

    result_list = []
  
     # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(generate_a360_file_for_tribunaldecision, row.CaseNo, row.Forenames, row.Name, df_archive_metadata)
            for row in CaseNo_list
        ]
        
        for future, row in zip(as_completed(futures), CaseNo_list):
            try:
                # Retrieve the result from the future
                result = future.result()
                # Ensure the result contains 2 fields (GeneratedFilePath, Status)
                if result and len(result) == 2:
                    # Append the CaseNo, Forenames, Name from CaseNo_list and the result from generate_html_for_tribunaldecision
                    result_list.append((row.CaseNo, row.Forenames, row.Name, result[0], result[1]))
                else:
                    print(f"Skipping result with incorrect structure: {result}")
            except Exception as e:
                print(f"Error processing future {row.CaseNo}: {str(e)}")

    # Check if results were generated
    if not result_list:
        print("No results generated. Returning an empty DataFrame.")
        # Return an empty DataFrame with the defined schema
        empty_schema = StructType([
            StructField("CaseNo", StringType(), True),
            StructField("Forenames", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("GeneratedFilePath", StringType(), True),
            StructField("Status", StringType(), True)
        ])
        return spark.createDataFrame([], empty_schema)

    # Convert the results list to a DataFrame and return as a Delta Live Table
    result_df = spark.createDataFrame(result_list, ["CaseNo", "Forenames", "Name", "GeneratedFilePath", "Status"])

    return result_df


# COMMAND ----------

# %sql
# select * from hive_metastore.ariadm_arm_td.silver_archive_metadata
# where client_identifier not in (
# select CaseNo from hive_metastore.ariadm_arm_td.gold_td_a360_generation_status)
# -- and client_identifier = 'TH/00073/2003'
# -- where Status = 'Success'
