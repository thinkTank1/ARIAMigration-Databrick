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
# MAGIC       <tr>
# MAGIC     <td style='text-align: left; '><a href="https://tools.hmcts.net/jira/browse/ARIADM-137">ARIADM-137</a>/NSA/16-OCT-2024</td>
# MAGIC     <td>TD: Tune Performance, Refactor Code for Reusability, Manage Broadcast Effectively, Implement Repartitioning Strategy</td>
# MAGIC </tr>
# MAGIC    </tbody>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import packages

# COMMAND ----------

pip install azure-storage-blob


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

# DBTITLE 1,Secret Retrieval for Database Connection
secret = dbutils.secrets.get("ingest00-keyvault-sbox", "ingest00-adls-ingest00curatedsbox-connection-string-sbox")

# COMMAND ----------

# DBTITLE 1,Azure Blob Storage Container Access
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

# Set up the BlobServiceClient with your connection string
connection_string = f"BlobEndpoint=https://ingest00curatedsbox.blob.core.windows.net/;QueueEndpoint=https://ingest00curatedsbox.queue.core.windows.net/;FileEndpoint=https://ingest00curatedsbox.file.core.windows.net/;TableEndpoint=https://ingest00curatedsbox.table.core.windows.net/;SharedAccessSignature={secret}"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Specify the container name
container_name = "gold"
container_client = blob_service_client.get_container_client(container_name)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate HTML
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create gold_td_html_generation_status & Processing TribunalDecision HTML's
# MAGIC This section is to prallel process the HTML and create atracker table with log information.

# COMMAND ----------

# DBTITLE 1,Generating Tribunal decision Profiles in HMTL Outputs
# Helper function to format dates
def format_date_iso(date_value):
    if date_value:
        return datetime.strftime(date_value, "%Y-%m-%d")
    return ""

def format_date(date_value):
    if date_value:
        return datetime.strftime(date_value, "%d/%m/%Y")
    return ""

# Define the function to find data in a list by CaseNo, Forenames, and Name
def find_data_in_list(data_list, CaseNo, Forenames, Name):
    for row in data_list:
        if row['CaseNo'] == CaseNo and row['Forenames'] == Forenames and row['Name'] == Name:
            return row
    return None

# Define the function to generate HTML content for a given Tribunal Decision
def generate_html_content(CaseNo, Forenames, Name, tribunaldecision_detail_list):
    try:
        # Step 1: Find tribunal decision details
        tribunaldecision_detail = find_data_in_list(tribunaldecision_detail_list, CaseNo, Forenames, Name)
        if not tribunaldecision_detail:
            print(f"No details found for CaseNo: {CaseNo}")
            return None, "No details found"

        # Step 2: Read the HTML template
        html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/TD-Details-no-js-v1.html"
        with open(html_template_path, "r") as f:
            html_template = "".join([l for l in f])

        # Step 3: Convert the Spark Row object to a dictionary
        row_dict = tribunaldecision_detail.asDict()

        # Step 4: Create a dictionary with the replacements
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

        # Step 5: Replace placeholders using the replacements dictionary
        for key, value in replacements.items():
            html_template = html_template.replace(key, value)

        # Step 6: Return the transformed HTML content
        return html_template, "Success"

    except Exception as e:
        print(f"Error writing file for CaseNo: {CaseNo}: {str(e)}")
        return None, f"Error writing file: {str(e)}"

# Function to process each partition
def process_partition(partition, tribunaldecision_detail_bc):
    results = []
    for row in partition:
        CaseNo = row['CaseNo']
        Forenames = row['Forenames']
        Name = row['Name']
        html_content, status = generate_html_content(
            CaseNo,
            Forenames,
            Name,
            tribunaldecision_detail_bc.value
        )
        # Generate file path
        if html_content:
            file_name = f"ARIADM/ARM/TD/HTML/tribunal_decision_{CaseNo.replace('/', '_')}_{Forenames}_{Name}.html"
            # upload_to_blob(file_name, html_content)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
            blob_client.upload_blob(html_content, overwrite=True)
            results.append((CaseNo, Forenames, Name, file_name, status))
        else:
            results.append((CaseNo, Forenames, Name, "", status))
            
    return results

# Define the DLT function
@dlt.table(
    name="gold_td_html_generation_status",
    comment="Delta Live Table for Gold Tribunal Decision HTML Generation Status.",
    path=f"{gold_mnt}/gold_td_html_generation_status"
)
def gold_td_html_generation_status():
    
    # Load necessary dataframes from DLT or Hive metastore
    df_archive_metadata = dlt.read("silver_archive_metadata")
    df_tribunaldecision_detail = dlt.read("silver_tribunaldecision_detail")

    if not initial_Load:
        print("Running non-initial load")
        df_archive_metadata = spark.read.table("hive_metastore.ariadm_arm_td.silver_archive_metadata")
        df_tribunaldecision_detail = spark.read.table("hive_metastore.ariadm_arm_td.silver_tribunaldecision_detail")

    # Fetch the list of CaseNo, Forenames, and Name from the archive metadata table
    case_list = df_archive_metadata.select(
        col('client_identifier').alias('CaseNo'), 
        col('bf_001').alias('Forenames'), 
        col('bf_002').alias('Name')
    ).distinct()

    # Broadcast the tribunal decision detail DataFrame for performance
    tribunaldecision_detail_bc = spark.sparkContext.broadcast(df_tribunaldecision_detail.collect())

    # Create an empty list to store results for the Delta Live Table
    result_list = []

     # Apply mapPartitions to process each partition
    result_rdd = case_list.rdd.mapPartitions(
        lambda partition: process_partition(partition, tribunaldecision_detail_bc)
    )

     # Collect the results
    result_list = result_rdd.collect()

    schema = StructType([
            StructField("CaseNo", StringType(), True),
            StructField("Forenames", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("GeneratedFilePath", StringType(), True),
            StructField("Status", StringType(), True)
        ])

    
     # If no results, return an empty DataFrame
    if not result_list:
        print("No results generated. Returning an empty DataFrame.")
        return spark.createDataFrame([], schema)
    
    return spark.createDataFrame(result_list, schema)

   


# COMMAND ----------

# DBTITLE 1,Testing with Thread Pools: HTML Outputs for Future Scope
# # Helper function to format dates
# def format_date_iso(date_value):
#     if date_value:
#         return datetime.strftime(date_value, "%Y-%m-%d")
#     return ""

# def format_date(date_value):
#     if date_value:
#         return datetime.strftime(date_value, "%d/%m/%Y")
#     return ""

# # Define the function to find data in a list by CaseNo, Forenames, and Name
# def find_data_in_list(data_list, CaseNo, Forenames, Name):
#     for row in data_list:
#         if row['CaseNo'] == CaseNo and row['Forenames'] == Forenames and row['Name'] == Name:
#             return row
#     return None

# # Define the function to generate HTML content for a given Tribunal Decision
# def generate_html_content(CaseNo, Forenames, Name, tribunaldecision_detail_list):
#     try:
#         # Step 1: Find tribunal decision details
#         tribunaldecision_detail = find_data_in_list(tribunaldecision_detail_list, CaseNo, Forenames, Name)
#         if not tribunaldecision_detail:
#             print(f"No details found for CaseNo: {CaseNo}")
#             return None, "No details found"

#         # Step 2: Read the HTML template
#         html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/TD-Details-no-js-v1.html"
#         with open(html_template_path, "r") as f:
#             html_template = "".join([l for l in f])

#         # Step 3: Convert the Spark Row object to a dictionary
#         row_dict = tribunaldecision_detail.asDict()

#         # Step 4: Create a dictionary with the replacements
#         replacements = {
#             "{{Archivedate}}":  format_date(row_dict.get('AdtclmnFirstCreatedDatetime')),
#             "{{CaseNo}}": str(row_dict.get('CaseNo', '') or ''),
#             "{{Forenames}}": str(row_dict.get('Forenames', '') or ''),
#             "{{Name}}": str(row_dict.get('Name', '') or ''),
#             "{{BirthDate}}": format_date(row_dict.get('BirthDate')),
#             "{{DestructionDate}}": format_date(row_dict.get('DestructionDate', '') or ''),
#             "{{HORef}}": str(row_dict.get('HORef', '') or ''),
#             "{{PortReference}}": str(row_dict.get('PortReference', '') or ''),
#             "{{HearingCentreDescription}}": str(row_dict.get('HearingCentreDescription', '') or ''),
#             "{{DepartmentDescription}}": str(row_dict.get('DepartmentDescription', '') or ''),
#             "{{Note}}": str(row_dict.get('Note', '') or '')
#         }

#         # Step 5: Replace placeholders using the replacements dictionary
#         for key, value in replacements.items():
#             html_template = html_template.replace(key, value)

#         # Step 6: Return the transformed HTML content
#         return html_template, "Success"

#     except Exception as e:
#         print(f"Error writing file for CaseNo: {CaseNo}: {str(e)}")
#         return None, f"Error writing file: {str(e)}"

# # Function to upload HTML content to Azure Blob Storage
# # def upload_to_blob(file_name, content):
# #     try:
# #         blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
# #         blob_client.upload_blob(content, overwrite=True)
# #         # print(f"File uploaded successfully: {file_name}")
# #     except Exception as e:
# #         print(f"Error uploading file to blob storage: {str(e)}")

# # Function to process each partition
# def process_partition(partition, tribunaldecision_detail_bc):
#     results = []
#     for row in partition:
#         CaseNo = row['CaseNo']
#         Forenames = row['Forenames']
#         Name = row['Name']
#         html_content, status = generate_html_content(
#             CaseNo,
#             Forenames,
#             Name,
#             tribunaldecision_detail_bc.value
#         )
#         # Generate file path
#         if html_content:
#             file_name = f"ARIADM/ARM/TD/HTML/tribunal_decision_{CaseNo.replace('/', '_')}_{Forenames}_{Name}.html"
#             # upload_to_blob(file_name, html_content)
#             blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
#             blob_client.upload_blob(html_content, overwrite=True)
#             results.append((CaseNo, Forenames, Name, file_name, status))
#     return results

# # Define the DLT function
# @dlt.table(
#     name="gold_td_html_generation_status",
#     comment="Delta Live Table for Gold Tribunal Decision HTML Generation Status.",
#     path=f"{gold_mnt}/gold_td_html_generation_status"
# )
# def gold_td_html_generation_status(initial_Load=False):
    
#     # Load necessary dataframes from DLT or Hive metastore
#     df_archive_metadata = dlt.read("silver_archive_metadata")
#     df_tribunaldecision_detail = dlt.read("silver_tribunaldecision_detail")

#     if not initial_Load:
#         print("Running non-initial load")
#         df_archive_metadata = spark.read.table("hive_metastore.ariadm_arm_td.silver_archive_metadata")
#         df_tribunaldecision_detail = spark.read.table("hive_metastore.ariadm_arm_td.silver_tribunaldecision_detail")

#     # Fetch the list of CaseNo, Forenames, and Name from the archive metadata table
#     CaseNo_list = df_archive_metadata.select(
#         col('client_identifier').alias('CaseNo'), 
#         col('bf_001').alias('Forenames'), 
#         col('bf_002').alias('Name')
#     ).distinct().collect()

#     # Broadcast the tribunal decision detail DataFrame for performance
#     tribunaldecision_detail_bc = spark.sparkContext.broadcast(df_tribunaldecision_detail.collect())

#     # Create an empty list to store results for the Delta Live Table
#     result_list = []

#     # Use ThreadPoolExecutor for parallel processing
#     with ThreadPoolExecutor(max_workers=10) as executor:
#         futures = [
#             executor.submit(process_partition, [row], tribunaldecision_detail_bc)
#             for row in CaseNo_list
#         ]
        
#         for future in as_completed(futures):
#             try:
#                 # Retrieve the result from the future
#                 result = future.result()
#                 if result:
#                     result_list.extend(result)
#             except Exception as e:
#                 print(f"Error processing future: {str(e)}")

#     # Check if results were generated
#     if not result_list:
#         print("No results generated. Returning an empty DataFrame.")
#         empty_schema = StructType([
#             StructField("CaseNo", StringType(), True),
#             StructField("Forenames", StringType(), True),
#             StructField("Name", StringType(), True),
#             StructField("GeneratedFilePath", StringType(), True),
#             StructField("Status", StringType(), True)
#         ])
#         return spark.createDataFrame([], empty_schema)

#     # Convert the result list to a DataFrame and return as Delta Live Table
#     result_df = spark.createDataFrame(result_list, ["CaseNo", "Forenames", "Name", "GeneratedFilePath", "Status"])

#     return result_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate JSON

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create gold_td_Json_generation_status & Processing TribunalDecision Json's
# MAGIC This section is to prallel process the json and create atracker table with log information.

# COMMAND ----------

# DBTITLE 1,Generating Tribunal decision Profiles in JSON Outputs
# Function to generate JSON content for a given tribunal decision
def generate_json_for_tribunaldecision(CaseNo, Forenames, Name, tribunaldecision_detail_list):
    try:
        # Find tribunal decision details
        tribunaldecision_detail = find_data_in_list(tribunaldecision_detail_list, CaseNo, Forenames, Name)
        if not tribunaldecision_detail:
            print(f"No details found for CaseNo: {CaseNo}")
            return None, "No details found"
        
        # Convert the Spark Row object to a dictionary
        row_dict = tribunaldecision_detail.asDict()  # Convert Spark Row object to a dictionary

        # Create a dictionary for the tribunal decision details
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

        # Convert the dictionary to a JSON string
        json_content = json.dumps(tribunal_decision_data, indent=4)

        return json_content, "Success"

    except Exception as e:
        print(f"Error writing file for CaseNo: {CaseNo}: {str(e)}")
        return None, f"Error writing file: {str(e)}"

# Function to process each partition of data
def process_partition_json(partition, tribunaldecision_detail_bc):
    results = []

    for row in partition:
        CaseNo = row['CaseNo']
        Forenames = row['Forenames']
        Name = row['Name']

        json_content, status = generate_json_for_tribunaldecision(
            CaseNo,
            Forenames,
            Name,
            tribunaldecision_detail_bc.value
        )

        # Generate file path
        if json_content:
            file_name = f"ARIADM/ARM/TD/JSON/tribunal_decision_{CaseNo.replace('/', '_')}_{Forenames}_{Name}.json"
            # upload_to_blob(file_name, html_content)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
            blob_client.upload_blob(json_content, overwrite=True)
            results.append((CaseNo, Forenames, Name, file_name, status))
        else:
            results.append((CaseNo, Forenames, Name, "", status))

    return results

# Define the DLT function
@dlt.table(
    name="gold_td_json_generation_status",
    comment="Delta Live Table for Gold Tribunal Decision JSON Generation Status.",
    path=f"{gold_mnt}/gold_td_json_generation_status"
)
def gold_td_json_generation_status():
    
    # Load the necessary dataframes from DLT or Hive metastore
    df_archive_metadata = dlt.read("silver_archive_metadata")
    df_tribunaldecision_detail = dlt.read("silver_tribunaldecision_detail")


    if not initial_Load:
        print("Running non-initial load")
        df_archive_metadata = spark.read.table("hive_metastore.ariadm_arm_td.silver_archive_metadata")
        df_tribunaldecision_detail = spark.read.table("hive_metastore.ariadm_arm_td.silver_tribunaldecision_detail")
    
    
    # Fetch the list of CaseNo, Forenames, and Name from the archive metadata table
    case_list = df_archive_metadata.select(
        col('client_identifier').alias('CaseNo'), 
        col('bf_001').alias('Forenames'), 
        col('bf_002').alias('Name')
    ).distinct()

     # Broadcast the tribunal decision detail DataFrame for performance
    tribunaldecision_detail_bc = spark.sparkContext.broadcast(df_tribunaldecision_detail.collect())

    # Create an empty list to store results for the Delta Live Table
    result_list = []

    
     # Apply mapPartitions to process each partition
    result_rdd = case_list.rdd.mapPartitions(
        lambda partition: process_partition_json(partition, tribunaldecision_detail_bc)
    )

     # Collect the results
    result_list = result_rdd.collect()

    schema = StructType([
            StructField("CaseNo", StringType(), True),
            StructField("Forenames", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("GeneratedFilePath", StringType(), True),
            StructField("Status", StringType(), True)
        ])

    
     # If no results, return an empty DataFrame
    if not result_list:
        print("No results generated. Returning an empty DataFrame.")
        return spark.createDataFrame([], schema)
    
    return spark.createDataFrame(result_list, schema)


# COMMAND ----------

# DBTITLE 1,Testing with Thread Pools: JSON Outputs for Future Scope


# # Helper function to format dates
# def format_date(date_value):
#     if date_value:
#         return datetime.strftime(date_value, "%d/%m/%Y")
#     return ""  # Return empty string if date_value is None

# # Function to find data in a list based on CaseNo, Forenames, and Name
# def find_data_in_list(data_list, CaseNo, Forenames, Name):
#     for item in data_list:
#         if item['CaseNo'] == CaseNo and item['Forenames'] == Forenames and item['Name'] == Name:
#             return item
#     return None  # Return None if no match is found

# # Define the function to generate JSON content for a given tribunal decision
# def generate_json_for_tribunaldecision(CaseNo, Forenames, Name, tribunaldecision_detail_list):
#     # Step 1: Find tribunal decision details
#     tribunaldecision_detail = find_data_in_list(tribunaldecision_detail_list, CaseNo, Forenames, Name)
    
#     if not tribunaldecision_detail:
#         print(f"No details found for CaseNo: {CaseNo}")
#         return None, "No details found"

#     # Create a dictionary for the tribunal decision details
#     tribunal_decision_data = {
#         "Archivedate": format_date(tribunaldecision_detail.get('AdtclmnFirstCreatedDatetime', '')),
#         "CaseNo": tribunaldecision_detail.get('CaseNo', ''),
#         "Forenames": tribunaldecision_detail.get('Forenames', ''),
#         "Name": tribunaldecision_detail.get('Name', ''),
#         "BirthDate": format_date(tribunaldecision_detail.get('BirthDate', '')),
#         "DestructionDate": format_date(tribunaldecision_detail.get('DestructionDate', '')),
#         "HORef": tribunaldecision_detail.get('HORef', ''),
#         "PortReference": tribunaldecision_detail.get('PortReference', ''),
#         "HearingCentreDescription": tribunaldecision_detail.get('HearingCentreDescription', ''),
#         "DepartmentDescription": tribunaldecision_detail.get('DepartmentDescription', ''),
#         "Note": tribunaldecision_detail.get('Note', '')
#     }

#     # Convert the dictionary to a JSON string
#     json_content = json.dumps(tribunal_decision_data, indent=4)

#     # Generate the file name
#     file_name = f"ARIADM/ARM/TD/JSON/tribunal_decision_{CaseNo.replace('/', '_')}_{Forenames}_{Name}.json"

#     # Upload JSON content to Azure Blob Storage
#     # upload_to_blob(file_name, json_content)

#     blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
#     blob_client.upload_blob(json_content, overwrite=True)

#     # print(f"JSON file created for Adjudicator with ID: {CaseNo} at {file_name}")

#     # Return the transformed JSON file path
#     return file_name, "Success"

# # # Function to upload JSON content to Azure Blob Storage
# # def upload_to_blob(file_name, content):
# #     try:
# #         blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
# #         blob_client.upload_blob(content, overwrite=True)
# #         # print(f"File uploaded successfully: {file_name}")
# #     except Exception as e:
# #         print(f"Error uploading file to blob storage: {str(e)}")

# # Define the DLT function
# @dlt.table(
#     name="gold_td_json_generation_status",
#     comment="Delta Live Table for Gold Tribunal Decision JSON Generation Status.",
#     path=f"{gold_mnt}/gold_td_json_generation_status"
# )
# def gold_td_json_generation_status(initial_Load=False):
    
#     # Load the necessary dataframes from DLT or Hive metastore
#     df_archive_metadata = dlt.read("silver_archive_metadata")
#     df_tribunaldecision_detail = dlt.read("silver_tribunaldecision_detail")

#     if not initial_Load:
#         print("Running non-initial load")
#         df_archive_metadata = spark.read.table("hive_metastore.ariadm_arm_td.silver_archive_metadata")
#         df_tribunaldecision_detail = spark.read.table("hive_metastore.ariadm_arm_td.silver_tribunaldecision_detail")

#     # Fetch the list of CaseNo, Forenames, and Name from the archive metadata table
#     CaseNo_list = df_archive_metadata.select(
#         col('client_identifier').alias('CaseNo'), 
#         col('bf_001').alias('Forenames'), 
#         col('bf_002').alias('Name')
#     ).distinct().collect()

#     # Convert tribunal decision details DataFrame to a list of dictionaries
#     tribunaldecision_detail_list = [
#         row.asDict() for row in df_tribunaldecision_detail.collect()
#     ]

#     # Create an empty list to store results for the Delta Live Table
#     result_list = []

#     # Use ThreadPoolExecutor for parallel processing
#     with ThreadPoolExecutor(max_workers=10) as executor:
#         futures = [
#             executor.submit(generate_json_for_tribunaldecision, row.CaseNo, row.Forenames, row.Name, tribunaldecision_detail_list)
#             for row in CaseNo_list
#         ]
        
#         for future, row in zip(as_completed(futures), CaseNo_list):
#             try:
#                 # Retrieve the result from the future
#                 result = future.result()
#                 # Ensure the result contains 2 fields (GeneratedFilePath, Status)
#                 if result and len(result) == 2:
#                     # Append the CaseNo, Forenames, Name from CaseNo_list and the result from generate_json_for_tribunaldecision
#                     result_list.append((row.CaseNo, row.Forenames, row.Name, result[0], result[1]))
#                 else:
#                     print(f"Skipping result with incorrect structure: {result}")
#             except Exception as e:
#                 print(f"Error processing future: {str(e)}")

#     # Check if results were generated
#     if not result_list:
#         print("No results generated. Returning an empty DataFrame.")
#         # Return an empty DataFrame with the defined schema
#         empty_schema = StructType([
#             StructField("CaseNo", StringType(), True),
#             StructField("Forenames", StringType(), True),
#             StructField("Name", StringType(), True),
#             StructField("GeneratedFilePath", StringType(), True),
#             StructField("Status", StringType(), True)
#         ])
#         return spark.createDataFrame([], empty_schema)

#     # Convert the results list to a DataFrame and return as a Delta Live Table
#     result_df = spark.createDataFrame(result_list, ["CaseNo", "Forenames", "Name", "GeneratedFilePath", "Status"])

#     return result_df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate a360 files

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create gold_a360_generation_status & Processing TribunalDecision A360's
# MAGIC This section is to prallel process the json and create atracker table with log information.

# COMMAND ----------

# DBTITLE 1,Sample for  reference
#Sample
# {"operation": "create_record","relation_id":"152820","record_metadata":{"publisher":"IADEMO","record_class":"IADEMO","region":"GBR","recordDate":"2023-04-12T00:00:00Z","event_date":"2023-04-12T00:00:00Z","client_identifier":"HU/02287/2021","bf_001":"Orgest","bf_002":"Hoxha","bf_003":"A1234567/001","bf_004":"1990-06-09T00:00:00Z","bf_005":"ABC/12345","bf_010":"2024-01-01T00:00:00Z"}}

# {"operation": "upload_new_file","relation_id":"152820","file_metadata":{"publisher":"IADEMO","dz_file_name":"HU_02287_2021.json","file_tag":"json"}}

# {"operation": "upload_new_file","relation_id":"152820","file_metadata":{"publisher":"IADEMO","dz_file_name":"HU022872021.pdf","file_tag":"pdf"}}
 

# COMMAND ----------

# DBTITLE 1,Generating Tribunal decision Profiles in A360 Outputs
# Function to format dates
def format_date_zulu(date_value):
    if isinstance(date_value, str):  # If the date is already a string, return as is
        return date_value
    elif isinstance(date_value, (datetime,)):
        return datetime.strftime(date_value, "%Y-%m-%dT%H:%M:%SZ")
    return None  # Return None if the date is invalid or not provided

# Function to generate .a360 file for tribunal decisions
def generate_a360_file_for_tribunaldecision(CaseNo, Forenames, Name, metadata_list):
    try:
        # Find the metadata for the case
        row_dict = next((row for row in metadata_list if row['client_identifier'] == CaseNo and row['bf_001'] == Forenames and row['bf_002'] == Name), None)
        if not row_dict:
            print(f"No details found for Case No: {CaseNo}")
            return None, f"No details for Case No: {CaseNo}"

        # Create metadata, HTML, and JSON strings
        metadata_data = {
            "operation": "create_record",
            "relation_id": row_dict['client_identifier'],
            "record_metadata": {
                "publisher": row_dict['publisher'],
                "record_class": row_dict['record_class'],
                "region": row_dict['region'],
                "recordDate": format_date_zulu(row_dict['recordDate']),
                "event_date": format_date_zulu(row_dict['event_date']),
                "client_identifier": row_dict['client_identifier'],
                "bf_001": row_dict['bf_001'],
                "bf_002": row_dict['bf_002'],
                "bf_003": format_date_zulu(row_dict['bf_003']),
                "bf_004": format_date_zulu(row_dict['bf_004']),
                "bf_005": row_dict['bf_005']
            }
        }

        html_data = {
            "operation": "upload_new_file",
            "relation_id": row_dict['client_identifier'],
            "file_metadata": {
                "publisher": row_dict['publisher'],
                "dz_file_name": f"tribunal_decision_{CaseNo.replace('/', '_')}_{Forenames}_{Name}.html",
                "file_tag": "html"
            }
        }

        json_data = {
            "operation": "upload_new_file",
            "relation_id": row_dict['client_identifier'],
            "file_metadata": {
                "publisher": row_dict['publisher'],
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

        # Define the file path for each tribunal decision
        target_path = f"ARIADM/ARM/TD/A360/tribunal_decision_{CaseNo.replace('/', '_')}_{Forenames}_{Name}.a360"
        
        # Upload the content to Azure Blob Storage
        blob_client = container_client.get_blob_client(target_path)
        blob_client.upload_blob(all_data_str, overwrite=True)

        return target_path, "Success"
    except Exception as e:
        print(f"Error writing file for Case No: {CaseNo}: {str(e)}")
        return None, f"Error writing file: {str(e)}"

# Define a function to run the A360 file generation for each partition
def process_partition_a360(partition, metadata_bc,blob_service_client, container_name):
    results = []
    for row in partition:
        CaseNo, Forenames, Name = row['CaseNo'], row['Forenames'], row['Name']

        file_name, status = generate_a360_file_for_tribunaldecision(CaseNo, Forenames, Name, metadata_bc.value)
        
        if file_name is None:
            results.append((CaseNo, Forenames, Name, "", status))
        else:
            results.append((CaseNo, Forenames, Name, file_name, status))
    return results

# Delta Live Table
@dlt.table(
    name="gold_td_a360_generation_status",
    comment="Delta Live Table for Gold Tribunal Decision .a360 File Generation Status.",
    path=f"{gold_mnt}/gold_td_a360_generation_status"
)
def gold_td_a360_generation_status():
    df_td_filtered = dlt.read("stg_td_filtered")
    df_td_metadata = dlt.read("silver_archive_metadata")

    if not initial_Load:
        print("Running non-initial load")
        df_td_filtered = spark.read.table("hive_metastore.ariadm_arm_td.stg_td_filtered")
        df_td_metadata = spark.read.table("hive_metastore.ariadm_arm_td.silver_archive_metadata")
    
    # Fetch the list of CaseNo, Forenames, and Name from the table (as Spark DataFrame)
    CaseNo_df = df_td_metadata.select(col('client_identifier').alias('CaseNo'), 
                                      col('bf_001').alias('Forenames'), 
                                      col('bf_002').alias('Name')).distinct()

    # Repartition by CaseNo for optimized parallel processing
    num_partitions = 32  # Assuming an 8-worker cluster
    repartitioned_df = CaseNo_df.repartition(num_partitions, "CaseNo")

    # Broadcast metadata to all workers (using the Spark DataFrame instead of the collected list)
    metadata_bc = spark.sparkContext.broadcast(df_td_metadata.collect())

    # Run the A360 file generation on each partition
    result_rdd = repartitioned_df.rdd.mapPartitions(lambda partition: process_partition_a360(partition, metadata_bc,blob_service_client, container_name))

    # Collect results
    results = result_rdd.collect()

    # Create DataFrame for output
    schema = StructType([
        StructField("CaseNo", StringType(), True),
        StructField("Forenames", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("GeneratedFilePath", StringType(), True),
        StructField("Status", StringType(), True)
    ])
    a360_result_df = spark.createDataFrame(results, schema=schema)

    # Log the A360 paths in the Delta Live Table
    return a360_result_df.select("CaseNo", "Forenames", "Name", "GeneratedFilePath", "Status")


# COMMAND ----------

# display(spark.read.format("binaryFile").load(f"{gold_mnt}/HTML").count())
# display(spark.read.format("binaryFile").load(f"{gold_mnt}/JSON").count())
# display(spark.read.format("binaryFile").load(f"{gold_mnt}/A360").count())
