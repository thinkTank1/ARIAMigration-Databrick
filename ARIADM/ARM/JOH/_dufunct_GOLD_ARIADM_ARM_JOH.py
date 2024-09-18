# Databricks notebook source
display(dbutils.fs.ls('/mnt/ingest00landingsboxhtml-template/'))

# COMMAND ----------

html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/judicial-info-no-js.html"

with open(html_template_path, "r") as f:
  html_template = "".join([l for l in f])

displayHTML(html=html_template)

# COMMAND ----------

# MAGIC %sql
# MAGIC --OtherCenter details
# MAGIC select AdjudicatorId, OtherCentres from hive_metastore.ariadm_arm_joh.bronze_othercentre_hearingcentre
# MAGIC -- where AdjudicatorId = 1660

# COMMAND ----------

# Step 3: Query the role details
roles = spark.sql(f"""
    SELECT Role, DateOfAppointment, EndDateOfAppointment
    FROM hive_metastore.ariadm_arm_joh.bronze_adjudicator_role
    WHERE AdjudicatorId = 1660
""").collect()

type(roles)

# COMMAND ----------

roles

# COMMAND ----------

for i, role in enumerate(roles):
        if role['Role']:  # Only replace if the value exists
            print(f"replace {{{{Role{i}}}}} with "+ str(role['Role']))

# COMMAND ----------

from datetime import datetime

# Define the function to generate HTML for a given adjudicator
def generate_html_for_adjudicator(adjudicator_id):
    # Step 1: Query the judicial officer details
    judicial_officer_details = spark.sql(f"""
        SELECT 
            AdjudicatorId,
            Surname,
            Forenames,
            Title,
            DateOfBirth,
            CorrespondenceAddress,
            ContactTelephone,
            ContactDetails,
            AvailableAtShortNotice,
            DesignatedCentre,
            EmploymentTerms,
            FullTime,
            IdentityNumber,
            DateOfRetirement,
            ContractEndDate,
            ContractRenewalDate,
            DoNotUse,
            DoNotUseReason,
            JudicialStatus,
            Address1,
            Address2,
            Address3,
            Address4,
            Address5,
            Postcode,
            Telephone,
            Mobile,
            Email,
            BusinessAddress1,
            BusinessAddress2,
            BusinessAddress3,
            BusinessAddress4,
            BusinessAddress5,
            BusinessPostcode,
            BusinessTelephone,
            BusinessFax,
            BusinessEmail,
            JudicialInstructions,
            JudicialInstructionsDate,
            Notes
        FROM hive_metastore.ariadm_arm_joh.bronze_adjudicator_et_hc_dnur
        WHERE AdjudicatorId = {adjudicator_id}
    """).collect()[0]

    # Step 2: Query the other centre details
    other_centres = spark.sql(f"""
        SELECT OtherCentres 
        FROM hive_metastore.ariadm_arm_joh.bronze_othercentre_hearingcentre
        WHERE AdjudicatorId = {adjudicator_id}
    """).collect()

    # Step 3: Query the role details
    roles = spark.sql(f"""
        SELECT Role, DateOfAppointment, EndDateOfAppointment
        FROM hive_metastore.ariadm_arm_joh.bronze_adjudicator_role
        WHERE AdjudicatorId = {adjudicator_id}
    """).collect()

    # Step 4: Read the HTML template
    html_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/judicial-info-no-js.html"
    with open(html_template_path, "r") as f:
        html_template = "".join([l for l in f])

    # Step 5: Replace template placeholders with values from the SQL queries
    def format_date(date_value):
        if date_value:
            return datetime.strftime(date_value, "%Y-%m-%d")
        return "yyyy-MM-dd"

    html_transformed = html_template.replace("{{Surname}}", str(judicial_officer_details['Surname']))
    html_transformed = html_transformed.replace("{{Forenames}}", str(judicial_officer_details['Forenames']))
    html_transformed = html_transformed.replace("{{Title}}", str(judicial_officer_details['Title']))
    html_transformed = html_transformed.replace("{{DateOfBirth}}", format_date(judicial_officer_details['DateOfBirth']))
    html_transformed = html_transformed.replace("{{CorrespondenceAddress}}", str(judicial_officer_details['CorrespondenceAddress']))
    html_transformed = html_transformed.replace("{{Telephone}}", str(judicial_officer_details['ContactTelephone']))
    html_transformed = html_transformed.replace("{{ContactDetails}}", str(judicial_officer_details['ContactDetails']))
    html_transformed = html_transformed.replace("{{AvailableAtShortNotice}}", "Yes" if judicial_officer_details['AvailableAtShortNotice'] else "No")
    html_transformed = html_transformed.replace("{{DesignatedCentre}}", str(judicial_officer_details['DesignatedCentre']))
    html_transformed = html_transformed.replace("{{EmploymentTerm}}", str(judicial_officer_details['EmploymentTerms']))
    html_transformed = html_transformed.replace("{{FullTime}}", "Yes" if judicial_officer_details['FullTime'] else "No")
    html_transformed = html_transformed.replace("{{IdentityNumber}}", str(judicial_officer_details['IdentityNumber']) or "None")
    html_transformed = html_transformed.replace("{{DateOfRetirement}}", format_date(judicial_officer_details['DateOfRetirement']))
    html_transformed = html_transformed.replace("{{ContractEndDate}}", format_date(judicial_officer_details['ContractEndDate']))
    html_transformed = html_transformed.replace("{{ContractRenewalDate}}", format_date(judicial_officer_details['ContractRenewalDate']))
    html_transformed = html_transformed.replace("{{DoNotUse}}", "Yes" if judicial_officer_details['DoNotUse'] else "No")
    html_transformed = html_transformed.replace("{{DoNotUseReason}}", str(judicial_officer_details['DoNotUseReason']) or "None")
    html_transformed = html_transformed.replace("{{JudicialStatus}}", str(judicial_officer_details['JudicialStatus']))
    html_transformed = html_transformed.replace("{{JudicialInstructions}}", str(judicial_officer_details['JudicialInstructions']))
    html_transformed = html_transformed.replace("{{JudicialInstructionsDate}}", format_date(judicial_officer_details['JudicialInstructionsDate']))
    html_transformed = html_transformed.replace("{{Notes}}", str(judicial_officer_details['Notes']))
    html_transformed = html_transformed.replace("{{Mobile}}", str(judicial_officer_details['Mobile']))
    html_transformed = html_transformed.replace("{{Email}}", str(judicial_officer_details['Email']))

    
    # Replace addresses
    html_transformed = html_transformed.replace("{{Address1}}", str(judicial_officer_details['Address1']))
    html_transformed = html_transformed.replace("{{Address2}}", str(judicial_officer_details['Address2']))
    html_transformed = html_transformed.replace("{{Address3}}", str(judicial_officer_details['Address3']))
    html_transformed = html_transformed.replace("{{Address4}}", str(judicial_officer_details['Address4']))
    html_transformed = html_transformed.replace("{{Address5}}", str(judicial_officer_details['Address5']))
    html_transformed = html_transformed.replace("{{Postcode}}", str(judicial_officer_details['Postcode']))

    # Business addresses
    html_transformed = html_transformed.replace("{{BusinessAddress1}}", str(judicial_officer_details['BusinessAddress1']))
    html_transformed = html_transformed.replace("{{BusinessAddress2}}", str(judicial_officer_details['BusinessAddress2']))
    html_transformed = html_transformed.replace("{{BusinessAddress3}}", str(judicial_officer_details['BusinessAddress3']))
    html_transformed = html_transformed.replace("{{BusinessAddress4}}", str(judicial_officer_details['BusinessAddress4']))
    html_transformed = html_transformed.replace("{{BusinessAddress5}}", str(judicial_officer_details['BusinessAddress5']))
    html_transformed = html_transformed.replace("{{BusinessPostcode}}", str(judicial_officer_details['BusinessPostcode']))
    html_transformed = html_transformed.replace("{{BusinessTelephone}}", str(judicial_officer_details['BusinessTelephone']))
    html_transformed = html_transformed.replace("{{BusinessFax}}", str(judicial_officer_details['BusinessFax']) or "")
    html_transformed = html_transformed.replace("{{BusinessEmail}}", str(judicial_officer_details['BusinessEmail']) or "")

    # Step 6: Handle multiple rows for Other Centres
    for i, centre in enumerate(other_centres, start=1):
        if centre['OtherCentres']:  # Only replace if the value exists
            html_transformed = html_transformed.replace(f"{{{{OtherCentre{i}}}}}", str(centre['OtherCentres']))
        else:  # If empty, replace with an empty string
            html_transformed = html_transformed.replace(f"{{{{OtherCentre{i}}}}}", "")

    # Clear any remaining placeholders for centers without values
    for i in range(len(other_centres) + 1, 6):
        html_transformed = html_transformed.replace(f"{{{{OtherCentre{i}}}}}", "")

    # Handle roles
    for i, role in enumerate(roles, start=1):
        if role['Role']:  # Only replace if the value exists
            html_transformed = html_transformed.replace(f"{{{{Role{i}}}}}", str(role['Role']))
            html_transformed = html_transformed.replace(f"{{{{Appointment{i}}}}}", format_date(role['DateOfAppointment']))
            html_transformed = html_transformed.replace(f"{{{{EndDate{i}}}}}", format_date(role['EndDateOfAppointment']))
        else:  # If empty, clear the placeholders
            html_transformed = html_transformed.replace(f"{{{{Role{i}}}}}", "")
            html_transformed = html_transformed.replace(f"{{{{Appointment{i}}}}}", "")
            html_transformed = html_transformed.replace(f"{{{{EndDate{i}}}}}", "")

    # Clear any remaining placeholders for roles without values
    for i in range(len(roles) + 1, 5):
        html_transformed = html_transformed.replace(f"{{{{Role{i}}}}}", "")
        html_transformed = html_transformed.replace(f"{{{{Appointment{i}}}}}", "")
        html_transformed = html_transformed.replace(f"{{{{EndDate{i}}}}}", "")
        
    # Step 7: Remove any remaining empty braces
    html_transformed = html_transformed.replace("{{}}", "")

    # Step 8: Return the transformed HTML
    return html_transformed


# Example :
adjudicator_id = 1660
html_output = generate_html_for_adjudicator(adjudicator_id)

# Display the final HTML
displayHTML(html_output)


# COMMAND ----------


file_name = f"/mnt/ingest00curatedsboxbronze/HTML/judicial_officer_{adjudicator_id}.html"
dbutils.fs.put(file_name, html_output, overwrite=True)

print(f"HTML file created for Officer with ID: {'adjudicator_id'} at {file_name}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC             AdjudicatorId,
# MAGIC             Surname,
# MAGIC             Forenames,
# MAGIC             Title,
# MAGIC             DateOfBirth,
# MAGIC             CorrespondenceAddress,
# MAGIC             ContactTelephone,
# MAGIC             ContactDetails,
# MAGIC             AvailableAtShortNotice,
# MAGIC             DesignatedCentre,
# MAGIC             EmploymentTerms,
# MAGIC             FullTime,
# MAGIC             IdentityNumber,
# MAGIC             DateOfRetirement,
# MAGIC             ContractEndDate,
# MAGIC             ContractRenewalDate,
# MAGIC             DoNotUse,
# MAGIC             DoNotUseReason,
# MAGIC             JudicialStatus,
# MAGIC             Address1,
# MAGIC             Address2,
# MAGIC             Address3,
# MAGIC             Address4,
# MAGIC             Address5,
# MAGIC             Postcode,
# MAGIC             Telephone,
# MAGIC             Mobile,
# MAGIC             Email,
# MAGIC             BusinessAddress1,
# MAGIC             BusinessAddress2,
# MAGIC             BusinessAddress3,
# MAGIC             BusinessAddress4,
# MAGIC             BusinessAddress5,
# MAGIC             BusinessPostcode,
# MAGIC             BusinessTelephone,
# MAGIC             BusinessFax,
# MAGIC             BusinessEmail,
# MAGIC             JudicialInstructions,
# MAGIC             JudicialInstructionsDate,
# MAGIC             Notes
# MAGIC         FROM hive_metastore.ariadm_arm_joh.bronze_adjudicator_et_hc_dnur
# MAGIC         
