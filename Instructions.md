Data Engineering Assessment Questions - External
Written Responses
Please select ONE of the following questions to complete.
Email your responses as attachments to your recruiter or interviewer.
Option 1:

Given the CMS provider data metastore, write a script that downloads all data sets related to the theme "Hospitals". 

The column names in the csv headers are currently in mixed case with spaces and special characters.
Convert all column names to snake_case (Example: "Patients' rating of the facility linear mean score" becomes
"patients_rating_of_the_facility_linear_mean_score").  

The csv files should be downloaded and processed in parallel, and the job should be designed to run every day, but only download files that have been modified since the previous run (need to track runs/metadata).  

Please email your code and a sample of your output to your recruiter or interviewer.  Add any additional comments or description below.

https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items

Submission Requirements:
- The job must be written in python and must run on a regular Windows or linux computer (i.e. there shouldn't be anything specific to Databricks, AWS, etc.)
- Include a requirements.txt file if your job uses python packages that do not come with the default python install
Enter your answer


Option 2:

Your friend is starting a restaurant business.  She has ambitions to add franchise locations, and food trucks.
She is "data-hungry" and wants to use data to support her operations / inform her strategy.
Create a data model for her company (you can make up the details).
At minimum, please include a logical and a physical model (you get to make up the tables, relationships, keys, etc).   

NOTE: if you use visio, please convert to a PDF.

Please email your response, and enter any comments below.
Enter your answer
