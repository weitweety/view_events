# Data Pipeline and CI/CD Demo Project: VIEW EVENTS

## Motivation
This sample project simulates data processing workloads after users interacting with a video advertisement. The following design diagram shows the data flow:

![Image](https://github.com/user-attachments/assets/aeeaa187-1adf-4cb8-9956-af98aee54c41)

We use python faker to generate fake view events and store them into Databricks Volumes. An  ingestor job is then regularly scheduled to check the Volumes for new events and write them to
the Delta Lake table. The second step of this job is an Analyzer which computes the desired statistics from the table for further visualization or buisiness analytics. The Analyzer is still ongoing work.

## Deploy
These steps deploy this project to Databricks:

1. Create a service principal (think of it as a user account created for the CI/CD from Github). Then create a service principal federation policy following [this tutorial](https://docs.databricks.com/aws/en/dev-tools/auth/provider-github). The policy tells Databricks exactly which service (in our case, Github Actions) it should grant access to and the related details. The Github Actions yaml file is included in the project -- it should match the provided details to Databricks (e.g. Entity Type)
2. On Databricks, create a Github repo and link it to the repo under the home directory of the corresponding service principal.
3. Run the workflow by triggering the CI/CD, either manually or automatically depending on the provided setting in the Github Actions yaml. Now the latest commits are synced to Databricks. We could further define the jobs.
4. Define two jobs on Databricks based on the above data pipeline in the diagram. Job templates are to be provided (still ongoing work).