# Musical Browser Project

This project aims to create a musical algorithm reducing popularity biases that can be encountered on streaming platforms. 
The goal is to create a more fair browser for artists to be seen, and auditors to discover new titles and genres.

# Tools

This project use data from MusicBrainz and others associated services.
Airflow is used to create DAGs (i.e workflows) to gather data on a periodic time interval.
MinIO buckets are used to store .tar files received from MusicBrainz as well as extracted files.
A postgres DB is used to store gathered data. Utimately, users' requests from the browser will take forms of SQL Lookups on indexed data within the DB.

Using Apache Spark isn't relevant in our context of only dealing with data from MusicBrainz. 
However, it could be used in context of using streaming platforms data even though they tend to restrict shared data to a minimum.

# Project Structure

## Docker Compose
Docker-Compose.yaml file create all necessary container for our application to work. 
To avoid re establishing connections to Postgres DB and MinIO at each restart, we establish these connections directly through a command line in an initialisation container named airflow-init.

## Dags
The dags folder contains python scripts defining our DAGs in Airflow. These are basically our workflows.
We can create some kind of pipelines by triggering a DAG after the successful completion of another (see code).

## Scripts
This folder contains pythons scripts regrouping functions used by the DAGs. One python file for one DAG.
This was done to preserve the readability of the code.
