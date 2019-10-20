# Purpose of this database and some background
We are start up called Sparkify, we are online data streaming company. 

I created this database to collect songs data on our company's streaming data as currently we don't have nice dataset to analyse the tendency.

The goal of this analytics is to find out what the users are listening to.


# Regarding database schema and ETL pipeline

About the database schema, I created according to very basic star schema concept. songplays table is fact table, and other tables are dimention tables.

I created etl.py file to process and retreive the neccessary information from each files. Then I converted the data to correct form which can be accomodated within each tables.
Finally I inserted the data into each Postgres SQL tables.


# Files in this repository

* data repositry ... this includes log files (log_data and song_data) with JSON format.
* create_tables.py ... this is the script to create tables.
* etl.ipynb ... this files was used to build basic ETL pipeline and see how it works.
* README.md ... this file
* sql_queries.py ... this file is called within create_tables.py and necessary queries are written here.
* test.ipynb ... this file was used to check the data if they are inserted correctly.
    
    
# How to run scripts
1. Call `create_tables.py` file to create necessary table and insert necessary columns.

    ```
    python create_tables.py
    ```


2. Call `python etl.py` file to transform the data and insert to each tables. Make sure that logs are generated without error.
    ```
    python etl.py
    ```

3. To check the result, you can user `test.ipynb` file and run queries.