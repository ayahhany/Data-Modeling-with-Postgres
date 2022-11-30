# Data-Modeling-with-Postgres <img align="left" alt="codeSTACKr | songs" width="70px" src="https://user-images.githubusercontent.com/58150666/185762207-07c1b2b4-783e-4afd-aebc-71ea6e06c794.png"/>

### **Project Overview**
> A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

> They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### In addition to the data files, the project workspace includes six files:
1. `test.ipynb` displays the first few rows of each table to let you check your database.
2. `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
3. `etl.ipynb` reads and processes a single file from `song_data` and `log_data` and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. `etl.py` reads and processes files from `song_data` and `log_data` and loads them into your tables. You can fill this out based on your work in the ETL notebook.
5. `sql_queries.py` contains all your sql queries, and is imported into the last three files above.
6. `README.md` provides discussion on your project.

### **Database schema design and ETL pipeline**:

The star schema has 1 *fact* table (songplays), and 4 *dimension* tables (users, songs, artists, time). `DROP`, `CREATE`, `INSERT`, and `SELECT` queries are defined in **sql_queries.py**. **create_tables.py** uses functions `create_database`, `drop_tables`, and `create_tables` to create the database sparkifydb and the required tables.

![image](https://user-images.githubusercontent.com/58150666/185780553-c3d26d7a-c494-4854-b5e9-ab1efa5f1ebf.png)

Extract, transform, load processes in **etl.py** populate the **songs** and **artists** tables with data derived from the JSON song files, `data/song_data`. Processed data derived from the JSON log files, `data/log_data`, is used to populate **time** and **users** tables. A `SELECT` query collects song and artist id from the **songs** and **artists** tables and combines this with log file derived data to populate the **songplays** fact table.

### **Example queries and results for song play analysis**

Query to count the number of users

`SELECT COUNT(UNIQUE(user_id)) FROM users;`

### **How to run Python Scripts**
![image](https://user-images.githubusercontent.com/58150666/185811208-49ed8e4b-21d1-4ac6-b3db-04715cc8706d.png)

