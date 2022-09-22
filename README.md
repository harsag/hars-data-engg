# hars-data-engg

# Postgres vs Pandas vs Spark

The objective of this project was to compare the performances of these tools while doing data transformations
1. 1 million files were created (1 file for each transaction) and loaded in mongodb 
2. 30 million such transactions were loaded in postgresql table
3. Summary of these 30 million transactions were performed using SQL, Pandas and Spark using different approaches as explained here

# Data architecture

![image](https://user-images.githubusercontent.com/89522672/191738824-bb9138fc-8cbd-4dde-aa9d-1efea42d855c.png)

# Transform using SQL

![image](https://user-images.githubusercontent.com/89522672/191738891-799cf7ef-7699-419d-9d14-c83ca6c544c2.png)

# Summary of various approaches

![image](https://user-images.githubusercontent.com/89522672/191739087-7a6fba1f-f723-4018-8d7d-64f4e805b946.png)

# Conclusions

1. When we have large data, it is best to do the transformations at the server end
2. Reading all data from postgres in Pandas and performing grouping took more time than executing the group by query at the database (server) level
3. Surprisingly Spark SQL is much faster than SQL and Pandas. It took 250+ seconds in SQL to read 30M records but Spark did it in 88 seconds without partitioning and 67 seconds without partitioning
4. Spark was run in a standalone mode with 4 processor (threads) machine. If Spark was used in cluster mode, it will be even faster
5. In Spark SQL, reading from DB and then transforming using Spark SQL was faster (88 seconds) than reading and transforming from DB in single step (198 seconds)
6. When we need to read a large amount of files from file system and create a data frame in Pandas, we should create a list of dfs (one df for each file) and concatenate them into a single df in one go (at the end). This is faster than concatenating all files in a DF one by one.
7. Transformations are very fast in Pandas and Spark. So in case we need heavy transformations on client side, pandas should be used.

Thanks for reading
