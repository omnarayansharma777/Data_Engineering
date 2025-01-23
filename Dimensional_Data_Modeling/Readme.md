# Dimensional Data Modeling - Actor Films Analysis
This project focuses on creating an efficient dimensional data model for the actor_films dataset. 
By defining data structures, SQL queries, Cumulative Table Design and implementing type 2 slowly changing dimensions (SCD), 
this project supports advanced analysis of actor performance and film history.

---

## Data Overview 
The actor_films dataset contains the following fields:

**actor**: The name of the actor.  
**actorid**: A unique identifier for each actor (Primary Key Component).  
**film**: The name of the film.  
**year**: The release year of the film.  
**votes**: Number of votes the film received.  
**rating**: The rating of the film.  
**filmid**: A unique identifier for each film (Primary Key Component).  

![image](https://github.com/user-attachments/assets/2af797ba-67e4-4fae-88c6-9e5aa020e720)

---

## **Goals of the Project**

### **1. Actors Table**
- Design a dimensional table `actors` to track each actor's performance and activity status.
- Define structured data types, including:
  - Arrays (`films[]`) for storing film details such as film, votes, rating and filmid.
  - ENUMs (`quality_class`) for classifying performance quality into categories like `star`, `good`, `average`, and `bad`.
- Incorporate fields to determine an actor's current activity status (`is_active`).


### **2. Cumulative Data Updates**
- Write a SQL query to populate the `actors` table incrementally, year by year.
- Use logic to:
  - Merge previous year’s data with the current year's records.
  - Update the actor's `quality_class` based on the average rating of their films.
  - Keep track of the actor's active status.


### **3. Slowly Changing Dimensions (SCD)**
- Create an `actors_history_scd` table using **type 2 SCD modeling** to capture historical changes in the following attributes:
  - `quality_class`
  - `is_active`
- Include `start_date` and `end_date` fields to track the validity period of each record.


### **4. Efficient Data Backfill**
- Develop a "backfill" query to populate the `actors_history_scd` table with historical data from the `actors` table.
- Ensure the backfill process is efficient and scalable for large datasets.


### **5. Incremental Updates for SCD**
- Write a SQL query to handle incremental updates for the `actors_history_scd` table.
- The query should:
  - Incorporate new data from the `actors` table.
  - Maintain the historical accuracy of previous records by managing `start_date` and `end_date` fields appropriately.
