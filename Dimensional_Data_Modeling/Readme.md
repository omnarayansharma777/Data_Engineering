# Dimensional Data Modeling - Actor Films Analysis
This project focuses on creating an efficient dimensional data model for the actor_films dataset. 
By defining data structures, SQL queries, Cumulative Table Design and implementing type 2 slowly changing dimensions (SCD), 
this project supports advanced analysis of actor performance and film history.

## Data Overview 
The actor_films dataset contains the following fields:

1. **actor**: The name of the actor.  
2. **actorid**: A unique identifier for each actor (Primary Key Component).  
3. **film**: The name of the film.  
4. **year**: The release year of the film.  
5. **votes**: Number of votes the film received.  
6. **rating**: The rating of the film.  
7. **filmid**: A unique identifier for each film (Primary Key Component).  


  
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
  - Update the actor's `quality_class` based on the average rating of their most recent year films.
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
 
---
## Code 

### 1. Actors Table
The actors table is designed to store structured and categorized data for efficient querying. The table schema includes:

1. **actorid**: Unique identifier for the actor.
2. **actor**: Actor's name.
3. **current_year**: Year for which the data is valid.
4. **films**: An array of structs containing details about the films (`film`, `votes`, `rating`, `filmid`).

5. **quality_class**: Categorized as follows:
   i) **`star`**: Average rating > 8.  
   ii) **`good`**: Average rating > 7 and ≤ 8.  
   iii) **`average`**: Average rating > 6 and ≤ 7.  
   iv) **`bad`**: Average rating ≤ 6.
7. **is_active**: A BOOLEAN field indicating whether the actor is currently active (making films in the current year).

```
CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating INTEGER,
    filmid TEXT
);
```

```
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');
```
```
CREATE TABLE actors (
    actorid TEXT NOT NULL,
    actor TEXT NOT NULL,
    current_year INTEGER NOT NULL,
    films films[],
    quality_class quality_class NOT NULL,
    is_active BOOLEAN DEFAULT TRUE NOT NULL,
    PRIMARY KEY (actorid, current_year)
);
```

### 2. Cumulative Data Updates

The cumulative query populates the actors table by merging data year by year. It:

  - Combines current-year films with previous data.
  - Updates the quality_class based on the average film rating.
  - Marks actors as active or retains their previous status.

```
WITH previous_year AS (
    SELECT *
    FROM actors
    WHERE current_year = 2022
), this_year AS (
    SELECT actorid, actor, year, film, votes, rating, filmid
    FROM actor_films
    WHERE year = 2023
), film_and_rating AS (
    SELECT
        actorid,
        actor,
        year,
        ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films,
        AVG(rating) AS avg_rating
    FROM this_year
    GROUP BY actorid, actor, year
)
INSERT INTO actors
SELECT
    COALESCE(t.actorid, p.actorid) AS actorid,
    COALESCE(t.actor, p.actor) AS actor,
    COALESCE(t.year, p.current_year + 1) AS current_year,
    CASE
        WHEN p.films IS NULL THEN t.films
        WHEN t.year IS NOT NULL THEN p.films || t.films
        ELSE p.films
    END AS films,
    CASE
        WHEN t.year IS NOT NULL THEN
            CASE
                WHEN t.avg_rating > 8 THEN 'star'
                WHEN t.avg_rating > 7 THEN 'good'
                WHEN t.avg_rating > 6 THEN 'average'
                ELSE 'bad'
            END::quality_class
        ELSE p.quality_class
    END AS quality_class,
    CASE
        WHEN t.year IS NOT NULL THEN TRUE
        ELSE p.is_active
    END AS is_active
FROM film_and_rating t
FULL OUTER JOIN previous_year p
    ON t.actorid = p.actorid;

select * from actors
where current_year<2023;
```
