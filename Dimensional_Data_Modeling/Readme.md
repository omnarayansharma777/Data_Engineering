# Dimensional Data Modeling - Actor Films Analysis
This project focuses on creating an efficient dimensional data model for the actor_films dataset. 
By defining data structures, SQL queries, Cumulative Table Design and implementing type 2 slowly changing dimensions (SCD), 
this project supports advanced analysis of actor performance and film history.

 **What This Project Will Achieve**  

✅ **Track Actor Performance** – See how actors' careers change over time.  
✅ **Keep a History of Changes** – Record shifts in an actor’s success and activity status.  
✅ **Easy Yearly Updates** – Add new data without affecting past records.  
✅ **Faster Data Searches** – Quickly find insights on actors and films.  
✅ **Ready for Reports & Analysis** – Well-structured data for charts, reports, and deeper insights.

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
    WHERE current_year = 2005
), this_year AS (
    SELECT actorid, actor, year, film, votes, rating, filmid
    FROM actor_films
    WHERE year = 2006
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
```
![image](https://github.com/user-attachments/assets/502b4b49-021b-44e9-a28c-da40a7cd2fae)




### 3. Slowly Changing Dimensions (SCD)

The actors_history_scd table tracks changes to quality_class and is_active status over time using start and end year for each record.

```
CREATE TABLE actors_history_scd (
    actor_id TEXT NOT NULL,
    is_active BOOLEAN NOT NULL,
    quality_class quality_class NOT NULL,
    start_year INTEGER NOT NULL,
    end_year INTEGER NOT NULL,
    current_year INTEGER NOT NULL,
    PRIMARY KEY (actor_id, start_year)
);
```

### 4.  Efficient Data Backfill
The backfill query populates the entire actors_history_scd table in a single execution.

```
WITH previous_data AS (
    SELECT
        actorid,
        current_year,
        quality_class,
        is_active,
        LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
        LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
    FROM actors
    WHERE current_year <= 2005
), with_indicators AS (
    SELECT *,
        CASE
            WHEN previous_quality_class <> quality_class THEN 1
            WHEN previous_is_active <> is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM previous_data
), with_streaks AS (
    SELECT *,
        SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
    FROM with_indicators
)
INSERT INTO actors_history_scd
SELECT
    actorid,
    is_active,
    quality_class,
    MIN(current_year) AS start_year,
    MAX(current_year) AS end_year,
    2005 AS current_season
FROM with_streaks
GROUP BY actorid, streak_identifier, is_active, quality_class
ORDER BY actorid;
```

![image](https://github.com/user-attachments/assets/4f07a913-dbe6-4554-bd92-d42fc34d8b8c)





### 5. Incremental Updates
The incremental query merges new data with the previous year’s SCD data, ensuring historical records remain intact.

```
CREATE TYPE actor_history_scd AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_year INTEGER,
    end_year INTEGER
);
```

```
CREATE TYPE actor_history_scd AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER
);

WITH last_year_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE current_year = 2005
    AND end_year = 2005
), historical_scd AS (
    SELECT actorid, quality_class, is_active, start_year, end_year
    FROM actors_history_scd
    WHERE current_year = 2005
    AND end_year < 2005
), this_year_scd AS (
    SELECT *
    FROM actors
    WHERE current_year = 2006
), unchanged_records AS (
    SELECT
        ts.actorid,
        ts.quality_class,
        ts.is_active,
        ls.start_year,
        ts.current_year AS end_date
    FROM this_year_scd ts
    JOIN last_year_scd ls
        ON ts.actorid = ls.actorid
    WHERE ts.quality_class = ls.quality_class
      AND ts.is_active = ls.is_active
), changed_records AS (
    SELECT
        ts.actorid,
        UNNEST(ARRAY [
            ROW(ls.quality_class, ls.is_active, ls.start_year, ls.end_year)::actor_history_scd,
            ROW(ts.quality_class, ts.is_active, ts.current_year, ts.current_year)::actor_history_scd
        ]) AS records
    FROM this_year_scd ts
    JOIN last_year_scd ls
        ON ts.actorid = ls.actorid
    WHERE ts.quality_class <> ls.quality_class
       OR ts.is_active <> ls.is_active
), unnested_changed_records AS (
    SELECT
        actorid,
        (records::actor_history_scd).quality_class,
        (records::actor_history_scd).is_active,
        (records::actor_history_scd).start_year,
        (records::actor_history_scd).end_year
    FROM changed_records
), new_records AS (
    SELECT
        ts.actorid,
        ts.quality_class,
        ts.is_active,
        ts.current_year AS start_date,
        ts.current_year AS end_date
    FROM this_year_scd ts
    LEFT JOIN last_year_scd ls
        ON ts.actorid = ls.actorid
    WHERE ls.actorid IS NULL
)
SELECT *
FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records
ORDER BY end_year DESC;

```
**New Records**
![image](https://github.com/user-attachments/assets/953888ec-ffa1-4e92-875d-b17e57029770)

**Historical Records**
![image](https://github.com/user-attachments/assets/40865951-95f2-42b8-be22-0a8084119224)

**Unchanged Records**
![image](https://github.com/user-attachments/assets/7be76731-f5e0-4079-91f7-2fafe7ce6ffd)

**Changed Record** 
![image](https://github.com/user-attachments/assets/69e309eb-d03f-4ace-a072-316df438589b)







