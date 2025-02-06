# **Graph-Based Sports Analytics Using SQL**

## **Project Overview**
This project implements a graph-based data model for sports analytics using SQL. By structuring data as vertices (players, teams, and games) and edges (relationships between them), we enable complex queries for in-depth analysis of player performance, team dynamics, and game statistics.

**What this project will achieve**
 

✅ **Track Player & Team Performance** – Analyze how players and teams perform over time.  
✅ **Keep a History of Relationships** – Track player interactions, team changes, and game participation.  
✅ **Easy Yearly Updates** – Add new game data without affecting past records.  
✅ **Faster Data Searches** – Quickly find insights on player stats, team dynamics, and game history.  
✅ **Ready for Reports & Analysis** – Well-structured graph data for network analysis, reports, and deeper insights.  

 
## Data Overview 
#### 1. The **games** dataset contains the following fields:

1. **game_id**: Unique identifier for each game.
2. **home_team_id**: Identifier for the home team.  
3. **visitor_team_id**: Identifier for the away team.   
4. **pts_home**: Points scored by the home team.  
5. **pts_away**: Points scored by the away team.  
6. **home_team_wins**: Indicator of whether the home team won the game.
7. **team_id_home**, **team_id_away**: Identifiers for teams involved in the game.

**Other columns** contain additional game information.
![image](https://github.com/user-attachments/assets/f7f398dd-6954-4388-9dab-dc9b4f823c13)

#### 2. The  **game_details** dataset contains the following fields:

1.**game_id**: Unique identifier for each game.  
2. **team_id**: Identifier for the team.  
3. **team_abbreviation**: Abbreviation of the team name.  
4. **team_city**: City of the team.  
5. **player_id**: Unique identifier for the player.  
6. **player_name**: Name of the player.  
7. **start_position**: The player's starting position.  
8. **fgm, fga, fg_pct**: Field goals made, attempted, and percentage.  
9. **fg3m, fg3a, fg3_pct**: Three-point field goals made, attempted, and percentage.  
10. **ftm, fta, ft_pct**: Free throws made, attempted, and percentage.  
11. **reb, ast, stl, blk, pts**: Rebounds, assists, steals, blocks, and points scored.  

Other columns contain additional player statistics like turnovers, fouls, and minutes played.  



![image](https://github.com/user-attachments/assets/b6eb1363-4d59-482f-b4a2-09ab58e82518)


---

## **Data Model Design**

### **1. Vertex Types (Entities)**
We define three main entity types to represent the key components of a game:
- **Player**
- **Team**
- **Game**

```sql
CREATE TYPE vertex_type AS ENUM ('player', 'team', 'game');
```

#### **Vertex Table**
Each entity is stored in the `vertices` table, containing an identifier, type, and additional properties stored as JSON.

```sql
CREATE TABLE vertices (
    identifier TEXT,
    type vertex_type,
    properties JSON,
    PRIMARY KEY (identifier, type)
);
```

### **2. Edge Types (Relationships)**
We define relationships between entities using an `edges` table.

```sql
CREATE TYPE edge_type AS ENUM (
    'plays_against',  -- player vs player in a game
    'shares_team',    -- players in the same team
    'plays_in',       -- player participates in a game
    'plays_on'        -- player belongs to a team
);
```

#### **Edge Table**
The `edges` table captures relationships between vertices.

```sql
CREATE TABLE edges (
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (subject_identifier, subject_type, object_identifier, object_type, edge_type)
);
```

---

## **Data Insertion**

### **1. Populating Game Data**
Insert games as vertices with their properties (points, winning team, etc.).

```sql
INSERT INTO vertices
SELECT game_id AS identifier,
       'game'::vertex_type AS type,
       json_build_object(
           'pts_home', pts_home,
           'pts_away', pts_away,
           'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE 0 END
       )
FROM games;
```

### **2. Populating Player Data**
Each player is represented as a vertex with aggregated stats.

```sql
WITH players_agg AS (
    SELECT player_id AS identifier,
           MAX(player_name) AS player_name,
           COUNT(1) AS number_of_games,
           SUM(pts) AS total_points,
           ARRAY_AGG(DISTINCT team_id) AS teams
    FROM game_details
    GROUP BY player_id
)
INSERT INTO vertices
SELECT identifier, 'player'::vertex_type,
       json_build_object(
           'player_name', player_name,
           'number_of_games', number_of_games,
           'total_points', total_points,
           'teams', teams
       )
FROM players_agg;
```

### **3. Populating Team Data**
Teams are stored as vertices with attributes such as city and arena.

```sql
WITH team_deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY team_id) AS row_num
    FROM teams
)
INSERT INTO vertices
SELECT team_id AS identifier,
       'team'::vertex_type AS type,
       json_build_object(
           'abbreviation', abbreviation,
           'nickname', nickname,
           'city', city,
           'arena', arena,
           'year_founded', yearfounded
       )
FROM team_deduped
WHERE row_num = 1;
```

---

## **Edge Creation (Relationships)**

### **1. Player-Game Relationships**
Link players to the games they played in.

```sql
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
)
INSERT INTO edges
SELECT player_id AS subject_identifier,
       'player'::vertex_type,
       game_id AS object_identifier,
       'game'::vertex_type,
       'plays_in'::edge_type,
       json_build_object(
           'start_position', start_position,
           'pts', pts,
           'team_id', team_id,
           'team_abbreviation', team_abbreviation
       ) AS properties
FROM deduped
WHERE row_num = 1;
```

### **2. Player-Player Relationships (Same Team or Opponent)**
Establish relationships between players who either played against each other or were on the same team.

```sql
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),
filtered AS (
    SELECT * FROM deduped WHERE row_num = 1
),
aggregated AS (
    SELECT f1.player_id AS left_player_id,
           f2.player_id AS right_player_id,
           CASE WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
                ELSE 'plays_against'::edge_type END AS edge_type,
           COUNT(1) AS num_games,
           SUM(f1.pts) AS left_points,
           SUM(f2.pts) AS right_points
    FROM filtered f1
    JOIN filtered f2 ON f1.game_id = f2.game_id AND f1.player_id <> f2.player_id
    WHERE f1.player_id > f2.player_id
    GROUP BY f1.player_id, f2.player_id, edge_type
)
INSERT INTO edges
SELECT left_player_id AS subject_identifier,
       'player'::vertex_type AS subject_type,
       right_player_id AS object_identifier,
       'player'::vertex_type AS object_type,
       edge_type,
       json_build_object(
           'num_games', num_games,
           'subject_points', left_points,
           'object_points', right_points
       )
FROM aggregated;
```

---

## **Querying the Graph Model**

### **1. Retrieve All Players and Their Maximum Points**

```sql
SELECT v.properties->>'player_name' AS player_name,
       MAX(CAST(e.properties->>'pts' AS INTEGER)) AS max_points
FROM vertices v
JOIN edges e ON v.identifier = e.subject_identifier AND v.type = e.subject_type
GROUP BY 1;
```
![image](https://github.com/user-attachments/assets/ae05f034-3a87-49dd-8e37-ee52bf5c6489)


### **2. Calculate Player Average Points Per Game**

```sql
SELECT v.properties->>'player_name' AS player_name,
       CAST(v.properties->>'total_points' AS REAL) /
       CASE WHEN CAST(v.properties->>'number_of_games' AS REAL) = 0 THEN 1
            ELSE CAST(v.properties->>'number_of_games' AS REAL) END AS avg_points_per_game
FROM vertices v
JOIN edges e ON v.identifier = e.subject_identifier AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type;
```
![image](https://github.com/user-attachments/assets/f671e689-f918-4d8d-91e6-d125adcce023)


---


