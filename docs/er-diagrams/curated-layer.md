# Curated layer -- ER diagram

Source: `pipeline_src/transformations/03_curated.sql`.

All four tables are SCD Type 1 (latest-state-only, via `ROW_NUMBER()` -- see the
file's header comment) -- there is no `__START_AT`/`__END_AT` history here,
unlike the Databricks original this project was ported from: AWS Glue 6.0 SDP
has no `create_auto_cdc_flow`/APPLY CHANGES primitive.

`dim_circuits` carries `track_type`, joined in from the second, independent
bronze source `circuit_track_type` (see the bronze-layer diagram) -- that join
is the entire mechanism proving SDP auto-infers a second DAG branch with zero
orchestration config.

```mermaid
erDiagram
    DIM_DRIVERS {
        string driver_Id PK
        string name
        string surname
        string shortName
        string nationality
        date birthday
        int number
        date race_Date
        timestamp load_date_time
    }

    DIM_TEAMS {
        string team_id PK
        string name
        string nationality
        int first_Appearance
        int constructors_championships
        int drivers_championships
        date race_Date
        timestamp load_date_time
    }

    DIM_CIRCUITS {
        string circuit_Id PK
        string name
        string city
        string country
        string track_type
        string length
        int corners
        int first_Participation_Year
        string lap_Record
        string fastest_Lap_Driver_Id
        string fastest_Lap_Team_Id
        int fastest_Lap_Year
        date race_Date
        timestamp load_date_time
    }

    FACT_RACE_RESULTS {
        int season_year
        string race_Id PK
        string driver_Id "PK,FK"
        string team_Id FK
        string circuit_Id FK
        string race_Name
        int race_Round
        date race_Date
        string race_Time
        string driver_Race_Grid_Position
        string driver_Race_Final_Position
        float driver_Race_Points
        string driver_Race_Fast_Lap
        string driver_Race_Gap_With_Win_Time
        timestamp refreshed_at
    }

    DIM_DRIVERS ||--o{ FACT_RACE_RESULTS : driver_Id
    DIM_TEAMS ||--o{ FACT_RACE_RESULTS : team_Id
    DIM_CIRCUITS ||--o{ FACT_RACE_RESULTS : circuit_Id
```
