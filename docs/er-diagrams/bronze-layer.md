# Bronze layer -- ER diagram

Source: `pipeline_src/transformations/00_bronze_circuit_track_type.py`,
`01_bronze_raw.py`, `02_bronze_prepared.py`.

`circuit_track_type` is deliberately drawn with no relationship to `raw_race` --
it's the second, independent, non-API source described in the main README's
"Multiple data sources, zero orchestration" section. It only becomes related to
the rest of the graph in the curated layer, via its join into `dim_circuits`.

```mermaid
erDiagram
    RAW_RACE {
        string api
        long limit
        long offset
        struct races
        long season
        long total
        string url
        string source_url
        string season_requested
        string round_requested
        timestamp load_date_time
    }

    CIRCUIT_TRACK_TYPE {
        string circuit_Id PK
        string track_type
        timestamp load_date_time
    }

    RACE_RESULTS_UNNESTED {
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
        timestamp load_date_time
    }

    RACE_DRIVERS {
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

    RACE_TEAMS {
        string team_id PK
        string name
        string nationality
        int first_Appearance
        int constructors_championships
        int drivers_championships
        date race_Date
        timestamp load_date_time
    }

    RACE_CIRCUITS {
        string circuit_Id PK
        string name
        string city
        string country
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

    RAW_RACE ||--o{ RACE_RESULTS_UNNESTED : "explodes results array into"
    RAW_RACE ||--o{ RACE_DRIVERS : "extracts driver entity from"
    RAW_RACE ||--o{ RACE_TEAMS : "extracts team entity from"
    RAW_RACE ||--o{ RACE_CIRCUITS : "extracts circuit entity from"
    RACE_DRIVERS ||--o{ RACE_RESULTS_UNNESTED : driver_Id
    RACE_TEAMS ||--o{ RACE_RESULTS_UNNESTED : team_Id
    RACE_CIRCUITS ||--o{ RACE_RESULTS_UNNESTED : circuit_Id
```
