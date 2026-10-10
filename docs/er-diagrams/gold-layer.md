# Gold layer -- ER diagram

Source: `pipeline_src/transformations/04_gold.sql`.

Three fully denormalized, standalone summary tables -- no foreign keys between
them (matching the Databricks original's gold layer shape), each aggregated
straight from the curated layer's `fact_race_results` joined to its relevant
dimension(s). `circuit_summary` carries `track_type` through from
`dim_circuits`, proving the second bronze source's data reaches all the way to
gold.

```mermaid
erDiagram
    DRIVER_PERFORMANCE_SUMMARY {
        int season_year
        string driver_Id
        string driver_name
        string team_Id
        string team_name
        long races_entered
        long wins
        long podiums
        float total_points
        double avg_finish_position
        string best_finish_position
        timestamp refreshed_at
    }

    TEAM_PERFORMANCE_SUMMARY {
        string team_Id
        string team_name
        long races_entered
        long wins
        long podiums
        float total_points
        long drivers_used
        timestamp refreshed_at
    }

    CIRCUIT_SUMMARY {
        string circuit_Id
        string circuit_name
        string city
        string country
        string track_type
        long total_races_held
        date first_race_date
        date last_race_date
        string lap_record_time
        string fastest_lap_driver_id
        timestamp refreshed_at
    }
```
