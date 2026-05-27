alter table strava_activities
    add column exercise_load_score decimal(8,2) default null after average_pace_second_per_km;