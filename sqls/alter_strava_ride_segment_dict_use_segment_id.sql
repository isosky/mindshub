alter table strava_ride_segment_dict
    add column segment_id bigint default null after id;

update strava_ride_segment_dict d
left join (
    select segment_name, max(segment_id) as segment_id
    from strava_ride_segments
    where segment_id is not null
    group by segment_name
) s on d.segment_name = s.segment_name
set d.segment_id = s.segment_id
where d.segment_id is null;

update strava_ride_segment_dict
set segment_id = id
where segment_id is null;

alter table strava_ride_segment_dict
    modify column segment_id bigint not null;

alter table strava_ride_segment_dict
    modify column segment_name varchar(255) default null;

alter table strava_ride_segment_dict
    drop index uk_strava_ride_segment_dict_segment_name,
    add unique key uk_strava_ride_segment_dict_segment_id (segment_id);
