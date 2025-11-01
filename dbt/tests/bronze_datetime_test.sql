select *
from {{ source('bronze', 'webtoon_user_events_raw') }}
where unix_timestamp(CAST(datetime AS STRING), 'yyyy-MM-dd')
    < unix_timestamp('2025-09-01', 'yyyy-MM-dd')