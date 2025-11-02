SELECT *
FROM {{ source('silver', 'webtoon_user_session_events') }}
WHERE
    session_state IN ('EXIT', 'TIMEOUT_EXIT')
    AND
    is_exit != 1
