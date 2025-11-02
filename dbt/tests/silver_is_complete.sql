SELECT *
FROM {{ source('silver', 'webtoon_user_session_events' )}}
WHERE
    session_state = 'COMPLETE'
    AND
    is_complete != 1
