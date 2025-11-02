SELECT *
FROM {{ source('silver', 'webtoon_user_session_events' )}}
WHERE
    (session_state = 'COMPLETE'
    AND
    (max_scroll_ratio < 0.95 OR max_scroll_ratio IS NULL))
    OR
    (session_state = 'EXIT'
    AND
    (max_scroll_ratio >= 0.95))
