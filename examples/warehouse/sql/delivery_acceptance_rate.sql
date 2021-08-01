SELECT
    CONCAT(site_id, '-', is_vip, '-', delivery_date) AS acceptance_rate_id,
    site_id,
    is_vip,
    delivery_date,
    'hour0' as ctime,
    0 as time_cnt,
    SUM(hour6) AS hour6,
    SUM(hour7) AS hour7,
    SUM(hour8) AS hour8,
    SUM(hour9) AS hour9,
    SUM(hour10) AS hour10,
    SUM(hour11) AS hour11,
    SUM(hour12) AS hour12,
    SUM(hour13) AS hour13,
    SUM(hour14) AS hour14,
    SUM(hour15) AS hour15,
    SUM(hour16) AS hour16,
    SUM(hour17) AS hour17,
    SUM(hour18) AS hour18,
    SUM(cnt) as cnt
FROM
    (SELECT
        site_id,
            personnel_id,
            is_vip,
            DATE(m_updated_at) AS delivery_date,
            SUM(IF(TIME(m_updated_at) < '06:00:00', 1, 0)) AS hour6,
            SUM(IF(TIME(m_updated_at) < '07:00:00', 1, 0)) AS hour7,
            SUM(IF(TIME(m_updated_at) < '08:00:00', 1, 0)) AS hour8,
            SUM(IF(TIME(m_updated_at) < '09:00:00', 1, 0)) AS hour9,
            SUM(IF(TIME(m_updated_at) < '10:00:00', 1, 0)) AS hour10,
            SUM(IF(TIME(m_updated_at) < '11:00:00', 1, 0)) AS hour11,
            SUM(IF(TIME(m_updated_at) < '12:00:00', 1, 0)) AS hour12,
            SUM(IF(TIME(m_updated_at) < '13:00:00', 1, 0)) AS hour13,
            SUM(IF(TIME(m_updated_at) < '14:00:00', 1, 0)) AS hour14,
            SUM(IF(TIME(m_updated_at) < '15:00:00', 1, 0)) AS hour15,
            SUM(IF(TIME(m_updated_at) < '16:00:00', 1, 0)) AS hour16,
            SUM(IF(TIME(m_updated_at) < '17:00:00', 1, 0)) AS hour17,
            SUM(IF(TIME(m_updated_at) < '18:00:00', 1, 0)) AS hour18,
            COUNT(*) AS cnt
    FROM
        warehouse.deliverys
    WHERE
        m_created_at >= %s
            AND m_created_at < %s
    GROUP BY site_id , personnel_id , is_vip , DATE(m_updated_at)) a
GROUP BY site_id , is_vip , delivery_date