SELECT
    CONCAT(a.site_id,
            '-',
            a.personnel_id,
            '-',
            a.delivery_date) AS personnel_date_id,
    a.delivery_date,
    a.site_id,
    a.personnel_id,
    a.team_cnt,
    a.vip_team_cnt,
    a.delivery_cnt,
    a.vip_delivery_cnt,
    a.abnormal_sign_cnt,
    a.delivery_start_time,
    a.delivery_finish_time,
    a.vip_delivery_start_time,
    a.vip_delivery_finish_time,
    b.timeout_at,
    b.vip_timeout_at,
    b.timeout_cnt,
    b.vip_timeout_cnt,
    c.delivery_ttime
FROM
    (SELECT
        DATE(m_updated_at) AS delivery_date,
            site_id,
            personnel_id,
            COUNT(*) AS team_cnt,
            SUM(IF(is_vip = 1, 1, 0)) AS vip_team_cnt,
            SUM(load_num) AS delivery_cnt,
            SUM(IF(is_vip = 1, load_num, 0)) AS vip_delivery_cnt,
            SUM(IF(sign_type = 0, 0, 1)) AS abnormal_sign_cnt,
            MIN(m_created_at) AS delivery_start_time,
            MAX(m_updated_at) AS delivery_finish_time,
            MIN(IF(is_vip = 1, m_created_at, NULL)) AS vip_delivery_start_time,
            MAX(IF(is_vip = 1, m_created_at, NULL)) AS vip_delivery_finish_time
    FROM
        warehouse.deliverys
    WHERE
        m_created_at >= %s
            AND m_created_at < %s
    GROUP BY personnel_id , site_id , DATE(m_updated_at)) a
        LEFT JOIN
    (SELECT
        aa.site_id,
            aa.personnel_id,
            DATE(aa.m_updated_at) AS delivery_date,
            bb.timeout_at AS timeout_at,
            bb.vip_timeout_at AS vip_timeout_at,
            SUM(IF(aa.is_vip = 0, 1, 0)) AS timeout_cnt,
            SUM(IF(aa.is_vip = 1, 1, 0)) AS vip_timeout_cnt
    FROM
        warehouse.deliverys aa
    LEFT JOIN statistics.site_delivery_timeout bb ON aa.site_id = bb.site_id
    WHERE
        aa.m_created_at >= %s
            AND aa.m_created_at < %s
            AND bb.id IS NOT NULL
            AND ((aa.is_vip = 0
            AND TIME(aa.m_updated_at) >= bb.timeout_at)
            OR (aa.is_vip = 1
            AND TIME(aa.m_updated_at) >= bb.vip_timeout_at))
    GROUP BY personnel_id , site_id , DATE(m_updated_at)) b ON a.personnel_id = b.personnel_id
        AND a.site_id = b.site_id
        AND a.delivery_date = b.delivery_date
        LEFT JOIN
    (SELECT
        site_id,
            personnel_id,
            delivery_date,
            SUM(TIMESTAMPDIFF(SECOND, TIMESTAMP(delivery_start_time), TIMESTAMP(delivery_end_time))) AS delivery_ttime
    FROM
        (SELECT
        site_id,
            personnel_id,
            DATE(m_updated_at) AS delivery_date,
            MAX(m_created_at) AS delivery_start_time,
            MAX(m_updated_at) AS delivery_end_time
    FROM
        warehouse.deliverys
    WHERE
        m_created_at >= %s
            AND m_created_at < %s
    GROUP BY site_id , personnel_id , DATE(m_updated_at)) cc
    GROUP BY site_id , personnel_id , delivery_date) c ON a.personnel_id = c.personnel_id
        AND a.site_id = c.site_id
        AND a.delivery_date = c.delivery_date