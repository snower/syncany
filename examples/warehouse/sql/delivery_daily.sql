SELECT
    CONCAT(a.site_id, '-', a.delivery_date) AS site_date_id,
    a.site_id,
    a.delivery_date,
    a.team_cnt,
    a.vip_team_cnt,
    a.abnormal_sign_cnt,
    a.driver_cnt,
    b.sorter_cnt,
    a.delivery_cnt,
    a.vip_delivery_cnt,
    a.delivery_start_time,
    a.delivery_finish_time,
    a.vip_delivery_start_time,
    a.vip_delivery_finish_time,
    b.sort_start_time,
    b.sort_end_time,
    c.timeout_at,
    c.vip_timeout_at,
    c.timeout_cnt,
    c.vip_timeout_cnt,
    d.delivery_ttime
FROM
    (SELECT
        site_id,
            DATE(m_updated_at) AS delivery_date,
            COUNT(*) AS team_cnt,
            SUM(IF(is_vip = 1, 1, 0)) AS vip_team_cnt,
            SUM(IF(sign_type = 0, 0, 1)) AS abnormal_sign_cnt,
            COUNT(DISTINCT personnel_id) AS driver_cnt,
            SUM(load_num) AS delivery_cnt,
            SUM(IF(is_vip = 1, load_num, 0)) AS vip_delivery_cnt,
            MIN(m_created_at) AS delivery_start_time,
            MAX(m_updated_at) AS delivery_finish_time,
            MIN(IF(is_vip = 1, m_created_at, NULL)) AS vip_delivery_start_time,
            MAX(IF(is_vip = 1, m_created_at, NULL)) AS vip_delivery_finish_time
    FROM
        warehouse.deliverys
    WHERE
        m_created_at >= %s
            AND m_created_at < %s
    GROUP BY site_id , DATE(m_updated_at)) a
        LEFT JOIN
    (SELECT
        site_id,
            `date`,
            COUNT(*) AS sorter_cnt,
            MIN(start_time) AS sort_start_time,
            MAX(end_time) AS sort_end_time
    FROM
        warehouse.sign
    WHERE
        `date` >= %s
            AND `date` < %s
    GROUP BY site_id , `date`) b ON a.site_id = b.site_id
        AND a.delivery_date = b.`date`
        LEFT JOIN
    (SELECT
        aa.site_id,
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
    GROUP BY site_id , DATE(m_updated_at)) c ON a.site_id = c.site_id
        AND a.delivery_date = c.delivery_date
        LEFT JOIN
    (SELECT
        site_id,
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
    GROUP BY site_id , delivery_date) d ON a.site_id = d.site_id
        AND a.delivery_date = d.delivery_date