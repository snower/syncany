SELECT
    CONCAT(terminal_code,
            ':',
            DATE(created_at),
            ':',
            charge_type) AS terminal_code_date_type,
    terminal_code,
    DATE(created_at) AS order_date,
    charge_type,
    COUNT(out_order_no) AS order_cnt,
    SUM(charge) AS charge,
    SUM(IF(charge > 0, 1, 0)) AS charge_cnt,
    SUM(discounted_charge) AS discounted_charge,
    SUM(IF(discounted_charge > 0, 1, 0)) AS discounted_charge_cnt
FROM
    (SELECT
        charge_type,
            terminal_code,
            out_order_no,
            created_at,
            SUM(charge) AS charge,
            SUM(discounted_charge) AS discounted_charge
    FROM
        `ebox_charge`
    WHERE
        created_at >= %s
            AND created_at < %s
    GROUP BY out_order_no , charge_type
    HAVING charge + discounted_charge > 0) a
GROUP BY terminal_code , DATE(created_at) , charge_type