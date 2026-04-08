-- Step 1: Close current rows for players traded today
UPDATE `{{ params.project }}.{{ params.dataset }}.player_team_scd` AS target
SET
    valid_to = DATE_SUB(DATE '{{ var.value.player_team_scd_simulated_date }}', INTERVAL 1 DAY),
    is_current = FALSE
WHERE
    target.is_current = TRUE
    AND target.player_id IN (
        SELECT player_id
        FROM `{{ params.project }}.{{ params.dataset }}.player_team_scd_staging`
        WHERE trade_date = DATE '{{ var.value.player_team_scd_simulated_date }}'
    );

-- Step 2: Insert new players not yet in the table (uses pre-trade team for traded players)
INSERT INTO `{{ params.project }}.{{ params.dataset }}.player_team_scd`
SELECT
    p.player_id,
    COALESCE(s.from_team_id, p.team_id) AS team_id,
    DATE '{{ var.value.player_team_scd_simulated_date }}' AS valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS inserted_at
FROM `{{ params.project }}.{{ params.dataset }}.player_dim` p
LEFT JOIN `{{ params.project }}.{{ params.dataset }}.player_team_scd_staging` s
    ON p.player_id = s.player_id
WHERE p.player_id NOT IN (
    SELECT player_id
    FROM `{{ params.project }}.{{ params.dataset }}.player_team_scd`
);

-- Step 3: Insert new rows for players traded today (their new team)
INSERT INTO `{{ params.project }}.{{ params.dataset }}.player_team_scd`
SELECT
    player_id,
    team_id,
    DATE '{{ var.value.player_team_scd_simulated_date }}' AS valid_from,
    DATE '9999-12-31' AS valid_to,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS inserted_at
FROM `{{ params.project }}.{{ params.dataset }}.player_team_scd_staging`
WHERE trade_date = DATE '{{ var.value.player_team_scd_simulated_date }}';
