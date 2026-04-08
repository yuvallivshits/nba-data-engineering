UPDATE `{{ params.project }}.{{ params.dataset }}.player_dim` AS target
SET
    target.team_id = source.team_id
FROM `{{ params.project }}.{{ params.dataset }}.player_team_scd` AS source
WHERE target.player_id = source.player_id and source.valid_from = DATE '{{ var.value.player_team_scd_simulated_date }}';