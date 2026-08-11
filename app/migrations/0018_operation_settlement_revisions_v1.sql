ALTER TABLE belllabs_control.operation_settlements
    DROP CONSTRAINT IF EXISTS operation_settlements_pkey;

ALTER TABLE belllabs_control.operation_settlements
    ADD CONSTRAINT operation_settlements_pkey
    PRIMARY KEY (request_scope, settlement_id, settlement_revision);

CREATE INDEX IF NOT EXISTS operation_settlements_latest_revision_idx
    ON belllabs_control.operation_settlements (
        request_scope, effect_claim_id, settlement_revision DESC
    );

WITH pending_candidates AS (
    SELECT
        claims.belllabs_run_id AS run_id,
        settlements.settlement_id,
        settlements.usage_payload,
        settlements.pending_external_usage_payload,
        COALESCE(
            settlements.settlement_payload->'released_usage',
            '{}'::jsonb
        ) AS released_usage,
        effects.state #> ARRAY['claims', settlements.effect_claim_id] AS effect_claim,
        count(*) OVER (
            PARTITION BY claims.belllabs_run_id
        ) AS candidate_count
    FROM belllabs_control.operation_settlements AS settlements
    JOIN belllabs_control.operation_effect_claims AS claims
      ON claims.request_scope = settlements.request_scope
     AND claims.effect_claim_id = settlements.effect_claim_id
    JOIN belllabs_control.effect_ledgers AS effects
      ON effects.run_id = claims.belllabs_run_id
    WHERE settlements.status = 'reconciliation_required'
      AND settlements.pending_external_usage_payload <> '{}'::jsonb
),
eligible AS (
    SELECT *
    FROM pending_candidates
    WHERE candidate_count = 1
      AND effect_claim IS NOT NULL
      AND effect_claim->>'settlement' IS NULL
      AND NULLIF(effect_claim->>'reservation_id', '') IS NOT NULL
      AND NULLIF(effect_claim->>'operation_ref', '') IS NOT NULL
)
UPDATE belllabs_control.budget_accounts AS budgets
SET state = jsonb_set(
    jsonb_set(
        jsonb_set(
            budgets.state,
            '{usage_ids}',
            COALESCE(budgets.state->'usage_ids', '[]'::jsonb)
                || to_jsonb(eligible.settlement_id),
            true
        ),
        '{usage_records}',
        jsonb_build_object(
            eligible.settlement_id,
            jsonb_build_object(
                'usage_id', eligible.settlement_id,
                'reservation_id', eligible.effect_claim->>'reservation_id',
                'authority_ref', eligible.effect_claim->>'operation_ref',
                'actual_amounts', eligible.usage_payload,
                'release_amounts', eligible.released_usage,
                'pending_external_amounts',
                    eligible.pending_external_usage_payload
            )
        ),
        true
    ),
    '{outstanding_usage_ids}',
    jsonb_build_array(eligible.settlement_id),
    true
)
FROM eligible
WHERE budgets.run_id = eligible.run_id
  AND COALESCE(budgets.state->'usage_records', '{}'::jsonb) = '{}'::jsonb
  AND COALESCE(budgets.state->'outstanding_usage_ids', '[]'::jsonb) = '[]'::jsonb
  AND COALESCE(budgets.state->'pending_settlement', '{}'::jsonb)
      = eligible.pending_external_usage_payload;
