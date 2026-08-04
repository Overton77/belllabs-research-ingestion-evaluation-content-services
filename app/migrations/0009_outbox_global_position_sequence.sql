CREATE SEQUENCE IF NOT EXISTS belllabs_control.outbox_position_seq;

SELECT setval(
    'belllabs_control.outbox_position_seq',
    COALESCE((SELECT MAX(position) FROM belllabs_control.outbox), 0) + 1,
    false
);

ALTER SEQUENCE belllabs_control.outbox_position_seq
    OWNED BY belllabs_control.outbox.position;

ALTER TABLE belllabs_control.outbox
    ALTER COLUMN position
    SET DEFAULT nextval('belllabs_control.outbox_position_seq');

GRANT USAGE, SELECT
    ON SEQUENCE belllabs_control.outbox_position_seq
    TO belllabs_control_runtime;
