BEGIN;

CREATE SCHEMA IF NOT EXISTS orders;
CREATE SCHEMA IF NOT EXISTS fulfilment;

COMMENT ON SCHEMA orders IS 'Schema reserved for the orders service.';
COMMENT ON SCHEMA fulfilment IS 'Schema reserved for the fulfilment service.';

COMMIT;
