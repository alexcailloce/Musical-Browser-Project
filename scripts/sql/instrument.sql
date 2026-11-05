INSERT INTO instrument (instrument_id, name, type)
VALUES %s
ON CONFLICT (instrument_id) DO UPDATE
SET name = EXCLUDED.name, type = EXCLUDED.type;