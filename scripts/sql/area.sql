INSERT INTO area (area_id, name, type, iso_code)
VALUES %s
ON CONFLICT (area_id) DO UPDATE
SET name = EXCLUDED.name, type = EXCLUDED.type, iso_code = EXCLUDED.iso_code; 