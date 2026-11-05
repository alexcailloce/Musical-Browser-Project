INSERT INTO artist (artist_id, name, type, area_id)
VALUES %s
ON CONFLICT (artist_id) DO UPDATE
SET name = EXCLUDED.name, type = EXCLUDED.type, area_id = EXCLUDED.area_id;