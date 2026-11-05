INSERT INTO label (label_id, name, label_code, area_id)
VALUES %S
ON CONFLICT (label_id) DO UPDATE
SET name = EXCLUDED.name, label_code = EXCLUDED.label_code, area_id = EXCLUDED.area_id;