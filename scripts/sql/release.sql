INSERT INTO releases (release_id, title, release_group_id, release_date, barcode)
VALUES %s
ON CONFLICT (release_id) DO UPDATE
SET title = EXCLUDED.title, release_group_id = EXCLUDED.release_group_id, release_date = EXCLUDED.release_date, barcode = EXCLUDED.barcode;