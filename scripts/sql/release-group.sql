INSERT INTO release_groups (release_group_id, title, primary_text, artist_id)
VALUES %s
ON CONFLICT (release_group_id) DO UPDATE
SET title = EXCLUDED.title, primary_text = EXCLUDED.primary_text, artist_id = EXCLUDED.artist_id;