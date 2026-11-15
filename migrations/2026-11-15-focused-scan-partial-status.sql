ALTER TABLE public.spotify_focused_scan_jobs
    DROP CONSTRAINT IF EXISTS spotify_focused_scan_jobs_status_check,
    ADD CONSTRAINT spotify_focused_scan_jobs_status_check
        CHECK (status = ANY (ARRAY['pending','running','complete','partial','failed']));
