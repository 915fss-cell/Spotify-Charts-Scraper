ALTER TABLE public.spotify_chart_tracking_configs ADD COLUMN IF NOT EXISTS backfill_end_date date;
