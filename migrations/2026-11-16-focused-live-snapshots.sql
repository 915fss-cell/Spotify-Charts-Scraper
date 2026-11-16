CREATE TABLE IF NOT EXISTS public.spotify_focused_chart_snapshots (
    cadence text NOT NULL CHECK (cadence = ANY (ARRAY['daily','weekly'])),
    entity_type text NOT NULL CHECK (entity_type = ANY (ARRAY['track','artist','album'])),
    market_scope text NOT NULL CHECK (market_scope = 'all'),
    snapshot_date date NOT NULL DEFAULT CURRENT_DATE,
    payload jsonb NOT NULL,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cadence, entity_type, market_scope, snapshot_date)
);
