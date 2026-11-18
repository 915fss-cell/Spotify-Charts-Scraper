INSERT INTO public.spotify_chart_tracking_configs
  (entity_uri, chart_type, country_code, enabled)
SELECT s.entity_uri, supported.chart_type, markets.country_code, true
FROM public.spotify_chart_selections s
CROSS JOIN (VALUES
  ('daily_top_songs', 'track'),
  ('weekly_top_songs', 'track'),
  ('daily_top_artists', 'artist'),
  ('weekly_top_artists', 'artist'),
  ('weekly_top_albums', 'artist')
) AS supported(chart_type, entity_type)
CROSS JOIN unnest(ARRAY[
  'GLOBAL', 'IN', 'AR', 'AU', 'AT', 'BY', 'BE', 'BO', 'BR', 'BG', 'CA', 'CL',
  'CO', 'CR', 'CZ', 'DK', 'DO', 'EC', 'EG', 'SV', 'EE', 'FI', 'FR', 'DE', 'GR',
  'GT', 'HN', 'HK', 'HU', 'IS', 'ID', 'IE', 'IL', 'JP', 'KZ', 'LV', 'LT', 'LU',
  'MY', 'MX', 'MA', 'NL', 'NZ', 'NI', 'NG', 'NO', 'PK', 'PA', 'PY', 'PE', 'PH',
  'PL', 'PT', 'RO', 'SA', 'SG', 'SK', 'ZA', 'KR', 'ES', 'SE', 'CH', 'TW', 'TH',
  'TR', 'AE', 'UA', 'GB', 'UY', 'US', 'VE', 'VN'
]::text[]) AS markets(country_code)
WHERE s.is_archived = false
  AND s.entity_type = supported.entity_type
ON CONFLICT (entity_uri, chart_type, country_code) DO UPDATE SET
  enabled = true,
  updated_at = now();
