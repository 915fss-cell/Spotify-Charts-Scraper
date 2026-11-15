(() => {
  const app = window.Chartroom;
  if (!app) return;

  const { byId, requestJson, escapeHtml, spotifyLink, getCachedChart } = app;
  const state = { cadence: "daily", entityType: "track", view: "release", marketScope: "global" };
  let activePage = "charts";
  let liveOverview = null;
  let liveKey = "";
  let liveLoadedAt = 0;
  let historyOverview = null;
  let historyChart = null;
  let historyTrendData = null;
  let liveRequest = 0;
  let isAutoRegionScanning = false;
  let historyRequest = 0;
  let historyOffset = 0;
  let historyEntitiesOffset = 0;
  let historyEntityDetailOffset = 0;
  let collectionRunsOffset = 0;
  let collectionJobsOffset = 0;
  let selectedHistoryEntityUri = "";
  let trackedEntityChart = null;
  const HISTORY_PAGE_SIZE = 50;
  const HISTORY_ENTITY_PAGE_SIZE = 20;
  const HISTORY_ENTITY_DETAIL_PAGE_SIZE = 120;
  const authChannel = "BroadcastChannel" in window ? new BroadcastChannel("spotify-charts-auth") : null;
  let trackedEntities = [];

  async function loadTrackedEntities() {
    trackedEntities = await requestJson("/api/selections");
    return trackedEntities;
  }

  function focusedSelectionName(entityType = state.entityType) {
    return `Tracked ${entityType === "track" ? "songs" : entityType === "artist" ? "artists" : "albums"}`;
  }

  function setStatus(id, message, isError = false) {
    const element = byId(id);
    if (!element) return;
    element.textContent = message;
    element.classList.toggle("error", isError);
  }

  function currentKey() {
    return `${state.cadence}:${state.entityType}:${state.marketScope}`;
  }

  function updateLiveActionLabel() {
    byId("live-refresh").textContent = state.marketScope === "global" ? "Load all regions" : "Refresh all regions";
  }

  function chartTypeName() {
    const names = { track: "songs", artist: "artists", album: "albums" };
    return `${state.cadence === "daily" ? "Daily" : "Weekly"} top ${names[state.entityType]}`;
  }

  function entityTypeName() {
    return { track: "song", artist: "artist", album: "album" }[state.entityType];
  }

  function updateFocusControls() {
    document.querySelectorAll("[data-focus-key]").forEach((button) => {
      button.setAttribute("aria-pressed", String(state[button.dataset.focusKey] === button.dataset.focusValue));
    });
    const isArtistChart = state.entityType === "artist";
    const streamsOption = byId("history-metric").querySelector('option[value="streams"]');
    streamsOption.disabled = isArtistChart;
    if (isArtistChart && byId("history-metric").value === "streams") byId("history-metric").value = "rank";
  }

  function coverMarkup(imageUrl, title) {
    if (imageUrl) return `<span class="focus-artwork"><img src="${escapeHtml(imageUrl)}" alt="" loading="lazy" /></span>`;
    return `<span class="focus-artwork" aria-hidden="true">${escapeHtml((title || "♫").slice(0, 1).toUpperCase())}</span>`;
  }

  function formatNumber(value) {
    if (value == null || value === "") return "—";
    const number = Number(String(value).replaceAll(",", ""));
    return Number.isFinite(number) ? number.toLocaleString() : String(value);
  }

  function movementMarkup(item) {
    const current = Number(item.chartRank);
    const previous = Number(item.previousRank);
    if (!Number.isFinite(current) || !Number.isFinite(previous) || current <= 0 || previous <= 0) {
      return '<span class="focus-movement same" aria-label="Rank movement unavailable">—</span>';
    }
    const change = previous - current;
    if (change > 0) return `<span class="focus-movement up" aria-label="Up ${change} positions">↑${change}</span>`;
    if (change < 0) return `<span class="focus-movement down" aria-label="Down ${Math.abs(change)} positions">↓${Math.abs(change)}</span>`;
    return '<span class="focus-movement same" aria-label="No rank change">—</span>';
  }

  function chartDetails(item) {
    const unit = state.cadence === "weekly" ? "w" : "d";
    const details = [];
    if (item.peakRank != null) details.push(`Peak: #${item.peakRank}`);
    if (item.consecutiveAppearancesOnChart != null) details.push(`Cons: ${item.consecutiveAppearancesOnChart}${unit}`);
    if (item.appearancesOnChart != null) details.push(`${state.cadence === "weekly" ? "WoC" : "Appearances"}: ${item.appearancesOnChart}${unit}`);
    if (!details.length && item.entryDate) details.push(`Since ${item.entryDate}`);
    return details.join(" · ") || "Chart details unavailable";
  }

  function trendButton(entityUri, entityName) {
    return `<button class="quiet-button focus-trend-button" type="button" data-history-uri="${escapeHtml(entityUri)}" aria-label="View ${escapeHtml(entityName)} history across regions">View trend</button>`;
  }

  function releaseRegionRow(item, showTrend = false) {
    return `<div class="focus-data-row"><div class="focus-row-rank">${escapeHtml(item.chartRank ?? "—")}${movementMarkup(item)}</div><div class="focus-row-copy"><strong class="focus-row-title">${escapeHtml(item.regionName)}</strong><span class="focus-row-subtitle">${escapeHtml(chartDetails(item))}</span></div><div class="focus-row-metric"><strong>${escapeHtml(formatNumber(item.metricValue ?? item.streamCount))}</strong><span>${escapeHtml((item.metricType || (item.streamCount == null ? "rank" : "streams")).toLowerCase())}</span>${showTrend ? trendButton(item.entityUri, item.entityName) : ""}</div></div>`;
  }

  function regionEntryRow(item) {
    const url = spotifyLink(item.entityUri);
    const title = escapeHtml(item.entityName || "Untitled");
    const titleMarkup = url
      ? `<a class="focus-row-title" href="${escapeHtml(url)}" target="_blank" rel="noreferrer">${title}</a>`
      : `<strong class="focus-row-title">${title}</strong>`;
    const subtitle = item.artistNames || (state.entityType === "artist" ? "Selected artist" : "");
    const artwork = item.imageUrl
      ? `<img class="focus-row-art" src="${escapeHtml(item.imageUrl)}" alt="" loading="lazy" />`
      : '<span class="focus-row-art" aria-hidden="true"></span>';
    return `<div class="focus-data-row has-art"><div class="focus-row-rank">${escapeHtml(item.chartRank ?? "—")}${movementMarkup(item)}</div>${artwork}<div class="focus-row-copy">${titleMarkup}<span class="focus-row-subtitle">${escapeHtml(subtitle)}</span></div><div class="focus-row-metric"><strong>${escapeHtml(formatNumber(item.metricValue ?? item.streamCount))}</strong><span>${escapeHtml((item.metricType || (item.streamCount == null ? "rank" : "streams")).toLowerCase())}</span>${item.entityUri ? trendButton(item.entityUri, item.entityName) : ""}</div></div>`;
  }

  function renderReleaseCard(release, isHistory = false) {
    const url = spotifyLink(release.entityUri);
    const title = escapeHtml(release.entityName || "Untitled");
    const titleMarkup = url
      ? `<a class="focus-release-title" href="${escapeHtml(url)}" target="_blank" rel="noreferrer">${title}</a>`
      : `<strong class="focus-release-title">${title}</strong>`;
    const artistNames = release.artistNames || (state.entityType === "artist" ? "Selected artist" : entityTypeName());
    const regions = release.regions || [];
    return `<article class="focus-release-card"><header class="focus-release-heading">${coverMarkup(release.imageUrl, release.entityName)}<div class="focus-release-copy">${titleMarkup}<p class="focus-release-subtitle">${escapeHtml(artistNames)}</p></div><span class="focus-region-count">${regions.length} ${regions.length === 1 ? "region" : "regions"}</span></header><div class="focus-data-list">${regions.map((region) => releaseRegionRow(region, isHistory)).join("")}</div></article>`;
  }

  function renderRegionCard(region, isHistory = false) {
    const entries = region.entries || [];
    const subtitle = isHistory ? `${entries.length} saved releases` : chartTypeName();
    const date = !isHistory && region.chartDate ? `<time class="focus-date">${escapeHtml(region.chartDate)}</time>` : "";
    return `<article class="focus-region-card"><header class="focus-card-title"><strong>${escapeHtml(region.regionName)}</strong><span>${escapeHtml(subtitle)}</span>${date}</header><div class="focus-data-list">${entries.map((entry) => regionEntryRow(entry)).join("")}</div></article>`;
  }

  function renderResultCards(containerId, releases, regions, isHistory = false) {
    const container = byId(containerId);
    const items = state.view === "release" ? releases : regions;
    if (!items.length) {
      container.innerHTML = `<p class="focus-empty">No ${escapeHtml(focusedSelectionName())} ${escapeHtml(entityTypeName())} chart entries were found in ${isHistory ? "the saved history range" : "the current charts"}.${isHistory ? " More history appears as regional chart snapshots are collected." : ""}</p>`;
      return;
    }
    container.innerHTML = state.view === "release"
      ? items.map((release) => renderReleaseCard(release, isHistory)).join("")
      : items.map((region) => renderRegionCard(region, isHistory)).join("");
  }

  function renderLive(payload) {
    byId("live-results-title").textContent = state.view === "release"
      ? `${focusedSelectionName()} ${state.entityType === "artist" ? "in the artist chart" : `${state.entityType === "track" ? "songs" : "albums"} charting now`}`
      : `${chartTypeName()} by region`;
    const count = state.view === "release" ? payload.releases.length : payload.regions.length;
    const matchingRegionLabel = payload.matchingRegions === 1 ? "region" : "regions";
    byId("live-results-count").textContent = state.view === "release"
      ? `${count} ${count === 1 ? "result" : "results"} · ${payload.matchingRegions} ${matchingRegionLabel}`
      : `${count} regions · ${payload.marketsScanned} markets scanned`;
    renderResultCards("live-results", payload.releases, payload.regions);
    const failures = payload.failedRegions?.length || 0;
    const marketMessage = payload.marketScope === "global"
      ? "Global chart loaded. Scan all regions to find more placements."
      : payload.snapshotReused
        ? `Today’s saved results loaded from History (${payload.marketsScanned} of ${payload.marketsTotal} markets).`
        : `${payload.marketsScanned} of ${payload.marketsTotal} markets scanned.`;
    const regionLabel = payload.matchingRegions === 1 ? "region" : "regions";
    const message = `${payload.artist.entityName}: ${payload.matchingRegions} ${regionLabel} charting ${entityTypeName()}s in the ${state.cadence} charts. ${marketMessage}`;
    const storageMessage = payload.historyStorageAvailable == null
      ? ""
      : payload.historyStorageAvailable
        ? `${payload.historySavedRegions} regional snapshots saved for History.`
        : "Live charts loaded; history storage is unavailable right now.";
    const partialMessage = failures ? ` ${failures} regions could not be loaded.` : "";
    setStatus("live-status", `${message}${storageMessage ? ` ${storageMessage}` : ""}${partialMessage}`, failures > 0 && payload.marketsScanned === 0);
    updateLiveActionLabel();
  }

  function buildGlobalOverview(chart) {
    const chartDate = String(chart.displayChart?.date || "Latest");
    const metadataKey = { track: "trackMetadata", artist: "artistMetadata", album: "albumMetadata" }[state.entityType];
    const selectedUris = new Set(trackedEntities
      .filter((entity) => entity.entityType === state.entityType)
      .map((entity) => entity.entityUri));
    const entries = (chart.entries || []).filter((entry) => {
      const metadata = entry[metadataKey] || {};
      const entityUri = metadata.trackUri || metadata.artistUri || metadata.albumUri
        || entry.trackUri || entry.artistUri || entry.albumUri;
      return selectedUris.has(entityUri);
    }).map((entry) => {
      const metadata = entry[metadataKey] || {};
      const stats = entry.chartEntryData || {};
      const metric = stats.rankingMetric || {};
      const uri = metadata.trackUri || metadata.artistUri || metadata.albumUri || entry.trackUri || entry.artistUri || entry.albumUri;
      if (!uri) return null;
      return {
        entityUri: uri,
        entityType: state.entityType,
        entityName: metadata.trackName || metadata.artistName || metadata.albumName || "Untitled",
        artistNames: state.entityType === "artist" ? (metadata.artistName || "Selected artist") : (metadata.artists || []).map((artist) => artist?.name).filter(Boolean).join(", " ),
        imageUrl: metadata.displayImageUri || null,
        regionCode: "GLOBAL",
        regionName: "Global",
        chartRank: stats.currentRank,
        previousRank: stats.previousRank,
        peakRank: stats.peakRank,
        consecutiveAppearancesOnChart: stats.consecutiveAppearancesOnChart,
        appearancesOnChart: stats.appearancesOnChart,
        entryDate: stats.entryDate,
        metricType: metric.type,
        metricValue: metric.value ?? null,
        chartDate,
      };
    }).filter(Boolean);
    const regions = entries.length ? [{ regionCode: "GLOBAL", regionName: "Global", chartDate, entries }] : [];
    const releases = entries.map((entry) => ({
      entityUri: entry.entityUri,
      entityType: entry.entityType,
      entityName: entry.entityName,
      artistNames: entry.artistNames,
      imageUrl: entry.imageUrl,
      regions: [{
        regionCode: entry.regionCode,
        regionName: entry.regionName,
        chartDate: entry.chartDate,
        chartRank: entry.chartRank,
        previousRank: entry.previousRank,
        peakRank: entry.peakRank,
        consecutiveAppearancesOnChart: entry.consecutiveAppearancesOnChart,
        appearancesOnChart: entry.appearancesOnChart,
        entryDate: entry.entryDate,
        metricType: entry.metricType,
        metricValue: entry.metricValue,
      }],
    }));
    return {
      artist: { entityName: "Taylor Swift and Felix" },
      cadence: state.cadence,
      entityType: state.entityType,
      chartType: `${state.cadence}_top_${{ track: "songs", artist: "artists", album: "albums" }[state.entityType]}`,
      marketScope: "global",
      marketsTotal: 1,
      marketsScanned: 1,
      matchingRegions: regions.length,
      failedRegions: [],
      releases,
      regions,
    };
  }

  async function fetchAllRegionsOverview(cadence, entityType, refresh = false) {
    const params = new URLSearchParams({ cadence, entity_type: entityType, scope: "all" });
    let refreshScan = refresh;
    while (true) {
      if (refreshScan) params.set("refresh", "true");
      const payload = await requestJson(`/api/focused/live?${params}`);
      params.delete("refresh");
      refreshScan = false;
      if (payload.scanError) throw new Error(payload.scanError);
      if (!payload.scanInProgress) return payload;
      if (state.cadence !== cadence || state.entityType !== entityType) throw new Error("The chart selection changed while regions were loading.");
      setStatus("live-status", "Global chart is loaded. The regional scan is running in the background and will update automatically.");
      await new Promise((resolve) => window.setTimeout(resolve, 3000));
    }
  }

  async function fetchLiveOverview(refresh = false) {
    let payload;
    if (!trackedEntities.length) await loadTrackedEntities();
    if (state.marketScope === "global") {
      const chartType = `${state.cadence}_top_${{ track: "songs", artist: "artists", album: "albums" }[state.entityType]}`;
      const cachedChart = getCachedChart(chartType, "GLOBAL", "latest");
      const params = new URLSearchParams({ type: chartType, country: "GLOBAL", date: "latest" });
      const chart = cachedChart || await requestJson(`/api/charts?${params}`);
      payload = buildGlobalOverview(chart);
    } else {
      payload = await fetchAllRegionsOverview(state.cadence, state.entityType, refresh);
    }
    return payload;
  }

  async function loadAllRegionsAutomatically(requestId) {
    isAutoRegionScanning = true;
    const cadence = state.cadence;
    const entityType = state.entityType;
    const button = byId("live-refresh");
    button.disabled = true;
    button.textContent = "Scanning regions…";
    setStatus("live-status", `Global chart loaded. Scanning all regions for ${focusedSelectionName()} ${entityTypeName()} charts…`);

    try {
      const payload = await fetchAllRegionsOverview(cadence, entityType);
      if (requestId !== liveRequest || activePage !== "live" || state.cadence !== cadence || state.entityType !== entityType) return;
      state.marketScope = "all";
      liveOverview = payload;
      liveKey = currentKey();
      liveLoadedAt = Date.now();
      renderLive(payload);
    } catch (error) {
      if (requestId === liveRequest && activePage === "live") {
        setStatus("live-status", `The Global chart is loaded, but the automatic regional scan failed: ${error.message}`, true);
      }
    } finally {
      if (requestId === liveRequest) {
        isAutoRegionScanning = false;
        button.disabled = false;
        updateLiveActionLabel();
      }
    }
  }

  async function loadFocusedLive(refresh = false) {
    isAutoRegionScanning = false;
    const requestId = ++liveRequest;
    byId("live-refresh").disabled = true;
    byId("live-refresh").textContent = state.marketScope === "global" ? "Loading Global…" : "Loading regions…";
    byId("live-results-count").textContent = state.marketScope === "global" ? "Loading Global chart…" : "Scanning all Spotify regions…";
    setStatus("live-status", state.marketScope === "global"
      ? `Loading ${focusedSelectionName()} ${entityTypeName()} chart for Global…`
      : `Loading ${focusedSelectionName()} ${entityTypeName()} charts across all regions…`);
    try {
      const payload = await fetchLiveOverview(refresh);
      if (requestId === liveRequest && activePage === "live") {
        liveOverview = payload;
        liveKey = currentKey();
        liveLoadedAt = Date.now();
        renderLive(payload);
        if (payload.marketScope === "global") void loadAllRegionsAutomatically(requestId);
      }
    } catch (error) {
      if (requestId === liveRequest) {
        if (state.marketScope === "all" && liveOverview?.marketScope === "global") {
          state.marketScope = "global";
          renderLive(liveOverview);
          setStatus("live-status", `The full regional scan failed: ${error.message} The Global chart is still shown.`, true);
        } else {
          byId("live-results-count").textContent = "Charts unavailable";
          byId("live-results").innerHTML = `<p class="focus-empty">${escapeHtml(error.message)}</p>`;
          setStatus("live-status", error.message, true);
        }
      }
    } finally {
      if (requestId === liveRequest && !isAutoRegionScanning) {
        byId("live-refresh").disabled = false;
        updateLiveActionLabel();
      }
    }
  }

  function renderHistory(payload) {
    historyOverview = payload;
    historyTrendData = null;
    byId("history-chart-card").hidden = true;
    const covers = new Map((liveOverview?.releases || []).map((release) => [release.entityUri, release.imageUrl]));
    payload.releases.forEach((release) => { release.imageUrl = covers.get(release.entityUri) || null; });
    byId("history-results-title").textContent = state.view === "release" ? `${focusedSelectionName()} history` : `${focusedSelectionName()} by market`;
    byId("history-results-count").textContent = `${payload.releases.length} entities · ${payload.regions.length} markets · ${Number(payload.totalRows || 0).toLocaleString()} saved appearances`;
    const pagination = byId("history-pagination");
    pagination.hidden = payload.totalRows <= HISTORY_PAGE_SIZE;
    byId("history-page-label").textContent = payload.totalRows
      ? `${historyOffset + 1}–${historyOffset + payload.snapshotRows} of ${Number(payload.totalRows).toLocaleString()}`
      : "No saved appearances";
    byId("history-page-previous").disabled = historyOffset === 0;
    byId("history-page-next").disabled = !payload.hasMore;
    renderResultCards("history-results", payload.releases, payload.regions, true);
    const empty = payload.snapshotRows === 0;
    const nextStep = empty ? "No saved appearances are available for this chart and range yet. Configure tracking above to continue." : "Choose View trend on an entity to compare its rank or streams over time.";
    setStatus("selected-history-status", `${payload.snapshotRows.toLocaleString()} saved appearances across ${payload.regions.length} markets. ${nextStep}`);
    byId("history-chart").hidden = true;
    byId("history-empty").hidden = false;
    byId("history-empty").textContent = "Choose a release below to compare its chart history across regions.";
    byId("history-chart-title").textContent = `${focusedSelectionName()} history by region`;
    byId("history-chart-range").textContent = "Choose a release to view its trend";
    if (historyChart) {
      historyChart.destroy();
      historyChart = null;
    }
  }

  async function loadCollectionJobs() {
    const params = new URLSearchParams({
      status: byId("collection-job-status").value,
      limit: "10",
      offset: String(collectionJobsOffset),
    });
    const chartType = byId("collection-job-chart").value;
    const country = byId("collection-job-country").value;
    const from = byId("collection-job-from").value;
    const to = byId("collection-job-to").value;
    if (chartType) params.set("chart_type", chartType);
    if (country) params.set("country", country);
    if (from) params.set("date_from", from);
    if (to) params.set("date_to", to);
    const payload = await requestJson(`/api/collection/jobs?${params}`);
    byId("collection-jobs-list").innerHTML = payload.items.length ? payload.items.map((job) => {
      const canRetry = ["retry", "failed", "unavailable"].includes(job.status) && job.requested_date !== "latest";
      const date = job.chart_date || job.requested_date || "Date unknown";
      const retryButton = canRetry
        ? `<button class="quiet-button" type="button" data-retry-chart-job data-chart-type="${escapeHtml(job.chart_type)}" data-country="${escapeHtml(job.country_code)}" data-requested-date="${escapeHtml(job.requested_date)}">${job.status === "unavailable" ? "Recheck date" : "Retry now"}</button>`
        : "";
      const detail = [job.last_http_status ? `HTTP ${job.last_http_status}` : "", job.last_error || "", job.next_attempt_at ? `Next attempt ${new Date(job.next_attempt_at).toLocaleString()}` : ""]
        .filter(Boolean).join(" · ");
      return `<div class="history-run-row"><span>${escapeHtml(date)} · ${escapeHtml(job.chart_type.replaceAll("_", " "))} · ${escapeHtml(job.country_code)} · ${escapeHtml(job.status)}${detail ? `<small class="history-run-error">${escapeHtml(detail)}</small>` : ""}</span><span>${Number(job.attempt_count || 0)} attempts ${retryButton}</span></div>`;
    }).join("") : '<p class="history-empty-row">No chart-date jobs match these filters.</p>';
    const pagination = byId("collection-jobs-pagination");
    pagination.hidden = payload.total <= 10;
    byId("collection-jobs-page-label").textContent = payload.total
      ? `${collectionJobsOffset + 1}–${collectionJobsOffset + payload.items.length} of ${Number(payload.total).toLocaleString()} jobs`
      : "No chart-date jobs";
    byId("collection-jobs-page-indicator").textContent = byId("collection-jobs-page-label").textContent;
    byId("collection-jobs-previous").disabled = collectionJobsOffset === 0;
    byId("collection-jobs-next").disabled = !payload.hasMore;
  }

  async function loadHistoryManagement() {
    try {
      const [overview, configs, selections, countries, runs, schedule] = await Promise.all([
        requestJson("/api/history/overview"),
        requestJson("/api/tracking/configs"),
        requestJson("/api/selections"),
        requestJson("/api/countries"),
        requestJson(`/api/collection/runs?limit=8&offset=${collectionRunsOffset}`),
        requestJson("/api/collection/schedule"),
      ]);
      byId("history-overview-grid").innerHTML = [
        [overview.tracked_entities, "Tracked entities"],
        [overview.active_configs, "Active chart configs"],
        [overview.snapshot_rows, "Saved appearances"],
        [overview.retry_jobs, "Dates needing retry"],
      ].map(([value, label]) => `<div class="history-stat"><strong>${Number(value || 0).toLocaleString()}</strong><span>${escapeHtml(label)}</span></div>`).join("");
      const entitySelect = byId("tracking-entity");
      entitySelect.innerHTML = selections.length
        ? selections.map((entity) => `<option value="${escapeHtml(entity.entityUri)}" data-type="${escapeHtml(entity.entityType)}">${escapeHtml(entity.entityName)} · ${escapeHtml(entity.entityType)}</option>`).join("")
        : '<option value="">Save an entity first</option>';
      byId("tracking-country").innerHTML = countries.map((country) => `<option value="${escapeHtml(country.code)}">${escapeHtml(country.name)}</option>`).join("");
      byId("collection-job-country").innerHTML = '<option value="">All markets</option>' + countries.map((country) => `<option value="${escapeHtml(country.code)}">${escapeHtml(country.name)}</option>`).join("");
      byId("history-detail-country").innerHTML = '<option value="">All markets</option>' + countries.map((country) => `<option value="${escapeHtml(country.code)}">${escapeHtml(country.name)}</option>`).join("");
      byId("history-detail-chart").innerHTML = '<option value="">All charts</option>' + [
        ["daily_top_songs", "Daily top songs"], ["weekly_top_songs", "Weekly top songs"],
        ["daily_top_artists", "Daily top artists"], ["weekly_top_artists", "Weekly top artists"],
        ["weekly_top_albums", "Weekly top albums"],
      ].map(([value, label]) => `<option value="${value}">${label}</option>`).join("");
      const setChartOptions = () => {
        const selected = entitySelect.selectedOptions[0];
        const type = selected?.dataset.type;
        const options = type === "artist"
          ? [["daily_top_artists", "Daily top artists"], ["weekly_top_artists", "Weekly top artists"]]
          : type === "album" ? [["weekly_top_albums", "Weekly top albums"]]
            : [["daily_top_songs", "Daily top songs"], ["weekly_top_songs", "Weekly top songs"]];
        byId("tracking-chart").innerHTML = options.map(([value, label]) => `<option value="${value}">${label}</option>`).join("");
      };
      entitySelect.onchange = setChartOptions;
      setChartOptions();
      byId("tracking-config-list").innerHTML = configs.length ? configs.map((config) => `
        <div class="history-config-row">
          <div class="history-config-copy"><strong>${escapeHtml(config.entity_name)} · ${escapeHtml(config.chart_type.replaceAll("_", " "))}</strong><span>${escapeHtml(config.country_code)} · ${config.backfill_start_date ? `from ${escapeHtml(config.backfill_start_date)}` : "start date not set"} · ${config.enabled ? "Active" : "Paused"}</span></div>
          <button class="quiet-button" type="button" data-toggle-config="${escapeHtml(config.entity_uri)}" data-config-chart="${escapeHtml(config.chart_type)}" data-config-country="${escapeHtml(config.country_code)}" data-config-enabled="${String(config.enabled)}">${config.enabled ? "Pause" : "Resume"}</button>
        </div>`).join("") : '<p class="history-empty-row">No chart tracking configured yet.</p>';
      byId("collection-runs-list").innerHTML = runs.items.length ? runs.items.map((run) => `<div class="history-run-row"><span>Run #${run.id} · ${escapeHtml(run.source)} · ${escapeHtml(run.status)}${run.summary_error ? `<small class="history-run-error">${escapeHtml(run.summary_error)}</small>` : ""}</span><span>${Number(run.completed_chart_keys || 0)} / ${Number(run.requested_chart_keys || 0)} completed · ${Number(run.unavailable_chart_keys || 0)} unavailable · ${Number(run.failed_chart_keys || 0)} failed · ${run.created_at ? escapeHtml(new Date(run.created_at).toLocaleString()) : ""}</span></div>`).join("") : '<p class="history-empty-row">No collection runs recorded.</p>';
      const runsPagination = byId("collection-runs-pagination");
      runsPagination.hidden = runs.total <= 8;
      byId("collection-runs-page-label").textContent = runs.total
        ? `${collectionRunsOffset + 1}–${collectionRunsOffset + runs.items.length} of ${Number(runs.total).toLocaleString()}`
        : "No collection runs";
      byId("collection-runs-previous").disabled = collectionRunsOffset === 0;
      byId("collection-runs-next").disabled = !runs.hasMore;
      const nextRun = schedule.next_run_at ? new Date(schedule.next_run_at).toLocaleString() : "Schedule disabled";
      byId("collection-next-run").textContent = nextRun;
      byId("collection-time").value = String(schedule.local_time || "09:00").slice(0, 5);
      byId("collection-timezone").value = schedule.timezone || "Africa/Algiers";
      byId("collection-enabled").checked = Boolean(schedule.enabled);
      void loadHistoryEntities();
      void loadCollectionJobs().catch((error) => {
        byId("collection-jobs-list").innerHTML = `<p class="history-empty-row">${escapeHtml(error.message)}</p>`;
      });
    } catch (error) {
      byId("history-overview-grid").innerHTML = `<p class="history-empty-row">${escapeHtml(error.message)}</p>`;
    }
  }

  async function loadHistoryEntities() {
    const params = new URLSearchParams({
      q: byId("history-entity-query").value.trim(),
      entity_type: byId("history-entity-type").value,
      sort: byId("history-entity-sort").value,
      limit: String(HISTORY_ENTITY_PAGE_SIZE),
      offset: String(historyEntitiesOffset),
    });
    try {
      const payload = await requestJson(`/api/history/entities?${params}`);
      byId("tracked-entities-count").textContent = `${Number(payload.total).toLocaleString()} tracked entities`;
      byId("history-entities-list").innerHTML = payload.items.length ? payload.items.map((entity) => `
        <div class="history-config-row">
          <div class="history-config-copy"><strong>${escapeHtml(entity.entity_name)} · ${escapeHtml(entity.entity_type)}</strong><span>${Number(entity.appearance_count).toLocaleString()} saved appearances · ${entity.latest_appearance ? `latest ${escapeHtml(entity.latest_appearance)}` : "no saved appearances"} · ${Number(entity.active_configs).toLocaleString()} active configs</span></div>
          <button class="quiet-button" type="button" data-history-entity="${escapeHtml(entity.entity_uri)}">View detail</button>
        </div>`).join("") : '<p class="history-empty-row">No tracked entities match these filters.</p>';
      const pagination = byId("history-entities-pagination");
      pagination.hidden = payload.total <= HISTORY_ENTITY_PAGE_SIZE;
      byId("history-entities-page-label").textContent = payload.total
        ? `${historyEntitiesOffset + 1}–${historyEntitiesOffset + payload.items.length} of ${Number(payload.total).toLocaleString()}`
        : "No tracked entities";
      byId("history-entities-previous").disabled = historyEntitiesOffset === 0;
      byId("history-entities-next").disabled = !payload.hasMore;
    } catch (error) {
      byId("tracked-entities-count").textContent = "Tracked history unavailable";
      byId("history-entities-list").innerHTML = `<p class="history-empty-row">${escapeHtml(error.message)}</p>`;
    }
  }

  async function loadHistoryEntityDetail(entityUri = selectedHistoryEntityUri) {
    if (!entityUri) return;
    selectedHistoryEntityUri = entityUri;
    const params = new URLSearchParams({
      entity_uri: entityUri,
      limit: String(HISTORY_ENTITY_DETAIL_PAGE_SIZE),
      offset: String(historyEntityDetailOffset),
    });
    const country = byId("history-detail-country").value;
    const chartType = byId("history-detail-chart").value;
    const dateFrom = byId("history-detail-from").value;
    const dateTo = byId("history-detail-to").value;
    if (country) params.set("country", country);
    if (chartType) params.set("chart_type", chartType);
    if (dateFrom) params.set("date_from", dateFrom);
    if (dateTo) params.set("date_to", dateTo);

    try {
      const payload = await requestJson(`/api/history/entity?${params}`);
      const entity = payload.entity;
      const summary = payload.summary;
      byId("history-entity-detail").hidden = false;
      byId("history-entity-detail-title").textContent = `${entity.entity_name} history`;
      byId("history-entity-summary").textContent = `${Number(summary.appearances).toLocaleString()} appearances · peak #${summary.peak_rank ?? "—"} · ${summary.first_appearance || "—"} to ${summary.latest_appearance || "—"}${entity.is_archived ? " · archived" : ""}`;
      const items = [...payload.items].reverse();
      const labels = [...new Set(items.map((item) => item.chart_date))].sort();
      const series = [...new Set(items.map((item) => `${item.country_code} · ${item.chart_type.replaceAll("_", " ")}`))];
      const metric = byId("history-detail-metric").value;
      const valueFor = (item) => metric === "rank" ? item.chart_rank : item.stream_count;
      const hasValues = items.some((item) => valueFor(item) != null);
      const canvas = byId("tracked-entity-chart");
      const empty = byId("tracked-entity-chart-empty");
      if (trackedEntityChart) {
        trackedEntityChart.destroy();
        trackedEntityChart = null;
      }
      canvas.hidden = !hasValues;
      empty.hidden = hasValues;
      empty.textContent = items.length ? "No stored values are available for the selected metric." : "No saved chart history matches these filters.";
      canvas.setAttribute("aria-label", `${metric === "rank" ? "Rank" : "Streams"} history for ${entity.entity_name}`);
      if (hasValues && window.Chart) {
        const colors = ["#187a45", "#3478c5", "#e37b26", "#8b5cc7", "#ca4f69", "#148b99"];
        const datasets = series.map((label, index) => {
          const matching = items.filter((item) => `${item.country_code} · ${item.chart_type.replaceAll("_", " ")}` === label);
          const values = new Map(matching.map((item) => [item.chart_date, valueFor(item)]));
          return { label, data: labels.map((day) => values.get(day) ?? null), borderColor: colors[index % colors.length], backgroundColor: colors[index % colors.length] + "20", borderWidth: 2, pointRadius: labels.length > 40 ? 0 : 2, tension: 0.2, spanGaps: false };
        });
        trackedEntityChart = new window.Chart(canvas.getContext("2d"), {
          type: "line",
          data: { labels, datasets },
          options: { responsive: true, maintainAspectRatio: false, interaction: { intersect: false, mode: "index" }, plugins: { legend: { display: series.length > 1, position: "bottom" } }, scales: { y: { reverse: metric === "rank", beginAtZero: metric !== "rank" }, x: { ticks: { maxTicksLimit: 8 } } } },
        });
      }
      byId("history-entity-points").innerHTML = payload.items.length ? payload.items.map((item) => `<div class="history-entity-point"><strong>${escapeHtml(item.chart_date)}</strong><span>${escapeHtml(item.country_code)}</span><span>${escapeHtml(item.chart_type.replaceAll("_", " "))}</span><span>#${escapeHtml(item.chart_rank ?? "—")} · ${item.stream_count == null ? "—" : Number(item.stream_count).toLocaleString()} streams</span></div>`).join("") : '<p class="history-empty-row">No saved chart history matches these filters.</p>';
      const pagination = byId("history-entity-detail-pagination");
      pagination.hidden = payload.total <= HISTORY_ENTITY_DETAIL_PAGE_SIZE;
      byId("history-entity-detail-page-label").textContent = payload.total
        ? `${historyEntityDetailOffset + 1}–${historyEntityDetailOffset + payload.items.length} of ${Number(payload.total).toLocaleString()}`
        : "No appearances";
      byId("history-entity-detail-previous").disabled = historyEntityDetailOffset === 0;
      byId("history-entity-detail-next").disabled = !payload.hasMore;
    } catch (error) {
      byId("history-entity-detail").hidden = false;
      byId("history-entity-points").innerHTML = `<p class="history-empty-row">${escapeHtml(error.message)}</p>`;
    }
  }

  async function loadFocusedHistory(refreshLive = false) {
    void loadHistoryManagement();
    const requestId = ++historyRequest;
    setStatus("selected-history-status", `Loading ${focusedSelectionName()} snapshots across regions…`);
    try {
      const key = currentKey();
      if (refreshLive || liveKey !== key || Date.now() - liveLoadedAt > 120000) {
        const payload = await fetchLiveOverview(refreshLive);
        if (requestId !== historyRequest) return;
        liveOverview = payload;
        liveKey = key;
        liveLoadedAt = Date.now();
      }
      if (requestId !== historyRequest) return;
      const params = new URLSearchParams({
        cadence: state.cadence,
        entity_type: state.entityType,
        range: byId("history-range").value,
        limit: String(HISTORY_PAGE_SIZE),
        offset: String(historyOffset),
      });
      const payload = await requestJson(`/api/focused/history?${params}`);
      if (requestId === historyRequest && activePage === "history") renderHistory(payload);
    } catch (error) {
      if (requestId === historyRequest) {
        byId("history-results-count").textContent = "History unavailable";
        byId("history-results").innerHTML = `<p class="focus-empty">${escapeHtml(error.message)}</p>`;
        setStatus("selected-history-status", error.message, true);
      }
    }
  }

  function renderHistoryTrend(payload) {
    historyTrendData = payload;
    const points = payload.points || [];
    const metric = byId("history-metric").value;
    const canvas = byId("history-chart");
    const empty = byId("history-empty");
    const labels = [...new Set(points.map((point) => point.chartDate))].sort();
    const regions = [...new Set(points.map((point) => point.regionCode))].sort((left, right) => {
      if (left === "GLOBAL") return -1;
      if (right === "GLOBAL") return 1;
      const leftName = points.find((point) => point.regionCode === left)?.regionName || left;
      const rightName = points.find((point) => point.regionCode === right)?.regionName || right;
      return leftName.localeCompare(rightName);
    });
    const palette = ["#187a45", "#3478c5", "#e37b26", "#8b5cc7", "#ca4f69", "#148b99", "#6d7c2e", "#525f73"];
    const hasValues = points.some((point) => (metric === "rank" ? point.chartRank : point.streamCount) != null);
    if (historyChart) {
      historyChart.destroy();
      historyChart = null;
    }
    canvas.hidden = !hasValues;
    empty.hidden = hasValues;
    if (hasValues && window.Chart) {
      const datasets = regions.map((regionCode, index) => {
        const regionPoints = points.filter((point) => point.regionCode === regionCode);
        const values = new Map(regionPoints.map((point) => [point.chartDate, metric === "rank" ? point.chartRank : point.streamCount]));
        return {
          label: regionPoints[0]?.regionName || regionCode,
          data: labels.map((label) => values.get(label) ?? null),
          borderColor: palette[index % palette.length],
          backgroundColor: palette[index % palette.length] + "20",
          borderWidth: 2,
          pointRadius: labels.length > 40 ? 0 : 2,
          pointHoverRadius: 5,
          tension: 0.25,
          spanGaps: false,
        };
      });
      historyChart = new window.Chart(canvas.getContext("2d"), {
        type: "line",
        data: { labels, datasets },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { intersect: false, mode: "index" },
          plugins: {
            legend: { display: regions.length > 1, position: "bottom", labels: { color: "#64748b", usePointStyle: true, boxWidth: 8 } },
            tooltip: { callbacks: { label: (context) => `${context.dataset.label}: ${Number(context.parsed.y).toLocaleString()}` } },
          },
          scales: {
            x: { ticks: { color: "#7b8798", maxTicksLimit: 8 }, grid: { color: "rgba(100,116,139,.10)" } },
            y: {
              reverse: metric === "rank",
              beginAtZero: metric === "streams",
              ticks: { color: "#7b8798", callback: (value) => Number(value).toLocaleString() },
              grid: { color: "rgba(100,116,139,.10)" },
            },
          },
        },
      });
    }
    const entityName = payload.entity?.entityName || focusedSelectionName();
    byId("history-chart-card").hidden = false;
    byId("history-chart-title").textContent = `${entityName} · ${metric === "rank" ? "rank" : "streams"} by region`;
    byId("history-chart-range").textContent = `${points.length} snapshots · ${regions.length} regions`;
    if (!hasValues) empty.textContent = "No saved chart snapshots were found for this release in the selected range.";
    setStatus("selected-history-status", `${points.length.toLocaleString()} stored snapshots for ${entityName} across ${regions.length} regions.`);
    byId("history-chart-card").scrollIntoView({ behavior: "smooth", block: "start" });
  }

  async function viewHistoryTrend(entityUri) {
    const params = new URLSearchParams({
      entity_uri: entityUri,
      cadence: state.cadence,
      entity_type: state.entityType,
      range: byId("history-range").value,
    });
    setStatus("selected-history-status", "Loading the release history across regions…");
    try {
      const payload = await requestJson(`/api/focused/history/trend?${params}`);
      renderHistoryTrend(payload);
    } catch (error) {
      setStatus("selected-history-status", error.message, true);
    }
  }

  function activatePage(page) {
    const liveRoutes = {
      "live-daily-songs": { cadence: "daily", entityType: "track", title: "Daily top songs", type: "songs" },
      "live-weekly-songs": { cadence: "weekly", entityType: "track", title: "Weekly top songs", type: "songs" },
      "live-daily-artists": { cadence: "daily", entityType: "artist", title: "Daily top artists", type: "artists" },
      "live-weekly-artists": { cadence: "weekly", entityType: "artist", title: "Weekly top artists", type: "artists" },
      "live-weekly-albums": { cadence: "weekly", entityType: "album", title: "Weekly top albums", type: "albums" },
    };
    const historyRouteKey = page.startsWith("history-") ? page.replace(/^history-/, "live-") : "";
    const routeKey = page === "live" ? "live-daily-songs" : historyRouteKey || page;
    const route = liveRoutes[routeKey];
    const isChartHistory = Boolean(route && historyRouteKey);
    const validPage = route ? (isChartHistory ? "history" : "live") : ["charts", "history"].includes(page) ? page : "charts";
    if (route) {
      state.cadence = route.cadence;
      state.entityType = route.entityType;
      state.view = "release";
      state.marketScope = "global";
      byId("live-title-label").textContent = route.title;
      byId("live-eyebrow").textContent = `${route.title} chart`;
      byId("live-description").textContent = `Explore your tracked entities on the Global ${route.title.toLowerCase()} chart.`;
      byId("live-view-control").hidden = route.entityType === "artist";
      byId("history-title-label").textContent = isChartHistory ? `${route.title} history` : "History";
      byId("history-eyebrow").textContent = isChartHistory ? `${route.title} history` : "Tracked chart history";
      byId("history-description").textContent = isChartHistory
        ? `Saved ${route.title.toLowerCase()} snapshots for your configured entities and markets.`
        : "Review saved appearances, tracked entities, and collection activity.";
      updateFocusControls();
      updateLiveActionLabel();
    } else if (page === "history") {
      state.cadence = "daily";
      state.entityType = "track";
      state.view = "release";
      state.marketScope = "global";
      byId("history-title-label").textContent = "History";
      byId("history-eyebrow").textContent = "Tracked chart history";
      byId("history-description").textContent = "Review saved appearances, tracked entities, and collection activity.";
      updateFocusControls();
    }
    document.querySelectorAll("[data-chart-tabs]").forEach((tabs) => {
      const showTabs = Boolean(route && (validPage === "live" || isChartHistory));
      tabs.hidden = !showTabs;
      if (!showTabs) return;
      tabs.querySelector('[data-chart-tab="live"]').href = `#${routeKey}`;
      tabs.querySelector('[data-chart-tab="history"]').href = `#history-${routeKey.replace(/^live-/, "")}`;
      tabs.querySelectorAll("[data-chart-tab]").forEach((tab) => {
        const selected = tab.dataset.chartTab === (validPage === "history" ? "history" : "live");
        if (selected) tab.setAttribute("aria-current", "page");
        else tab.removeAttribute("aria-current");
      });
    });
    byId("history-cadence-control").hidden = isChartHistory;
    byId("history-entity-control").hidden = isChartHistory;
    activePage = validPage;
    document.querySelectorAll("[data-page-panel]").forEach((panel) => {
      panel.hidden = panel.dataset.pagePanel !== validPage;
    });
    document.querySelectorAll("[data-page-nav]").forEach((link) => {
      const active = link.dataset.pageNav === routeKey;
      link.classList.toggle("active", active);
      if (active) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    });
    if (validPage === "live") loadFocusedLive();
    if (validPage === "history") loadFocusedHistory();
  }

  document.querySelectorAll("[data-focus-key]").forEach((button) => {
    button.addEventListener("click", () => {
      const key = button.dataset.focusKey;
      const value = button.dataset.focusValue;
      if (state[key] === value) return;
      state[key] = value;
      updateFocusControls();
      if (key === "view") {
        if (activePage === "live" && liveOverview) renderLive(liveOverview);
        if (activePage === "history" && historyOverview) renderHistory(historyOverview);
      } else if (activePage === "live") {
        state.marketScope = "global";
        updateLiveActionLabel();
        loadFocusedLive();
      } else if (activePage === "history") {
        historyOffset = 0;
        loadFocusedHistory();
      }
    });
  });

  byId("live-refresh").addEventListener("click", () => {
    state.marketScope = "all";
    loadFocusedLive(true);
  });
  byId("history-refresh").addEventListener("click", () => loadFocusedHistory(true));
  byId("collection-run-now").addEventListener("click", async () => {
    const button = byId("collection-run-now");
    button.disabled = true;
    button.textContent = "Collecting…";
    try {
      const result = await requestJson("/api/collection/run", { method: "POST" });
      setStatus("selected-history-status", `Collection ${result.status}: ${Number(result.completedChartKeys || 0)} of ${Number(result.requestedChartKeys || 0)} chart keys processed.`);
      await loadHistoryManagement();
      await loadFocusedHistory(true);
    } catch (error) {
      setStatus("selected-history-status", error.message, true);
    } finally {
      button.disabled = false;
      button.textContent = "Run collection now";
    }
  });
  byId("history-range").addEventListener("change", () => {
    historyOffset = 0;
    loadFocusedHistory();
  });
  byId("history-page-previous").addEventListener("click", () => {
    historyOffset = Math.max(0, historyOffset - HISTORY_PAGE_SIZE);
    loadFocusedHistory();
  });
  byId("history-page-next").addEventListener("click", () => {
    historyOffset += HISTORY_PAGE_SIZE;
    loadFocusedHistory();
  });
  byId("history-entity-filter").addEventListener("submit", (event) => {
    event.preventDefault();
    historyEntitiesOffset = 0;
    void loadHistoryEntities();
  });
  byId("history-entities-previous").addEventListener("click", () => {
    historyEntitiesOffset = Math.max(0, historyEntitiesOffset - HISTORY_ENTITY_PAGE_SIZE);
    void loadHistoryEntities();
  });
  byId("history-entities-next").addEventListener("click", () => {
    historyEntitiesOffset += HISTORY_ENTITY_PAGE_SIZE;
    void loadHistoryEntities();
  });
  byId("history-entities-list").addEventListener("click", (event) => {
    const button = event.target.closest("[data-history-entity]");
    if (!button) return;
    historyEntityDetailOffset = 0;
    void loadHistoryEntityDetail(button.dataset.historyEntity);
  });
  ["history-detail-country", "history-detail-chart", "history-detail-from", "history-detail-to", "history-detail-metric"].forEach((id) => {
    byId(id).addEventListener("change", () => {
      historyEntityDetailOffset = 0;
      void loadHistoryEntityDetail();
    });
  });
  byId("collection-runs-previous").addEventListener("click", () => {
    collectionRunsOffset = Math.max(0, collectionRunsOffset - 8);
    void loadHistoryManagement();
  });
  byId("collection-runs-next").addEventListener("click", () => {
    collectionRunsOffset += 8;
    void loadHistoryManagement();
  });
  byId("collection-jobs-filter").addEventListener("submit", (event) => {
    event.preventDefault();
    collectionJobsOffset = 0;
    void loadCollectionJobs().catch((error) => setStatus("selected-history-status", error.message, true));
  });
  byId("collection-jobs-previous").addEventListener("click", () => {
    collectionJobsOffset = Math.max(0, collectionJobsOffset - 10);
    void loadCollectionJobs().catch((error) => setStatus("selected-history-status", error.message, true));
  });
  byId("collection-jobs-next").addEventListener("click", () => {
    collectionJobsOffset += 10;
    void loadCollectionJobs().catch((error) => setStatus("selected-history-status", error.message, true));
  });
  byId("collection-jobs-list").addEventListener("click", async (event) => {
    const button = event.target.closest("[data-retry-chart-job]");
    if (!button) return;
    button.disabled = true;
    try {
      await requestJson("/api/collection/jobs/retry", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chart_type: button.dataset.chartType,
          country_code: button.dataset.country,
          requested_date: button.dataset.requestedDate,
        }),
      });
      setStatus("selected-history-status", "Chart date queued for an explicit retry. It will be processed by a collection run.");
      await loadCollectionJobs();
      await loadHistoryManagement();
    } catch (error) {
      button.disabled = false;
      setStatus("selected-history-status", error.message, true);
    }
  });
  byId("history-entity-detail-previous").addEventListener("click", () => {
    historyEntityDetailOffset = Math.max(0, historyEntityDetailOffset - HISTORY_ENTITY_DETAIL_PAGE_SIZE);
    void loadHistoryEntityDetail();
  });
  byId("history-entity-detail-next").addEventListener("click", () => {
    historyEntityDetailOffset += HISTORY_ENTITY_DETAIL_PAGE_SIZE;
    void loadHistoryEntityDetail();
  });
  byId("tracking-config-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const entity = byId("tracking-entity").selectedOptions[0];
    if (!entity?.value) return;
    try {
      await requestJson("/api/tracking/configs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          entity_uri: entity.value,
          chart_type: byId("tracking-chart").value,
          country_code: byId("tracking-country").value,
          enabled: true,
          backfill_start_date: byId("tracking-start-date").value || null,
        }),
      });
      setStatus("selected-history-status", "Tracking configuration saved.");
      await loadHistoryManagement();
      await loadFocusedHistory();
    } catch (error) {
      setStatus("selected-history-status", error.message, true);
    }
  });
  byId("tracking-config-list").addEventListener("click", async (event) => {
    const button = event.target.closest("[data-toggle-config]");
    if (!button) return;
    try {
      await requestJson("/api/tracking/configs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          entity_uri: button.dataset.toggleConfig,
          chart_type: button.dataset.configChart,
          country_code: button.dataset.configCountry,
          enabled: button.dataset.configEnabled !== "true",
        }),
      });
      await loadHistoryManagement();
      await loadFocusedHistory();
    } catch (error) {
      setStatus("selected-history-status", error.message, true);
    }
  });
  byId("collection-schedule-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const schedule = await requestJson("/api/collection/schedule", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          enabled: byId("collection-enabled").checked,
          local_time: byId("collection-time").value,
          timezone: byId("collection-timezone").value.trim(),
        }),
      });
      byId("collection-next-run").textContent = schedule.next_run_at ? new Date(schedule.next_run_at).toLocaleString() : "Schedule disabled";
      setStatus("selected-history-status", "Collection schedule saved. Automatic triggering must be enabled in the deployment runtime.");
    } catch (error) {
      setStatus("selected-history-status", error.message, true);
    }
  });
  byId("history-metric").addEventListener("change", () => {
    if (historyTrendData) renderHistoryTrend(historyTrendData);
  });
  byId("history-results").addEventListener("click", (event) => {
    const button = event.target.closest("[data-history-uri]");
    if (button) viewHistoryTrend(button.dataset.historyUri);
  });
  byId("live-connect").addEventListener("click", () => {
    window.open("/auth/login", "_blank", "noopener,noreferrer");
    setStatus("live-status", "Spotify sign-in opened in a new tab. Return here after authorization; charts will refresh automatically.");
  });
  byId("history-connect").addEventListener("click", () => {
    window.open("/auth/login", "_blank", "noopener,noreferrer");
    setStatus("selected-history-status", "Spotify sign-in opened in a new tab. Return here after authorization; charts will refresh automatically.");
  });

  authChannel?.addEventListener("message", (event) => {
    if (event.data === "connected") {
      if (activePage === "live") loadFocusedLive(true);
      if (activePage === "history") loadFocusedHistory(true);
    }
  });
  window.addEventListener("hashchange", () => activatePage(location.hash.slice(1)));
  updateFocusControls();
  activatePage(location.hash.slice(1) || "charts");
})();
