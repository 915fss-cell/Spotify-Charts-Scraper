(() => {
  const app = window.Chartroom;
  if (!app) return;

  const { byId, requestJson, escapeHtml, spotifyLink } = app;
  const state = { cadence: "daily", entityType: "track", chartType: "daily_top_songs", view: "release", marketScope: "global" };
  let activePage = "charts";
  let liveOverview = null;
  let liveKey = "";
  let liveLoadedAt = 0;
  let historyOverview = null;
  let historyChart = null;
  let historyTrendData = null;
  let liveRequest = 0;
  let historyRequest = 0;
  let historyOffset = 0;
  let historyEntitiesOffset = 0;
  let historyEntityDetailOffset = 0;
  let historyEntityTrigger = null;
  let collectionRunsOffset = 0;
  let collectionJobsOffset = 0;
  const historyCalendarContexts = {
    all: { prefix: "history", year: null, month: null, selectedDate: "", chartType: null, request: 0, resultsRequest: 0, resultsOffset: 0, daysByDate: new Map() },
    focused: { prefix: "focus-history", year: null, month: null, selectedDate: "", chartType: null, request: 0, resultsRequest: 0, resultsOffset: 0, daysByDate: new Map() },
  };
  let backfillRunnerActive = false;
  let backfillRunnerTimer = null;
  let backfillRunnerWaiter = null;
  let selectedHistoryEntityUri = "";
  let trackedEntityChart = null;
  const HISTORY_PAGE_SIZE = 50;
  const HISTORY_ENTITY_PAGE_SIZE = 20;
  const HISTORY_ENTITY_DETAIL_PAGE_SIZE = 120;
  const authChannel = "BroadcastChannel" in window ? new BroadcastChannel("spotify-charts-auth") : null;
  function focusedSelectionName(entityType = state.entityType) {
    if (state.chartType === "weekly_top_albums") return "Albums by selected artists";
    if (entityType === "artist") return "Selected artists";
    return "Selected songs and artists";
  }

  function setStatus(id, message, isError = false) {
    const element = byId(id);
    if (!element) return;
    element.textContent = message;
    element.classList.toggle("error", isError);
  }

  function currentKey() {
    return `${state.chartType}:${state.marketScope}`;
  }

  function updateLiveActionLabel() {
    const savedForToday = state.marketScope === "all"
      && liveOverview?.marketScope === "all"
      && liveOverview.cadence === state.cadence
      && liveOverview.entityType === state.entityType
      && liveOverview.chartType === state.chartType
      && (liveOverview.snapshotSaved || liveOverview.snapshotReused);
    byId("live-refresh").textContent = savedForToday
      ? "Today’s regions saved"
      : state.marketScope === "global" ? "Load all regions" : "Refresh all regions";
    byId("live-refresh").disabled = Boolean(savedForToday);
    const retryButton = byId("live-retry-failed");
    retryButton.hidden = !(state.marketScope === "all" && liveOverview?.failedRegions?.length);
  }

  function chartTypeName() {
    const names = {
      daily_top_songs: "Daily top songs", daily_viral_songs: "Daily viral songs",
      weekly_top_songs: "Weekly top songs", daily_top_artists: "Daily top artists",
      weekly_top_artists: "Weekly top artists", weekly_top_albums: "Weekly top albums",
    };
    return names[state.chartType] || "Spotify chart";
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

  function chartDetails(item, cadence = state.cadence) {
    const unit = cadence === "weekly" ? "w" : "d";
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

  function releaseRegionRow(item, showTrend = false, cadence = state.cadence) {
    return `<div class="focus-data-row"><div class="focus-row-rank">${escapeHtml(item.chartRank ?? "—")}${movementMarkup(item)}</div><div class="focus-row-copy"><strong class="focus-row-title">${escapeHtml(item.regionName)}</strong><span class="focus-row-subtitle">${escapeHtml(chartDetails(item, cadence))}</span></div><div class="focus-row-metric"><strong>${escapeHtml(formatNumber(item.metricValue ?? item.streamCount))}</strong><span>${escapeHtml((item.metricType || (item.streamCount == null ? "rank" : "streams")).toLowerCase())}</span>${showTrend ? trendButton(item.entityUri, item.entityName) : ""}</div></div>`;
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

  function renderReleaseCard(release, isHistory = false, cadence = state.cadence) {
    const url = spotifyLink(release.entityUri);
    const title = escapeHtml(release.entityName || "Untitled");
    const titleMarkup = url
      ? `<a class="focus-release-title" href="${escapeHtml(url)}" target="_blank" rel="noreferrer">${title}</a>`
      : `<strong class="focus-release-title">${title}</strong>`;
    const artistNames = release.artistNames || (state.entityType === "artist" ? "Selected artist" : entityTypeName());
    const regions = release.regions || [];
    return `<article class="focus-release-card"><header class="focus-release-heading">${coverMarkup(release.imageUrl, release.entityName)}<div class="focus-release-copy">${titleMarkup}<p class="focus-release-subtitle">${escapeHtml(artistNames)}</p></div><span class="focus-region-count">${regions.length} ${regions.length === 1 ? "region" : "regions"}</span></header><div class="focus-data-list">${regions.map((region) => releaseRegionRow(region, isHistory, cadence)).join("")}</div></article>`;
  }

  function renderRegionCard(region, isHistory = false) {
    const entries = region.entries || [];
    const subtitle = isHistory ? `${entries.length} saved releases` : chartTypeName();
    return `<article class="focus-region-card"><header class="focus-card-title"><strong>${escapeHtml(region.regionName)}</strong><span>${escapeHtml(subtitle)}</span></header><div class="focus-data-list">${entries.map((entry) => regionEntryRow(entry)).join("")}</div></article>`;
  }

  function renderResultCards(containerId, releases, regions, isHistory = false) {
    const container = byId(containerId);
    const items = state.view === "release" ? releases : regions;
    if (!items.length) {
      const emptyMessage = state.entityType === "album"
        ? "No charting albums associated with your saved artists were found."
        : state.entityType === "artist"
          ? `No selected artists appeared in ${isHistory ? "the saved history range" : "the current charts"}.`
          : `No selected songs or songs by your selected artists appeared in ${isHistory ? "the saved history range" : "the current charts"}.`;
      container.innerHTML = `<p class="focus-empty">${escapeHtml(emptyMessage)}${isHistory ? " More history appears as regional chart snapshots are collected." : ""}</p>`;
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
    const chartDates = [
      ...(payload.regions || []).map((region) => region.chartDate),
      ...(payload.releases || []).flatMap((release) => (release.regions || []).map((region) => region.chartDate)),
    ].filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(String(date || ""))).sort();
    const chartDate = chartDates[chartDates.length - 1];
    const chartDateElement = byId("live-chart-date");
    chartDateElement.hidden = !chartDate;
    chartDateElement.textContent = chartDate ? `Chart date: ${chartDate}` : "";
    if (chartDate) chartDateElement.dateTime = chartDate;
    const count = state.view === "release" ? payload.releases.length : payload.regions.length;
    const matchingRegionLabel = payload.matchingRegions === 1 ? "region" : "regions";
    byId("live-results-count").textContent = state.view === "release"
      ? `${count} ${count === 1 ? "result" : "results"} · ${payload.matchingRegions} ${matchingRegionLabel}`
      : `${count} regions · ${payload.marketsScanned} markets scanned`;
    renderResultCards("live-results", payload.releases, payload.regions);
    const failures = payload.failedRegions?.length || 0;
    const marketDiagnostics = payload.marketDiagnostics || [];
    if (marketDiagnostics.length) {
      const failedMarketDetails = marketDiagnostics.filter((item) => item.status === "failed").length;
      const succeededMarketDetails = marketDiagnostics.filter((item) => item.status === "succeeded").length;
      const diagnostics = document.createElement("details");
      diagnostics.className = "focus-region-card market-diagnostics";
      diagnostics.innerHTML = `<summary class="focus-card-title"><strong>Market diagnostics</strong><span>${succeededMarketDetails} succeeded · ${failedMarketDetails} failed</span></summary><div class="focus-data-list">${marketDiagnostics.map((item) => `<div class="focus-data-row"><div class="focus-row-copy"><strong class="focus-row-title">${escapeHtml(item.status)} · ${escapeHtml(item.countryName)} (${escapeHtml(item.countryCode)})</strong><span class="focus-row-subtitle">${escapeHtml(item.chartType)} · ${item.httpStatus ? `HTTP ${item.httpStatus}` : "No HTTP response"} · ${Number(item.attemptCount || 0)} attempt${Number(item.attemptCount || 0) === 1 ? "" : "s"}${item.timeout ? " · timeout" : ""}${item.rateLimited ? " · rate limited" : ""}${item.retryDelaySeconds != null ? ` · retry delay ${Number(item.retryDelaySeconds).toFixed(1)}s` : item.retryAfterSeconds != null ? ` · retry after ${Number(item.retryAfterSeconds).toFixed(1)}s` : ""}</span>${item.error || item.errorResponse ? `<span class="focus-row-subtitle">${escapeHtml(item.error || "Request failed")}${item.errorResponse ? ` · ${escapeHtml(item.errorResponse)}` : ""}</span>` : ""}</div></div>`).join("")}</div>`;
      byId("live-results").append(diagnostics);
    }
    const marketMessage = payload.marketScope === "global"
      ? "Global chart loaded. Scan all regions to find more placements."
      : payload.snapshotReused
        ? `Today’s saved results loaded from History (${payload.marketsScanned} of ${payload.marketsTotal} markets).`
        : `${payload.marketsScanned} of ${payload.marketsTotal} markets scanned.`;
    const regionLabel = payload.matchingRegions === 1 ? "region" : "regions";
    const message = `${payload.artist.entityName}: ${payload.matchingRegions} ${regionLabel} charting ${entityTypeName()} in ${chartTypeName()}. ${marketMessage}${payload.metadataLimitation ? ` ${payload.metadataLimitation}` : ""}`;
    const storageMessage = payload.historyStorageAvailable == null
      ? ""
      : payload.historyStorageAvailable
        ? `${payload.historySavedRegions} regional snapshots saved for History.`
        : "Live charts loaded; history storage is unavailable right now.";
    const partialMessage = failures ? ` ${failures} regions could not be loaded.` : "";
    const recoveryWarning = payload.scanError ? " Results could not be saved for recovery." : "";
    setStatus("live-status", `${message}${storageMessage ? ` ${storageMessage}` : ""}${partialMessage}${recoveryWarning}`, failures > 0 && payload.marketsScanned === 0);
    updateLiveActionLabel();
  }


  async function fetchAllRegionsOverview(cadence, entityType, chartType, refresh = false, retryFailed = false) {
    const params = new URLSearchParams({ cadence, entity_type: entityType, chart_type: chartType, scope: "all" });
    if (retryFailed) params.set("retry_failed", "true");
    let refreshScan = refresh && !retryFailed;
    while (true) {
      if (refreshScan) params.set("refresh", "true");
      const payload = await requestJson(`/api/focused/live?${params}`);
      params.delete("refresh");
      refreshScan = false;
      if (payload.scanError) {
        if (payload.marketDiagnostics?.length) return payload;
        throw new Error(payload.scanError);
      }
      if (!payload.scanInProgress) return payload;
      if (state.cadence !== cadence || state.entityType !== entityType || state.chartType !== chartType) throw new Error("The chart selection changed while regions were loading.");
      setStatus("live-status", "Global chart is loaded. The regional scan is running in the background and will update automatically.");
      await new Promise((resolve) => window.setTimeout(resolve, 3000));
    }
  }

  async function fetchLiveOverview(refresh = false) {
    const params = new URLSearchParams({
      cadence: state.cadence,
      entity_type: state.entityType,
      chart_type: state.chartType,
      scope: state.marketScope,
    });
    if (refresh) params.set("refresh", "true");
    return requestJson(`/api/focused/live?${params}`);
  }

  async function loadFocusedLive(refresh = false, retryFailed = false) {
    const requestId = ++liveRequest;
    const showingGlobalWhileScanning = state.marketScope === "all"
      && liveOverview?.marketScope === "global"
      && liveOverview.chartType === state.chartType
      && liveOverview.cadence === state.cadence
      && liveOverview.entityType === state.entityType;
    const preservingLoadedRegions = retryFailed
      && liveOverview?.marketScope === "all"
      && liveOverview.chartType === state.chartType
      && liveOverview.cadence === state.cadence
      && liveOverview.entityType === state.entityType;
    byId("live-refresh").disabled = true;
    byId("live-refresh").textContent = state.marketScope === "global" ? "Loading Global…" : "Loading regions…";
    if (!showingGlobalWhileScanning && !preservingLoadedRegions) {
      byId("live-results-count").textContent = state.marketScope === "global" ? "Loading Global chart…" : "Scanning all Spotify regions…";
      byId("live-results").innerHTML = '<p class="focus-empty">Loading chart…</p>';
    }
    setStatus("live-status", state.marketScope === "global"
      ? `Loading ${focusedSelectionName()} ${entityTypeName()} chart for Global…`
      : preservingLoadedRegions
        ? `Retrying ${liveOverview.failedRegions.length} failed regions. Previously loaded results remain visible.`
        : showingGlobalWhileScanning
          ? "Global results remain visible while all regions are scanned…"
          : `Loading ${focusedSelectionName()} ${entityTypeName()} charts across all regions…`);
    try {
      const payload = state.marketScope === "all"
        ? await fetchAllRegionsOverview(state.cadence, state.entityType, state.chartType, refresh, retryFailed)
        : await fetchLiveOverview(refresh);
      if (requestId === liveRequest && activePage === "live") {
        liveOverview = payload;
        liveKey = currentKey();
        liveLoadedAt = Date.now();
        renderLive(payload);
      }
    } catch (error) {
      if (requestId === liveRequest) {
        if (state.marketScope === "all" && liveOverview?.marketScope === "global") {
          state.marketScope = "global";
          renderLive(liveOverview);
          setStatus("live-status", `The full regional scan failed: ${error.message} The Global chart is still shown.`, true);
        } else {
          const globalChartUnavailable = error.marketDiagnostics?.some(
            (item) => item.countryCode === "GLOBAL" && item.httpStatus === 404,
          );
          const message = globalChartUnavailable
            ? `${chartTypeName()} is not available as a Global chart. Choose “Load all regions” to check country charts.`
            : error.message;
          byId("live-results-count").textContent = "Charts unavailable";
          byId("live-results").innerHTML = `<p class="focus-empty">${escapeHtml(message)}</p>`;
          setStatus("live-status", message, true);
        }
      }
    } finally {
      if (requestId === liveRequest) {
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
    payload.releases.forEach((release) => { release.imageUrl = release.imageUrl || covers.get(release.entityUri) || null; });
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

  function chartTypeLabel(chartType) {
    return {
      daily_top_songs: "Daily top songs",
      daily_viral_songs: "Daily viral songs",
      weekly_top_songs: "Weekly top songs",
      daily_top_artists: "Daily top artists",
      weekly_top_artists: "Weekly top artists",
      weekly_top_albums: "Weekly top albums",
    }[chartType] || chartType.replaceAll("_", " ");
  }

  function renderHistoryCalendar(context, payload) {
    context.year = payload.year;
    context.month = payload.month;
    const prefix = context.prefix;
    const monthLabel = new Date(payload.year, payload.month - 1, 1).toLocaleDateString(undefined, { month: "long" });
    byId(`${prefix}-calendar-month`).textContent = monthLabel;
    const yearPicker = byId(`${prefix}-calendar-year`);
    const availableYears = [...new Set((payload.available_years || [payload.year]).map(Number).filter(Number.isInteger))];
    if (!availableYears.includes(payload.year)) availableYears.push(payload.year);
    const firstYear = Math.min(2017, ...availableYears);
    const lastYear = Math.max(new Date().getFullYear(), ...availableYears);
    const selectableYears = Array.from({ length: lastYear - firstYear + 1 }, (_, index) => lastYear - index);
    yearPicker.innerHTML = selectableYears.map((year) => `<option value="${year}">${year}</option>`).join("");
    yearPicker.value = String(payload.year);
    yearPicker.disabled = false;
    byId(`${prefix}-calendar-grid`).setAttribute("aria-label", `Saved chart data calendar for ${monthLabel} ${payload.year}`);
    context.daysByDate = new Map(payload.days.map((day) => [day.chart_date, day]));
    const firstWeekday = (new Date(payload.year, payload.month - 1, 1).getDay() + 6) % 7;
    const daysInMonth = new Date(payload.year, payload.month, 0).getDate();
    const weekdayLabels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
      .map((label) => `<span class="history-calendar-weekday" aria-hidden="true">${label}</span>`).join("");
    const emptyCells = Array.from({ length: firstWeekday }, () => '<span class="history-calendar-empty-day" aria-hidden="true"></span>').join("");
    const dateCells = Array.from({ length: daysInMonth }, (_, index) => {
      const dayNumber = index + 1;
      const date = `${payload.year}-${String(payload.month).padStart(2, "0")}-${String(dayNumber).padStart(2, "0")}`;
      const saved = context.daysByDate.get(date);
      if (!saved) {
        return `<button class="history-calendar-day" type="button" disabled aria-label="${date}, no saved chart data"><span class="history-calendar-day-number">${dayNumber}</span><span class="history-calendar-day-count">No data</span></button>`;
      }
      const count = Number(saved.appearance_count || 0);
      const level = count <= 50 ? 1 : count <= 500 ? 2 : 3;
      const labelDate = new Date(payload.year, payload.month - 1, dayNumber).toLocaleDateString(undefined, { month: "long", day: "numeric", year: "numeric" });
      const chartCount = Number(saved.chart_count || 0);
      const marketCount = Number(saved.market_count || 0);
      return `<button class="history-calendar-day has-data" type="button" data-history-calendar-date="${date}" data-level="${level}" aria-pressed="${context.selectedDate === date}" aria-label="${labelDate}, ${count.toLocaleString()} saved appearances, ${chartCount} chart types, ${marketCount} markets"><span class="history-calendar-day-number">${dayNumber}</span><span class="history-calendar-day-count">${count.toLocaleString()} saved</span></button>`;
    }).join("");
    byId(`${prefix}-calendar-grid`).innerHTML = weekdayLabels + emptyCells + dateCells;
    byId(`${prefix}-calendar-previous`).disabled = false;
    byId(`${prefix}-calendar-next`).disabled = false;
  }

  async function loadHistoryCalendar(context, year = context.year, month = context.month, chartType = context.chartType) {
    const requestId = ++context.request;
    context.chartType = chartType;
    const params = new URLSearchParams();
    if (year && month) {
      params.set("year", String(year));
      params.set("month", String(month));
    }
    if (chartType) params.set("chart_type", chartType);
    try {
      const payload = await requestJson(`/api/history/calendar${params.size ? `?${params}` : ""}`);
      if (requestId !== context.request) return;
      renderHistoryCalendar(context, payload);
    } catch (error) {
      if (requestId !== context.request) return;
      byId(`${context.prefix}-calendar-month`).textContent = "Calendar unavailable";
      byId(`${context.prefix}-calendar-grid`).innerHTML = `<p class="history-empty-row">${escapeHtml(error.message)}</p>`;
    }
  }

  function snapshotCards(items, chartType) {
    const releases = new Map();
    for (const item of items) {
      let release = releases.get(item.track_uri);
      if (!release) {
        release = {
          entityUri: item.track_uri,
          entityName: item.track_name || item.track_uri,
          entityType: item.entity_type,
          artistNames: item.artist_names || (item.entity_type === "artist" ? "Artist" : ""),
          imageUrl: item.imageUrl || null,
          regions: [],
        };
        releases.set(item.track_uri, release);
      } else if (!release.imageUrl && item.imageUrl) {
        release.imageUrl = item.imageUrl;
      }
      release.regions.push({
        entityUri: item.track_uri,
        entityType: item.entity_type,
        entityName: item.track_name || item.track_uri,
        artistNames: item.artist_names,
        regionCode: item.country_code,
        regionName: item.country_name || item.country_code,
        chartDate: item.chart_date,
        chartRank: item.chart_rank,
        previousRank: item.previous_rank,
        peakRank: item.peak_rank,
        consecutiveAppearancesOnChart: item.consecutive_appearances,
        appearancesOnChart: item.appearances,
        entryDate: item.entry_date,
        streamCount: item.stream_count,
        metricValue: item.stream_count,
        metricType: item.stream_count == null ? "rank" : "streams",
      });
    }
    const cadence = chartType.startsWith("weekly_") ? "weekly" : "daily";
    return [...releases.values()].map((release) => renderReleaseCard(release, false, cadence)).join("")
      || '<p class="history-calendar-empty">No saved chart results were found for this date.</p>';
  }

  async function loadHistoryCalendarDateResults(context, chartType = context.resultsChartType || context.chartType) {
    if (!context.selectedDate || !chartType) return;
    context.resultsChartType = chartType;
    const requestId = ++context.resultsRequest;
    const { prefix, selectedDate: date } = context;
    const isFocused = context === historyCalendarContexts.focused;
    const pageSize = isFocused ? 200 : 50;
    context.resultsPageSize = pageSize;
    const params = isFocused
      ? new URLSearchParams({
        cadence: state.cadence,
        entity_type: state.entityType,
        chart_type: chartType,
        chart_date: date,
        range: "all",
        limit: String(pageSize),
        offset: String(context.resultsOffset),
      })
      : new URLSearchParams({
        chart_date: date,
        chart_type: chartType,
        limit: String(pageSize),
        offset: String(context.resultsOffset),
      });
    const resultsPanel = byId(`${prefix}-calendar-results`);
    resultsPanel.hidden = false;
    byId(`${prefix}-calendar-results-title`).textContent = new Date(`${date}T00:00:00`).toLocaleDateString(undefined, { weekday: "long", month: "long", day: "numeric", year: "numeric" });
    byId(`${prefix}-calendar-results-count`).textContent = "Loading saved results…";
    byId(`${prefix}-calendar-results-list`).innerHTML = '<p class="history-calendar-empty">Loading saved chart results…</p>';
    try {
      const path = isFocused ? "/api/focused/history" : "/api/history/date";
      const payload = await requestJson(`${path}?${params}`);
      if (requestId !== context.resultsRequest || date !== context.selectedDate) return;
      let total;
      let itemCount;
      if (isFocused) {
        const covers = new Map((liveOverview?.releases || []).map((release) => [release.entityUri, release.imageUrl]));
        payload.releases.forEach((release) => { release.imageUrl = release.imageUrl || covers.get(release.entityUri) || null; });
        renderResultCards(`${prefix}-calendar-results-list`, payload.releases, payload.regions, true);
        total = Number(payload.totalRows || 0);
        itemCount = Number(payload.snapshotRows || 0);
      } else {
        byId(`${prefix}-calendar-results-list`).innerHTML = snapshotCards(payload.items, chartType);
        total = Number(payload.total || 0);
        itemCount = payload.items.length;
      }
      byId(`${prefix}-calendar-results-count`).textContent = `${total.toLocaleString()} saved ${chartTypeLabel(chartType)} entries`;
      const pagination = byId(`${prefix}-calendar-results-pagination`);
      pagination.hidden = total <= pageSize;
      byId(`${prefix}-calendar-results-page-label`).textContent = total
        ? `${context.resultsOffset + 1}–${context.resultsOffset + itemCount} of ${total.toLocaleString()}`
        : "No results";
      byId(`${prefix}-calendar-results-previous`).disabled = context.resultsOffset === 0;
      byId(`${prefix}-calendar-results-next`).disabled = !payload.hasMore;
    } catch (error) {
      if (requestId !== context.resultsRequest || date !== context.selectedDate) return;
      byId(`${prefix}-calendar-results-count`).textContent = "Saved results unavailable";
      byId(`${prefix}-calendar-results-list`).innerHTML = `<p class="history-calendar-empty">${escapeHtml(error.message)}</p>`;
    }
  }

  async function loadHistoryCalendarDate(context, date) {
    context.selectedDate = date;
    context.resultsOffset = 0;
    context.resultsChartType = null;
    byId(`${context.prefix}-calendar-grid`).querySelectorAll("[data-history-calendar-date]").forEach((button) => {
      button.setAttribute("aria-pressed", String(button.dataset.historyCalendarDate === date));
    });
    const resultsPanel = byId(`${context.prefix}-calendar-results`);
    resultsPanel.hidden = false;
    byId(`${context.prefix}-calendar-results-title`).textContent = new Date(`${date}T00:00:00`).toLocaleDateString(undefined, { weekday: "long", month: "long", day: "numeric", year: "numeric" });
    if (context === historyCalendarContexts.all) {
      const saved = context.daysByDate.get(date);
      const picker = byId("history-calendar-chart");
      picker.innerHTML = '<option value="">Choose a chart</option>' + (saved?.charts || []).map((chart) =>
        `<option value="${escapeHtml(chart.chart_type)}">${escapeHtml(chartTypeLabel(chart.chart_type))} · ${Number(chart.appearance_count).toLocaleString()} entries</option>`
      ).join("");
      picker.disabled = !saved?.charts?.length;
      picker.value = "";
      byId("history-calendar-results-count").textContent = saved?.charts?.length ? "Choose a chart to see its saved results." : "No saved results for this date.";
      byId("history-calendar-results-list").innerHTML = '<p class="history-calendar-empty">Choose a chart above to view the entries saved that day.</p>';
      byId("history-calendar-results-pagination").hidden = true;
      return;
    }
    await loadHistoryCalendarDateResults(context, context.chartType);
  }

  async function loadHistoryManagement() {
    try {
      const [overview, configs, selections, countries, runs, backfillQueue] = await Promise.all([
        requestJson("/api/history/overview"),
        requestJson("/api/tracking/configs"),
        requestJson("/api/selections"),
        requestJson("/api/countries"),
        requestJson(`/api/collection/runs?limit=8&offset=${collectionRunsOffset}`),
        requestJson("/api/chart-history/backfill/queue"),
      ]);
      setStatus("collection-status", "Collection settings and activity are up to date.");
      if (!backfillRunnerActive) {
        const remaining = Number(backfillQueue.remainingDates || 0).toLocaleString();
        setStatus("backfill-runner-status", `${remaining} historical chart dates remaining across ${Number(backfillQueue.activeJobs || 0).toLocaleString()} active jobs. Progress is saved after each date.`);
      }
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
      byId("tracking-country").innerHTML = '<option value="ALL">All supported regions</option>' + countries.map((country) => `<option value="${escapeHtml(country.code)}">${escapeHtml(country.name)}</option>`).join("");
      byId("collection-job-country").innerHTML = '<option value="">All markets</option>' + countries.map((country) => `<option value="${escapeHtml(country.code)}">${escapeHtml(country.name)}</option>`).join("");
      byId("history-detail-country").innerHTML = '<option value="">All regions</option>' + countries.map((country) => `<option value="${escapeHtml(country.code)}">${escapeHtml(country.name)}</option>`).join("");
      byId("history-detail-chart").innerHTML = '<option value="">Choose a chart</option>' + [
        ["daily_top_songs", "Daily top songs"], ["daily_viral_songs", "Daily viral songs"], ["weekly_top_songs", "Weekly top songs"],
        ["daily_top_artists", "Daily top artists"], ["weekly_top_artists", "Weekly top artists"],
        ["weekly_top_albums", "Weekly top albums"],
      ].map(([value, label]) => `<option value="${value}">${label}</option>`).join("");
      const setChartOptions = () => {
        const selected = entitySelect.selectedOptions[0];
        const type = selected?.dataset.type;
        const options = type === "artist"
          ? [["daily_top_songs", "Daily top songs"], ["daily_viral_songs", "Daily viral songs"], ["weekly_top_songs", "Weekly top songs"], ["daily_top_artists", "Daily top artists"], ["weekly_top_artists", "Weekly top artists"], ["weekly_top_albums", "Weekly top albums"]]
          : [["daily_top_songs", "Daily top songs"], ["daily_viral_songs", "Daily viral songs"], ["weekly_top_songs", "Weekly top songs"]];
        byId("tracking-chart").innerHTML = options.map(([value, label]) => `<option value="${value}">${label}</option>`).join("");
      };
      entitySelect.onchange = setChartOptions;
      setChartOptions();
      byId("tracking-config-count").textContent = `${configs.length.toLocaleString()} configurations`;
      byId("tracking-config-list").innerHTML = configs.length ? configs.map((config) => `
        <div class="history-config-row">
          <div class="history-config-copy"><strong>${escapeHtml(config.entity_name)} · ${escapeHtml(config.chart_type.replaceAll("_", " "))}</strong><span>${escapeHtml(config.country_code)} · ${config.backfill_start_date ? `from ${escapeHtml(config.backfill_start_date)}` : "start date not set"} · ${config.enabled ? "Active" : "Paused"}</span></div>
          ${config.backfill_start_date ? `<button class="quiet-button" type="button" data-backfill-config="${escapeHtml(config.entity_uri)}" data-config-chart="${escapeHtml(config.chart_type)}" data-config-country="${escapeHtml(config.country_code)}" data-config-start="${escapeHtml(config.backfill_start_date)}">Backfill next batch</button>` : ""}
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
          <button class="quiet-button" type="button" data-history-entity="${escapeHtml(entity.entity_uri)}" aria-expanded="false" aria-controls="history-entity-detail">View detail</button>
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
    try {
      const countrySelect = byId("history-detail-country");
    if (countrySelect.options.length < 2) {
      const countries = await requestJson("/api/countries");
      countrySelect.innerHTML = '<option value="">All regions</option>' + countries.map((country) => `<option value="${escapeHtml(country.code)}">${escapeHtml(country.name)}</option>`).join("");
    }
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

      const payload = await requestJson(`/api/history/entity?${params}`);
      const entity = payload.entity;
      const summary = payload.summary;
      byId("history-entity-detail").hidden = false;
      byId("history-entity-detail-title").textContent = chartType
        ? `${entity.entity_name} · ${chartType.replaceAll("_", " ")}`
        : `${entity.entity_name} history`;
      byId("tracked-entity-trend").hidden = !chartType;
      if (!chartType) byId("tracked-entity-trend").open = false;
      byId("history-entity-summary").textContent = `${Number(summary.appearances).toLocaleString()} appearances · peak #${summary.peak_rank ?? "—"} · ${summary.first_appearance || "—"} to ${summary.latest_appearance || "—"}${entity.is_archived ? " · archived" : ""}`;
      byId("history-detail-country").disabled = !chartType;
      const items = [...payload.items].reverse();
      const labels = [...new Set(items.map((item) => item.chart_date))].sort();
      const series = [...new Set(items.map((item) => `${item.country_code} · ${item.chart_type.replaceAll("_", " ")}`))];
      const metric = byId("history-detail-metric").value;
      const valueFor = (item) => metric === "rank" ? item.chart_rank : item.stream_count;
      const hasValues = Boolean(chartType) && items.some((item) => valueFor(item) != null);
      const canvas = byId("tracked-entity-chart");
      const empty = byId("tracked-entity-chart-empty");
      const legend = byId("tracked-entity-legend");
      legend.replaceChildren();
      legend.hidden = true;
      if (trackedEntityChart) {
        trackedEntityChart.destroy();
        trackedEntityChart = null;
      }
      canvas.hidden = !hasValues;
      empty.hidden = hasValues;
      empty.textContent = !chartType
        ? "Choose a chart to see its trend."
        : items.length ? "No stored values are available for the selected metric." : "No saved chart history matches these filters.";
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
          options: { responsive: true, maintainAspectRatio: false, interaction: { intersect: false, mode: "index" }, plugins: { legend: { display: false } }, scales: { y: { reverse: metric === "rank", beginAtZero: metric !== "rank" }, x: { ticks: { maxTicksLimit: 8 } } } },
        });
        legend.innerHTML = datasets.map((dataset, index) => `<button type="button" data-chart-series="${index}" aria-pressed="true"><span class="tracked-entity-legend-swatch" style="--series-color:${dataset.borderColor}"></span><span class="tracked-entity-legend-label">${escapeHtml(dataset.label)}</span></button>`).join("");
        legend.hidden = datasets.length === 0;
      }
      const pointList = byId("history-entity-points");
      if (!chartType) {
        pointList.innerHTML = '<p class="history-empty-row">Choose a chart to view its saved appearances grouped by region.</p>';
      } else if (payload.items.length) {
        const pointsByRegion = new Map();
        payload.items.forEach((item) => {
          if (!pointsByRegion.has(item.country_code)) pointsByRegion.set(item.country_code, []);
          pointsByRegion.get(item.country_code).push(item);
        });
        const regionOrder = (left, right) => {
          if (left === "GLOBAL") return -1;
          if (right === "GLOBAL") return 1;
          return left.localeCompare(right);
        };
        pointList.innerHTML = `<div class="history-entity-points-list">${[...pointsByRegion.entries()].sort(([left], [right]) => regionOrder(left, right)).map(([region, points]) => {
          const regionName = [...byId("history-detail-country").options].find((option) => option.value === region)?.textContent || (region === "GLOBAL" ? "Global" : region);
          const sortedPoints = points.sort((left, right) => right.chart_date.localeCompare(left.chart_date));
          const latest = sortedPoints[0];
          const display = (value) => value == null ? "—" : Number(value).toLocaleString();
          const metric = (label, value) => `<div class="history-point-metric"><span>${label}</span><strong>${escapeHtml(String(value ?? "—"))}</strong></div>`;
          const regionSummary = `<div class="history-region-summary">${metric("Rank", `#${latest.chart_rank ?? "—"}`)}${metric("Movement", latest.previous_rank == null || latest.chart_rank == null ? "—" : latest.previous_rank > latest.chart_rank ? `↑${latest.previous_rank - latest.chart_rank}` : latest.previous_rank < latest.chart_rank ? `↓${latest.chart_rank - latest.previous_rank}` : "—")}${metric("Release date", entity.release_date || "—")}${metric("Peak", `#${latest.peak_rank ?? "—"}`)}${metric("Streak", `${display(latest.consecutive_appearances)} days`)}${metric("Total days on chart", display(latest.total_days))}${metric("First entry position", `#${latest.first_entry_position ?? "—"}`)}${metric("Streams", display(latest.daily_streams))}${metric("Total chart streams", display(latest.total_chart_streams))}</div>`;
          return `
          <details class="history-region-group" aria-label="${escapeHtml(regionName)} chart history">
            <summary class="history-region-heading"><span><strong>${escapeHtml(region)} · ${escapeHtml(regionName)}</strong></span><span>${points.length.toLocaleString()} ${points.length === 1 ? "appearance" : "appearances"} <span aria-hidden="true">▾</span></span></summary>
            ${regionSummary}
            <div class="history-region-entries">${sortedPoints.map((item) => {
              const movement = item.previous_rank == null || item.chart_rank == null ? "—" : item.previous_rank > item.chart_rank ? `↑${item.previous_rank - item.chart_rank}` : item.previous_rank < item.chart_rank ? `↓${item.chart_rank - item.previous_rank}` : "—";
              return `<article class="history-entity-point"><strong class="history-point-date">${escapeHtml(item.chart_date)}</strong>${metric("Rank", `#${item.chart_rank ?? "—"}`)}${metric("Movement", movement)}${metric("Release date", entity.release_date || "—")}${metric("Peak", `#${item.peak_rank ?? "—"}`)}${metric("Streak", `${display(item.consecutive_appearances)} days`)}${metric("Total days on chart", display(item.total_days))}${metric("First entry position", `#${item.first_entry_position ?? "—"}`)}${metric("Streams", display(item.daily_streams))}${metric("Total chart streams", display(item.total_chart_streams))}</article>`;
            }).join("")}</div>
          </details>`;
        }).join("")}</div>`;
      } else {
        pointList.innerHTML = '<p class="history-empty-row">No saved chart history matches these filters.</p>';
      }
      const pagination = byId("history-entity-detail-pagination");
      pagination.hidden = !chartType || payload.total <= HISTORY_ENTITY_DETAIL_PAGE_SIZE;
      byId("history-entity-detail-page-label").textContent = payload.total
        ? `${historyEntityDetailOffset + 1}–${historyEntityDetailOffset + payload.items.length} of ${Number(payload.total).toLocaleString()}`
        : "No appearances";
      byId("history-entity-detail-previous").disabled = historyEntityDetailOffset === 0;
      byId("history-entity-detail-next").disabled = !payload.hasMore;
      const detailPanel = byId("history-entity-detail");
      detailPanel.scrollIntoView({
        behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth",
        block: "start",
      });
      detailPanel.focus({ preventScroll: true });
    } catch (error) {
      const detailPanel = byId("history-entity-detail");
      detailPanel.hidden = false;
      byId("history-entity-points").innerHTML = `<p class="history-empty-row">${escapeHtml(error.message)}</p>`;
      detailPanel.scrollIntoView({
        behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth",
        block: "start",
      });
      detailPanel.focus({ preventScroll: true });
    }
  }

  async function loadFocusedHistory(refreshLive = false) {
    void loadHistoryEntities();
    void loadHistoryCalendar(historyCalendarContexts.all);
    const focusedCalendar = historyCalendarContexts.focused;
    if (focusedCalendar.chartType !== state.chartType || focusedCalendar.year == null) {
      focusedCalendar.selectedDate = "";
      focusedCalendar.resultsOffset = 0;
      byId("focus-history-calendar-results").hidden = true;
      void loadHistoryCalendar(focusedCalendar, null, null, state.chartType);
    }
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
        chart_type: state.chartType,
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
      chart_type: state.chartType,
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
      "live-daily-songs": { cadence: "daily", entityType: "track", title: "Daily top songs", type: "songs", chartType: "daily_top_songs" },
      "live-daily-viral-songs": { cadence: "daily", entityType: "track", title: "Daily viral songs", type: "songs", chartType: "daily_viral_songs" },
      "live-weekly-songs": { cadence: "weekly", entityType: "track", title: "Weekly top songs", type: "songs", chartType: "weekly_top_songs" },
      "live-daily-artists": { cadence: "daily", entityType: "artist", title: "Daily top artists", type: "artists", chartType: "daily_top_artists" },
      "live-weekly-artists": { cadence: "weekly", entityType: "artist", title: "Weekly top artists", type: "artists", chartType: "weekly_top_artists" },
      "live-weekly-albums": { cadence: "weekly", entityType: "album", title: "Weekly top albums", type: "albums", chartType: "weekly_top_albums" },
    };
    const historyRouteKey = page.startsWith("history-") ? page.replace(/^history-/, "live-") : "";
    const routeKey = page === "live" ? "live-daily-songs" : historyRouteKey || page;
    const route = liveRoutes[routeKey];
    const isChartHistory = Boolean(route && historyRouteKey);
    const validPage = route ? (isChartHistory ? "history" : "live") : ["charts", "history", "collection"].includes(page) ? page : "charts";
    if (route) {
      state.cadence = route.cadence;
      state.entityType = route.entityType;
      state.chartType = route.chartType;
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
      state.chartType = "daily_top_songs";
      state.view = "release";
      state.marketScope = "global";
      byId("history-title-label").textContent = "History";
      byId("history-eyebrow").textContent = "Tracked chart history";
      byId("history-description").textContent = "Review saved appearances and tracked entities across Spotify charts.";
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
    byId("history-calendar-card").hidden = isChartHistory;
    byId("focus-history-calendar").hidden = !isChartHistory;
    byId("history-chart-controls").hidden = !isChartHistory;
    byId("focused-history-results").hidden = !isChartHistory;
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
    if (validPage === "collection") void loadHistoryManagement();
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
        state.chartType = `${state.cadence}_top_${{ track: "songs", artist: "artists", album: "albums" }[state.entityType]}`;
        historyOffset = 0;
        loadFocusedHistory();
      }
    });
  });

    byId("live-refresh").addEventListener("click", () => {
      state.marketScope = "all";
      loadFocusedLive(true);
    });
    byId("live-retry-failed").addEventListener("click", () => {
      state.marketScope = "all";
      byId("live-retry-failed").disabled = true;
      byId("live-retry-failed").textContent = "Retrying failed regions…";
      loadFocusedLive(false, true).finally(() => {
        byId("live-retry-failed").disabled = false;
        byId("live-retry-failed").textContent = "Retry failed regions";
      });
    });
  byId("history-refresh").addEventListener("click", () => {
    loadFocusedHistory(true);
  });
  byId("collection-refresh").addEventListener("click", () => {
    void loadHistoryManagement();
  });
  byId("collection-run-now").addEventListener("click", async () => {
    const button = byId("collection-run-now");
    button.disabled = true;
    button.textContent = "Collecting…";
    try {
      const result = await requestJson("/api/collection/run", { method: "POST" });
    setStatus("collection-status", `Collection ${result.status}: ${Number(result.completedChartKeys || 0)} of ${Number(result.requestedChartKeys || 0)} chart keys processed.`);
    await loadHistoryManagement();
  } catch (error) {
    setStatus("collection-status", error.message, true);
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
  function closeHistoryEntityDetail(restoreFocus = true) {
    byId("history-entity-detail").hidden = true;
    if (trackedEntityChart) {
      trackedEntityChart.destroy();
      trackedEntityChart = null;
    }
    if (historyEntityTrigger?.isConnected) {
      historyEntityTrigger.textContent = "View detail";
      historyEntityTrigger.setAttribute("aria-expanded", "false");
    }
    historyEntityTrigger = historyEntityTrigger?.isConnected
      ? historyEntityTrigger
      : byId("history-entities-list").querySelector("[data-history-entity]");
    if (restoreFocus) historyEntityTrigger?.focus();
  }
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
    if (historyEntityTrigger === button && !byId("history-entity-detail").hidden) {
      closeHistoryEntityDetail();
      return;
    }
    if (historyEntityTrigger?.isConnected) {
      historyEntityTrigger.textContent = "View detail";
      historyEntityTrigger.setAttribute("aria-expanded", "false");
    }
    historyEntityTrigger = button;
    button.textContent = "Close details";
    button.setAttribute("aria-expanded", "true");
    byId("history-detail-chart").value = "";
    byId("history-detail-country").value = "";
    byId("history-detail-country").disabled = true;
    byId("tracked-entity-trend").open = false;
    byId("history-detail-from").value = "";
    byId("history-detail-to").value = "";
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
    void loadCollectionJobs().catch((error) => setStatus("collection-status", error.message, true));
  });
  byId("collection-jobs-previous").addEventListener("click", () => {
    collectionJobsOffset = Math.max(0, collectionJobsOffset - 10);
    void loadCollectionJobs().catch((error) => setStatus("collection-status", error.message, true));
  });
  byId("collection-jobs-next").addEventListener("click", () => {
    collectionJobsOffset += 10;
    void loadCollectionJobs().catch((error) => setStatus("collection-status", error.message, true));
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
      setStatus("collection-status", "Chart date queued for an explicit retry. It will be processed by a collection run.");
      await loadCollectionJobs();
      await loadHistoryManagement();
    } catch (error) {
      button.disabled = false;
      setStatus("collection-status", error.message, true);
    }
  });
  function moveHistoryCalendarMonth(context, offset) {
    if (context.year == null || context.month == null) return;
    const next = new Date(context.year, context.month - 1 + offset, 1);
    context.selectedDate = "";
    context.resultsOffset = 0;
    byId(`${context.prefix}-calendar-results`).hidden = true;
    void loadHistoryCalendar(context, next.getFullYear(), next.getMonth() + 1, context.chartType);
  }

  function wireHistoryCalendar(context) {
    const prefix = context.prefix;
    byId(`${prefix}-calendar-previous`).addEventListener("click", () => moveHistoryCalendarMonth(context, -1));
    byId(`${prefix}-calendar-next`).addEventListener("click", () => moveHistoryCalendarMonth(context, 1));
    byId(`${prefix}-calendar-year`).addEventListener("change", (event) => {
      const year = Number(event.target.value);
      if (!Number.isInteger(year) || context.month == null) return;
      context.selectedDate = "";
      context.resultsOffset = 0;
      byId(`${prefix}-calendar-results`).hidden = true;
      void loadHistoryCalendar(context, year, context.month, context.chartType);
    });
    byId(`${prefix}-calendar-grid`).addEventListener("click", (event) => {
      const button = event.target.closest("[data-history-calendar-date]");
      if (button) void loadHistoryCalendarDate(context, button.dataset.historyCalendarDate);
    });
    byId(`${prefix}-calendar-results-previous`).addEventListener("click", () => {
      context.resultsOffset = Math.max(0, context.resultsOffset - (context.resultsPageSize || 50));
      void loadHistoryCalendarDateResults(context);
    });
    byId(`${prefix}-calendar-results-next`).addEventListener("click", () => {
      context.resultsOffset += context.resultsPageSize || 50;
      void loadHistoryCalendarDateResults(context);
    });
  }

  wireHistoryCalendar(historyCalendarContexts.all);
  wireHistoryCalendar(historyCalendarContexts.focused);
  byId("history-calendar-chart").addEventListener("change", (event) => {
    const context = historyCalendarContexts.all;
    context.resultsOffset = 0;
    context.resultsChartType = event.target.value || null;
    if (context.resultsChartType) void loadHistoryCalendarDateResults(context, context.resultsChartType);
  });
  byId("history-entity-detail-close").addEventListener("click", () => {
    closeHistoryEntityDetail();
  });
  byId("tracked-entity-legend").addEventListener("click", (event) => {
    const button = event.target.closest("[data-chart-series]");
    if (!button || !trackedEntityChart) return;
    const index = Number(button.dataset.chartSeries);
    const visible = trackedEntityChart.isDatasetVisible(index);
    trackedEntityChart.setDatasetVisibility(index, !visible);
    button.setAttribute("aria-pressed", String(!visible));
    trackedEntityChart.update();
  });
  byId("history-entity-detail-previous").addEventListener("click", () => {
    historyEntityDetailOffset = Math.max(0, historyEntityDetailOffset - HISTORY_ENTITY_DETAIL_PAGE_SIZE);
    void loadHistoryEntityDetail();
  });
  byId("history-entity-detail-next").addEventListener("click", () => {
    historyEntityDetailOffset += HISTORY_ENTITY_DETAIL_PAGE_SIZE;
    void loadHistoryEntityDetail();
  });
  byId("tracked-entity-trend").addEventListener("toggle", (event) => {
    if (event.currentTarget.open) requestAnimationFrame(() => trackedEntityChart?.resize());
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
      setStatus("collection-status", "Tracking configuration saved.");
      await loadHistoryManagement();
    } catch (error) {
      setStatus("collection-status", error.message, true);
    }
  });
  byId("tracking-config-list").addEventListener("click", async (event) => {
    const backfillButton = event.target.closest("[data-backfill-config]");
    const toggleButton = event.target.closest("[data-toggle-config]");
    if (!backfillButton && !toggleButton) return;
    const button = backfillButton || toggleButton;
    button.disabled = true;
    try {
      if (backfillButton) {
        const chartParams = new URLSearchParams({
          type: button.dataset.configChart,
          country: button.dataset.configCountry,
          date: "latest",
        });
        const chart = await requestJson(`/api/charts?${chartParams}`);
        const targetDate = chart.displayChart?.date;
        if (!targetDate) throw new Error("Spotify did not return a chart date for this backfill.");
        const uri = button.dataset.backfillConfig;
        const progress = await requestJson("/api/chart-history/backfill", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            chart_type: button.dataset.configChart,
            country: button.dataset.configCountry,
            start_date: button.dataset.configStart,
            target_date: String(targetDate).slice(0, 10),
            entity_uris: [uri],
            target_entity_uris: [uri],
          }),
        });
        setStatus("collection-status", `Backfill ${progress.status}: ${Number(progress.processedDates || 0)} of ${Number(progress.totalDates || 0)} dates processed. Select “Backfill next batch” again to resume.`);
        await loadHistoryManagement();
      } else {
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
      }
    } catch (error) {
      setStatus("collection-status", error.message, true);
    } finally {
      button.disabled = false;
    }
  });
  async function waitForBackfillRetry(seconds) {
    let remaining = Math.max(1, Math.ceil(Number(seconds) || 30));
    return new Promise((resolve) => {
      const finish = () => {
        if (backfillRunnerTimer) window.clearInterval(backfillRunnerTimer);
        backfillRunnerTimer = null;
        backfillRunnerWaiter = null;
        resolve();
      };
      backfillRunnerWaiter = finish;
      setStatus("backfill-runner-status", `A date is waiting for retry. Resuming in ${remaining.toLocaleString()} seconds; saved progress is safe.`);
      backfillRunnerTimer = window.setInterval(() => {
        if (!backfillRunnerActive) return finish();
        remaining -= 1;
        if (remaining <= 0) return finish();
        setStatus("backfill-runner-status", `A date is waiting for retry. Resuming in ${remaining.toLocaleString()} seconds; saved progress is safe.`);
      }, 1000);
    });
  }

  byId("backfill-run-all").addEventListener("click", async () => {
    if (backfillRunnerActive) return;
    const runButton = byId("backfill-run-all");
    const pauseButton = byId("backfill-pause");
    let finalMessage = "Backfill paused. Saved progress can be resumed later.";
    try {
      const queue = await requestJson("/api/chart-history/backfill/queue");
      const remaining = Number(queue.remainingDates || 0);
      if (!remaining) {
        setStatus("backfill-runner-status", "There are no pending historical chart dates.");
        return;
      }
      if (!window.confirm(`Run the pending backfill for up to ${remaining.toLocaleString()} chart dates? Requests are paced and progress is saved after every date. You can pause and resume later.`)) return;
      backfillRunnerActive = true;
      runButton.disabled = true;
      pauseButton.hidden = false;
      finalMessage = "Backfill paused. Saved progress can be resumed later.";
      while (backfillRunnerActive) {
        const result = await requestJson("/api/chart-history/backfill/queue/step", { method: "POST" });
        const remainingDates = Number(result.queue?.remainingDates || 0);
        if (result.state === "complete") {
          finalMessage = "Historical backfill queue complete.";
          break;
        }
        if (result.state === "waiting") {
          await waitForBackfillRetry(result.retryAfterSeconds);
          continue;
        }
        const batch = result.batch || {};
        setStatus("backfill-runner-status", `${Number(batch.processedDates || 0).toLocaleString()} of ${Number(batch.totalDates || 0).toLocaleString()} dates processed for ${batch.chartType || "current chart"} / ${batch.country || "current market"}; ${remainingDates.toLocaleString()} remain across the queue.`);
        await new Promise((resolve) => window.setTimeout(resolve, 2000));
      }
    } catch (error) {
      finalMessage = `Backfill stopped: ${error.message}. Saved progress is safe to resume.`;
    } finally {
      backfillRunnerActive = false;
      if (backfillRunnerTimer) window.clearInterval(backfillRunnerTimer);
      backfillRunnerTimer = null;
      if (backfillRunnerWaiter) backfillRunnerWaiter();
      backfillRunnerWaiter = null;
      runButton.disabled = false;
      pauseButton.hidden = true;
      await loadHistoryManagement();
      setStatus("backfill-runner-status", finalMessage);
    }
  });
  byId("backfill-pause").addEventListener("click", () => {
    backfillRunnerActive = false;
    setStatus("backfill-runner-status", "Pause requested. The current bounded batch will finish before the runner stops.");
    if (backfillRunnerWaiter) backfillRunnerWaiter();
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
