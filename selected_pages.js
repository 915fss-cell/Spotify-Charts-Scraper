(() => {
  const app = window.Chartroom;
  if (!app) return;

  const { byId, requestJson, escapeHtml } = app;
  const historyTypes = {
    daily_top_songs: "track",
    weekly_top_songs: "track",
    daily_top_artists: "artist",
    weekly_top_artists: "artist",
  };
  let selections = [];
  let searchResults = [];
  let liveData = { songs: [], artists: [] };
  let historyChart = null;
  let historyData = null;
  let historyTrendData = null;
  let liveRequest = 0;
  let activePage = "charts";
  let connectionRefreshInFlight = false;
  const authChannel = "BroadcastChannel" in window ? new BroadcastChannel("spotify-charts-auth") : null;

  function setStatus(id, message, isError = false) {
    const element = byId(id);
    if (!element) return;
    element.textContent = message;
    element.classList.toggle("error", isError);
  }

  function spotifyUrl(uri) {
    const match = /^spotify:(track|artist):([A-Za-z0-9]+)$/.exec(uri || "");
    return match ? `https://open.spotify.com/${match[1]}/${match[2]}` : null;
  }

  function artwork(imageUrl, title) {
    return imageUrl
      ? `<img class="entity-cover" src="${escapeHtml(imageUrl)}" alt="" loading="lazy" />`
      : '<span class="entity-cover" aria-hidden="true"></span>';
  }

  function entityName(item) {
    return item.entityName || "Untitled";
  }

  function renderLiveGroup(type, items) {
    const container = byId(type === "track" ? "live-songs" : "live-artists");
    const count = byId(type === "track" ? "live-songs-count" : "live-artists-count");
    const query = byId("live-filter").value.trim().toLocaleLowerCase();
    const filtered = items.filter((item) => `${item.entityName} ${item.artistNames} ${item.entityUri}`.toLocaleLowerCase().includes(query));
    count.textContent = query ? `${filtered.length} of ${items.length} results` : `${items.length} chart entries`;
    if (!items.length) {
      container.innerHTML = '<p class="entity-empty">No entries were returned for this chart.</p>';
      return;
    }
    if (!filtered.length) {
      container.innerHTML = '<p class="entity-empty">No chart entries match this filter.</p>';
      return;
    }

    container.innerHTML = filtered.map((item) => {
      const url = spotifyUrl(item.entityUri);
      const title = escapeHtml(entityName(item));
      const link = url
        ? `<a class="entity-title" href="${escapeHtml(url)}" target="_blank" rel="noreferrer">${title}</a>`
        : `<span class="entity-title">${title}</span>`;
      const subtitle = item.artistNames || (type === "track" ? "Song" : "Artist");
      const metricValue = item.metricValue == null ? null : String(item.metricValue);
      const numericMetric = metricValue == null ? Number.NaN : Number(metricValue.replaceAll(",", ""));
      const formattedMetric = Number.isFinite(numericMetric) ? numericMetric.toLocaleString() : metricValue;
      const metric = formattedMetric != null
        ? `${escapeHtml(formattedMetric)} ${escapeHtml(item.metricType || "")}`
        : "No metric returned";
      const rank = item.chartRank != null ? `#${escapeHtml(item.chartRank)}` : "—";
      return `<article class="entity-row">${artwork(item.imageUrl, item.entityName)}<div class="entity-copy">${link}<div class="entity-subtitle">${escapeHtml(subtitle)}</div></div><div class="entity-metrics"><div class="entity-rank">${rank}</div><div class="entity-streams">${metric}</div></div></article>`;
    }).join("");
  }

  async function loadLiveData() {
    const requestId = ++liveRequest;
    const button = byId("live-refresh");
    button.disabled = true;
    button.textContent = "Loading…";
    setStatus("live-status", "Loading the latest available charts…");
    const params = new URLSearchParams({
      country: byId("live-country").value || "GLOBAL",
      cadence: byId("live-cadence").value || "daily",
    });

    try {
      const payload = await requestJson(`/api/selected/live?${params}`);
      if (requestId !== liveRequest) return;
      liveData = { songs: payload.songs || [], artists: payload.artists || [] };
      renderLiveGroup("track", liveData.songs);
      renderLiveGroup("artist", liveData.artists);
      const total = liveData.songs.length + liveData.artists.length;
      setStatus("live-status", `${total.toLocaleString()} chart entries loaded for ${payload.country}. Full snapshots are saved to History when available.`);
    } catch (error) {
      if (requestId === liveRequest) setStatus("live-status", error.message, true);
    } finally {
      if (requestId === liveRequest) {
        button.disabled = false;
        button.textContent = "Refresh live data";
      }
    }
  }

  function renderSelectionRow(item, saved) {
    const uri = item.entityUri;
    const alreadySaved = saved || selections.some((selection) => selection.entityUri === uri);
    const button = saved
      ? `<button class="selection-action remove" type="button" data-remove-uri="${escapeHtml(uri)}">Remove</button>`
      : `<button class="selection-action" type="button" data-add-uri="${escapeHtml(uri)}" ${alreadySaved ? "disabled" : ""}>${alreadySaved ? "Added" : "Add"}</button>`;
    const typeLabel = item.entityType === "artist" ? "Artist" : "Song";
    const subtitle = item.artistNames || typeLabel;
    return `<article class="selection-item">${artwork(item.imageUrl, item.entityName)}<div class="entity-copy"><span class="entity-title">${escapeHtml(entityName(item))}</span><span class="selection-kind">${escapeHtml(subtitle)}</span></div>${button}</article>`;
  }

  function renderSelections() {
    const savedList = byId("selection-list");
    savedList.innerHTML = selections.length
      ? selections.map((item) => renderSelectionRow(item, true)).join("")
      : '<p class="selection-empty">Your saved selection is empty.</p>';
    byId("selection-results").innerHTML = searchResults.length
      ? searchResults.map((item) => renderSelectionRow(item, false)).join("")
      : '<p class="selection-empty">Search results will appear here.</p>';
  }

  async function loadSelections() {
    try {
      selections = await requestJson("/api/selections");
      renderSelections();
      return selections;
    } catch (error) {
      setStatus("selection-status", error.message, true);
      throw error;
    }
  }

  async function refreshAfterConnection() {
    if (connectionRefreshInFlight) return;
    connectionRefreshInFlight = true;
    try {
      const status = await requestJson("/api/status");
      if (!status.connected) return;
      await loadSelections();
      if (activePage === "live") await loadLiveData();
      if (activePage === "history") await loadHistory();
    } catch (error) {
      if (activePage === "live") setStatus("live-status", error.message, true);
      if (activePage === "history") setStatus("selected-history-status", error.message, true);
    } finally {
      connectionRefreshInFlight = false;
    }
  }

  authChannel?.addEventListener("message", (event) => {
    if (event.data === "connected") refreshAfterConnection();
  });
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) refreshAfterConnection();
  });
  window.addEventListener("focus", refreshAfterConnection);

  async function searchSpotify(event) {
    event.preventDefault();
    const query = byId("selection-query").value.trim();
    const button = byId("selection-search-button");
    if (query.length < 2) return;
    button.disabled = true;
    button.textContent = "Searching…";
    setStatus("selection-status", "Searching Spotify…");
    try {
      const params = new URLSearchParams({ query, entity_type: byId("selection-type").value, limit: "10" });
      const payload = await requestJson(`/api/search?${params}`);
      searchResults = payload.items || [];
      renderSelections();
      setStatus("selection-status", `${searchResults.length} Spotify results found.`);
    } catch (error) {
      searchResults = [];
      renderSelections();
      setStatus("selection-status", error.message, true);
    } finally {
      button.disabled = false;
      button.textContent = "Search";
    }
  }

  async function addSelection(uri) {
    const item = searchResults.find((result) => result.entityUri === uri);
    if (!item) return;
    setStatus("selection-status", `Adding ${entityName(item)}…`);
    try {
      await requestJson("/api/selections", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          entity_type: item.entityType,
          entity_uri: item.entityUri,
          entity_name: item.entityName,
          artist_names: item.artistNames || "",
          image_url: item.imageUrl || null,
        }),
      });
      await loadSelections();
      renderSelections();
      setStatus("selection-status", `${entityName(item)} added to your saved selection.`);
      if (location.hash === "#live") setStatus("live-status", "Saved to your watchlist. The full chart results remain unchanged.");
    } catch (error) {
      setStatus("selection-status", error.message, true);
    }
  }

  async function removeSelection(uri) {
    const item = selections.find((selection) => selection.entityUri === uri);
    try {
      await requestJson(`/api/selections/${encodeURIComponent(uri)}`, { method: "DELETE" });
      selections = selections.filter((selection) => selection.entityUri !== uri);
      renderSelections();
      const message = `${entityName(item || {})} removed from your selection. Existing snapshots are retained.`;
      setStatus("selection-status", message);
      if (location.hash === "#live") setStatus("live-status", "Watchlist updated. Full chart results remain available.");
    } catch (error) {
      setStatus("selection-status", error.message, true);
      if (location.hash === "#live") setStatus("live-status", error.message, true);
    }
  }

  function renderHistoryTable(points = historyData?.points || []) {
    const query = byId("history-filter").value.trim().toLocaleLowerCase();
    const filtered = points.filter((point) => `${point.entityName} ${point.artistNames} ${point.entityUri}`.toLocaleLowerCase().includes(query));
    const rows = filtered.slice(0, 200);
    if (!filtered.length) {
      byId("history-table").innerHTML = '<p class="entity-empty">No saved chart entries match this filter.</p>';
      return;
    }
    byId("history-table").innerHTML = `<table><thead><tr><th scope="col">Song or artist</th><th scope="col">Latest chart date</th><th scope="col">Rank</th><th scope="col">Streams</th><th scope="col"><span class="sr-only">Actions</span></th></tr></thead><tbody>${rows.map((point) => `<tr><td>${escapeHtml(point.entityName)}${point.artistNames ? `<span class="entity-subtitle">${escapeHtml(point.artistNames)}</span>` : ""}</td><td>${escapeHtml(point.chartDate)}</td><td>${escapeHtml(point.chartRank ?? "—")}</td><td>${escapeHtml(point.streamCount == null ? "—" : Number(point.streamCount).toLocaleString())}</td><td><button class="quiet-button history-trend" type="button" data-history-uri="${escapeHtml(point.entityUri)}" aria-label="View history for ${escapeHtml(point.entityName)}">View trend</button></td></tr>`).join("")}</tbody></table>${filtered.length > rows.length ? `<p class="entity-empty">Showing ${rows.length} of ${filtered.length} matching results.</p>` : ""}`;
  }

  function renderHistory(payload) {
    historyData = payload;
    historyTrendData = null;
    if (historyChart) {
      historyChart.destroy();
      historyChart = null;
    }
    byId("history-chart").hidden = true;
    byId("history-empty").hidden = false;
    byId("history-empty").textContent = "Choose any result below to view its history trend.";
    byId("history-chart-title").textContent = "Chart history";
    byId("history-chart-range").textContent = `${(payload.points || []).length} latest results · ${payload.country}`;
    renderHistoryTable(payload.points || []);
    setStatus("selected-history-status", `${(payload.points || []).length.toLocaleString()} latest chart results loaded. Filter the results, then open a trend.`);
  }

  function renderHistoryTrend(payload) {
    const points = payload.points || [];
    const metric = byId("history-metric").value;
    const entity = payload.entity;
    const canvas = byId("history-chart");
    const empty = byId("history-empty");
    const values = points.map((point) => metric === "rank" ? point.chartRank : point.streamCount);
    const hasValues = values.some((value) => value != null);
    if (historyChart) {
      historyChart.destroy();
      historyChart = null;
    }
    canvas.hidden = !hasValues;
    empty.hidden = hasValues;
    if (hasValues && window.Chart) {
      historyChart = new window.Chart(canvas.getContext("2d"), {
        type: "line",
        data: {
          labels: points.map((point) => point.chartDate),
          datasets: [{
            label: metric === "rank" ? "Chart rank" : "Chart streams",
            data: values,
            borderColor: "#b8f26b",
            backgroundColor: "rgba(184,242,107,.12)",
            borderWidth: 2,
            pointRadius: points.length > 40 ? 0 : 2,
            pointHoverRadius: 5,
            tension: 0.28,
            fill: true,
            spanGaps: false,
          }],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { intersect: false, mode: "index" },
          plugins: {
            legend: { display: false },
            tooltip: { callbacks: { label: (context) => `${context.dataset.label}: ${Number(context.parsed.y).toLocaleString()}` } },
          },
          scales: {
            x: { ticks: { color: "#92998d", maxTicksLimit: 8 }, grid: { color: "rgba(146,153,141,.10)" } },
            y: {
              reverse: metric === "rank",
              beginAtZero: metric === "streams",
              ticks: { color: "#92998d", callback: (value) => Number(value).toLocaleString() },
              grid: { color: "rgba(146,153,141,.10)" },
            },
          },
        },
      });
    }
    byId("history-chart-title").textContent = entity ? `${entity.entityName} · ${metric === "rank" ? "chart rank" : "chart streams"}` : "Chart history";
    byId("history-chart-range").textContent = points.length ? `${points.length} snapshots · ${payload.country}` : "No data loaded";
    if (!points.length) empty.textContent = "No snapshots were found for this result in the selected date range.";
    setStatus("selected-history-status", entity ? `${points.length} stored chart snapshots for ${entity.entityName}.` : "Choose a result to view its trend.");
  }

  async function viewHistoryTrend(entityUri) {
    const params = new URLSearchParams({
      chart_type: byId("history-type").value,
      country: byId("history-country").value || "GLOBAL",
      entity_uri: entityUri,
      range: byId("history-range").value,
    });
    setStatus("selected-history-status", "Loading this chart entry’s history…");
    try {
      historyTrendData = await requestJson(`/api/selected/history?${params}`);
      renderHistoryTrend(historyTrendData);
    } catch (error) {
      setStatus("selected-history-status", error.message, true);
    }
  }

  function updateHistoryMetric() {
    const isArtist = historyTypes[byId("history-type").value] === "artist";
    const streamsOption = byId("history-metric").querySelector('option[value="streams"]');
    streamsOption.disabled = isArtist;
    if (isArtist && byId("history-metric").value === "streams") byId("history-metric").value = "rank";
  }

  async function loadHistory() {
    setStatus("selected-history-status", "Loading all saved chart results…");
    const params = new URLSearchParams({
      chart_type: byId("history-type").value,
      country: byId("history-country").value || "GLOBAL",
      range: byId("history-range").value,
    });
    try {
      const payload = await requestJson(`/api/selected/history?${params}`);
      renderHistory(payload);
    } catch (error) {
      setStatus("selected-history-status", error.message, true);
    }
  }

  async function backfillHistory() {
    const chartType = byId("history-type").value;
    const country = byId("history-country").value || "GLOBAL";
    const button = byId("history-backfill");
    button.disabled = true;
    button.textContent = "Loading current chart…";
    setStatus("selected-history-status", "Loading the latest chart to set the history target…");
    try {
      const chartParams = new URLSearchParams({ type: chartType, country, date: "latest" });
      const chartPayload = await requestJson(`/api/charts?${chartParams}`);
      const historyInfo = chartPayload.chartHistory;
      if (!historyInfo?.supported || !historyInfo.targetDate) {
        setStatus("selected-history-status", historyInfo?.reason || "This chart cannot be backfilled yet.", true);
        return;
      }

      const rangeValue = byId("history-range").value;
      const rangeDays = rangeValue === "all" ? 3650 : Number(rangeValue);
      const startDate = new Date(`${historyInfo.targetDate}T00:00:00Z`);
      startDate.setUTCDate(startDate.getUTCDate() - Math.max(0, rangeDays - 1));
      button.textContent = "Fetching one batch…";
      const progress = await requestJson("/api/chart-history/backfill", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chart_type: chartType,
          country,
          target_date: historyInfo.targetDate,
          start_date: startDate.toISOString().slice(0, 10),
        }),
      });
      await loadHistory();
      const progressText = `${Number(progress.processedDates || 0).toLocaleString()} of ${Number(progress.totalDates || 0).toLocaleString()} chart dates processed.`;
      if (progress.complete) {
        button.textContent = "History complete";
        setStatus("selected-history-status", `All chart entries are saved through ${progress.targetDate}. ${progressText}`);
      } else if (progress.status === "paused" || progress.status === "unavailable") {
        button.textContent = "Retry history backfill";
        setStatus("selected-history-status", `${progress.error || "Backfill paused."} ${progressText}`, true);
      } else {
        button.textContent = "Continue backfill";
        setStatus("selected-history-status", `${progressText} Continue to fetch the next batch.`);
      }
    } catch (error) {
      button.textContent = "Retry history backfill";
      setStatus("selected-history-status", error.message, true);
    } finally {
      button.disabled = false;
    }
  }

  function activatePage(page) {
    const validPage = ["charts", "live", "history"].includes(page) ? page : "charts";
    activePage = validPage;
    document.querySelectorAll("[data-page-panel]").forEach((panel) => {
      panel.hidden = panel.dataset.pagePanel !== validPage;
    });
    document.querySelectorAll("[data-page-nav]").forEach((link) => {
      const active = link.dataset.pageNav === validPage;
      link.classList.toggle("active", active);
      if (active) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    });
    if (validPage === "live") {
      loadSelections().catch(() => {}).finally(loadLiveData);
    }
    if (validPage === "history") {
      updateHistoryMetric();
      loadHistory();
    }
  }

  window.addEventListener("hashchange", () => activatePage(location.hash.slice(1)));
  byId("manage-selections").addEventListener("click", async () => {
    byId("selection-dialog").showModal();
    await loadSelections().catch(() => {});
  });
  byId("selection-close").addEventListener("click", () => byId("selection-dialog").close());
  byId("selection-search-form").addEventListener("submit", searchSpotify);
  byId("selection-results").addEventListener("click", (event) => {
    const button = event.target.closest("[data-add-uri]");
    if (button) addSelection(button.dataset.addUri);
  });
  byId("selection-list").addEventListener("click", (event) => {
    const button = event.target.closest("[data-remove-uri]");
    if (button) removeSelection(button.dataset.removeUri);
  });
  byId("live-controls").addEventListener("submit", (event) => {
    event.preventDefault();
    loadLiveData();
  });
  byId("live-filter").addEventListener("input", () => {
    renderLiveGroup("track", liveData.songs);
    renderLiveGroup("artist", liveData.artists);
  });
  byId("live-connect").addEventListener("click", () => {
    window.open("/auth/login", "_blank", "noopener,noreferrer");
    setStatus("live-status", "Spotify sign-in opened in a new tab. Return here after authorization; this page will refresh automatically.");
  });
  byId("history-controls").addEventListener("submit", (event) => {
    event.preventDefault();
    loadHistory();
  });
  byId("history-type").addEventListener("change", () => {
    updateHistoryMetric();
    loadHistory();
  });
  byId("history-metric").addEventListener("change", () => {
    if (historyTrendData) renderHistoryTrend(historyTrendData);
  });
  byId("history-range").addEventListener("change", loadHistory);
  byId("history-country").addEventListener("change", loadHistory);
  byId("history-filter").addEventListener("input", () => renderHistoryTable());
  byId("history-table").addEventListener("click", (event) => {
    const button = event.target.closest("[data-history-uri]");
    if (button) viewHistoryTrend(button.dataset.historyUri);
  });
  byId("history-refresh").addEventListener("click", loadHistory);
  byId("history-connect").addEventListener("click", () => {
    window.open("/auth/login", "_blank", "noopener,noreferrer");
    setStatus("selected-history-status", "Spotify sign-in opened in a new tab. Return here after authorization; this page will refresh automatically.");
  });
  byId("history-backfill").addEventListener("click", backfillHistory);
  updateHistoryMetric();

  activatePage(location.hash.slice(1) || "charts");
})();
