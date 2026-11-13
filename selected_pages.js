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
  let historyChart = null;
  let liveRequest = 0;

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
    count.textContent = `${items.length} followed`;
    if (!items.length) {
      container.innerHTML = '<p class="entity-empty">No selections yet. Use Manage selection to add songs or artists.</p>';
      return;
    }

    container.innerHTML = items.map((item) => {
      const url = spotifyUrl(item.entityUri);
      const title = escapeHtml(entityName(item));
      const link = url
        ? `<a class="entity-title" href="${escapeHtml(url)}" target="_blank" rel="noreferrer">${title}</a>`
        : `<span class="entity-title">${title}</span>`;
      const subtitle = item.artistNames || (item.inChart ? "In the latest chart" : "Not in the latest chart");
      const metricValue = item.metricValue == null ? null : String(item.metricValue);
      const numericMetric = metricValue == null ? Number.NaN : Number(metricValue.replaceAll(",", ""));
      const formattedMetric = Number.isFinite(numericMetric) ? numericMetric.toLocaleString() : metricValue;
      const metric = formattedMetric != null
        ? `${escapeHtml(formattedMetric)} ${escapeHtml(item.metricType || "")}`
        : "No metric returned";
      const rank = item.inChart && item.chartRank != null ? `#${escapeHtml(item.chartRank)}` : "—";
      return `<article class="entity-row">${artwork(item.imageUrl, item.entityName)}<div class="entity-copy">${link}<div class="entity-subtitle">${escapeHtml(subtitle)}</div></div><div class="entity-metrics"><div class="entity-rank">${rank}</div><div class="entity-streams">${item.inChart ? metric : "Outside current chart"}</div></div></article>`;
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
      renderLiveGroup("track", payload.songs || []);
      renderLiveGroup("artist", payload.artists || []);
      const total = (payload.songs || []).length + (payload.artists || []).length;
      const hasCurrent = [...(payload.songs || []), ...(payload.artists || [])].some((item) => item.inChart);
      setStatus("live-status", total
        ? `${hasCurrent ? "Latest chart data loaded" : "No selected entities are in this chart"}. Current snapshots are saved to history when available.`
        : "Add songs or artists to your watchlist to get started.");
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
    populateHistoryEntities();
  }

  function currentHistoryType() {
    return historyTypes[byId("history-type").value] || "track";
  }

  function populateHistoryEntities() {
    const type = currentHistoryType();
    const options = selections.filter((item) => item.entityType === type);
    byId("history-backfill").disabled = options.length === 0;
    const select = byId("history-entity");
    const previousValue = select.value;
    select.innerHTML = options.length
      ? options.map((item) => `<option value="${escapeHtml(item.entityUri)}">${escapeHtml(entityName(item))}</option>`).join("")
      : `<option value="">No selected ${type === "track" ? "songs" : "artists"}</option>`;
    if (options.some((item) => item.entityUri === previousValue)) select.value = previousValue;
    const streamsOption = byId("history-metric").querySelector('option[value="streams"]');
    streamsOption.disabled = type === "artist";
    if (type === "artist" && byId("history-metric").value === "streams") byId("history-metric").value = "rank";
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

  async function searchSpotify(event) {
    event.preventDefault();
    const query = byId("selection-query").value.trim();
    const button = byId("selection-search-button");
    if (query.length < 2) return;
    button.disabled = true;
    button.textContent = "Searching…";
    setStatus("selection-status", "Searching Spotify…");
    try {
      const params = new URLSearchParams({ query, entity_type: byId("selection-type").value, limit: "12" });
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
      if (location.hash === "#live") setStatus("live-status", "Selection saved. Refresh live data to update its current chart position.");
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
      setStatus("selection-status", `${entityName(item || {})} removed from your selection. Existing snapshots are retained.`);
      if (location.hash === "#live") {
        renderLiveGroup("track", selections.filter((selection) => selection.entityType === "track").map((selection) => ({ ...selection, inChart: false })));
        renderLiveGroup("artist", selections.filter((selection) => selection.entityType === "artist").map((selection) => ({ ...selection, inChart: false })));
      }
    } catch (error) {
      setStatus("selection-status", error.message, true);
    }
  }

  function renderHistoryTable(points, metric) {
    const rows = [...points].reverse().slice(0, 12);
    if (!rows.length) {
      byId("history-table").innerHTML = '<p class="entity-empty">No snapshots are available for this selection and market yet.</p>';
      return;
    }
    const metricLabel = metric === "rank" ? "Chart rank" : "Chart streams";
    byId("history-table").innerHTML = `<table><thead><tr><th scope="col">Chart date</th><th scope="col">${metricLabel}</th><th scope="col">Rank</th><th scope="col">Streams</th></tr></thead><tbody>${rows.map((point) => `<tr><td>${escapeHtml(point.chartDate)}</td><td>${metric === "rank" ? escapeHtml(point.chartRank ?? "—") : escapeHtml(point.streamCount == null ? "—" : Number(point.streamCount).toLocaleString())}</td><td>${escapeHtml(point.chartRank ?? "—")}</td><td>${escapeHtml(point.streamCount == null ? "—" : Number(point.streamCount).toLocaleString())}</td></tr>`).join("")}</tbody></table>`;
  }

  function renderHistory(payload) {
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
    renderHistoryTable(points, metric);
    if (!entity) setStatus("history-status", "Add a song or artist matching this chart type, then load live data to begin collecting history.");
    else if (!points.length) setStatus("history-status", `No stored snapshots for ${entity.entityName} in this market yet. Refresh Live selected to collect the latest snapshot.`);
    else setStatus("history-status", `${points.length} stored chart snapshots for ${entity.entityName}.`);
  }

  async function loadHistory() {
    const chartType = byId("history-type").value;
    const entityUri = byId("history-entity").value;
    if (!entityUri) {
      renderHistory({ entity: null, points: [], country: byId("history-country").value });
      return;
    }
    setStatus("history-status", "Loading stored snapshots…");
    const params = new URLSearchParams({
      chart_type: chartType,
      country: byId("history-country").value || "GLOBAL",
      entity_uri: entityUri,
      range: byId("history-range").value,
    });
    try {
      const payload = await requestJson(`/api/selected/history?${params}`);
      renderHistory(payload);
    } catch (error) {
      setStatus("history-status", error.message, true);
    }
  }

  async function backfillHistory() {
    const chartType = byId("history-type").value;
    const country = byId("history-country").value || "GLOBAL";
    const entityType = historyTypes[chartType];
    const entityUris = selections.filter((item) => item.entityType === entityType).map((item) => item.entityUri);
    if (!entityUris.length) {
      setStatus("history-status", `Add a ${entityType === "track" ? "song" : "artist"} to your selection first.`, true);
      return;
    }

    const button = byId("history-backfill");
    button.disabled = true;
    button.textContent = "Loading current chart…";
    setStatus("history-status", "Loading the latest chart to set the history target…");
    try {
      const chartParams = new URLSearchParams({ type: chartType, country, date: "latest" });
      const chartPayload = await requestJson(`/api/charts?${chartParams}`);
      const historyInfo = chartPayload.chartHistory;
      if (!historyInfo?.supported) {
        setStatus("history-status", historyInfo?.reason || "This chart cannot be backfilled yet.", true);
        return;
      }
      if (historyInfo.complete) {
        setStatus("history-status", `History is complete through ${historyInfo.targetDate}.`);
        await loadHistory();
        return;
      }

      button.textContent = "Fetching one batch…";
      const progress = await requestJson("/api/chart-history/backfill", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          chart_type: chartType,
          country,
          target_date: historyInfo.targetDate,
          entity_uris: entityUris,
        }),
      });
      await loadHistory();
      const progressText = `${Number(progress.processedDates || 0).toLocaleString()} of ${Number(progress.totalDates || 0).toLocaleString()} chart dates processed.`;
      if (progress.complete) {
        button.textContent = "History complete";
        setStatus("history-status", `History is complete through ${progress.targetDate}. ${progressText}`);
      } else if (progress.status === "paused" || progress.status === "unavailable") {
        button.textContent = "Retry history backfill";
        setStatus("history-status", `${progress.error || "Backfill paused."} ${progressText}`, true);
      } else {
        button.textContent = "Continue backfill";
        setStatus("history-status", `${progressText} Continue to fetch the next batch.`);
      }
    } catch (error) {
      button.textContent = "Retry history backfill";
      setStatus("history-status", error.message, true);
    } finally {
      button.disabled = false;
    }
  }

  function activatePage(page) {
    const validPage = ["charts", "live", "history"].includes(page) ? page : "charts";
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
      loadSelections().then((items) => {
        if (items.length) return loadLiveData();
        renderLiveGroup("track", []);
        renderLiveGroup("artist", []);
      }).catch((error) => {
        renderLiveGroup("track", []);
        renderLiveGroup("artist", []);
        setStatus("live-status", error.message, true);
      });
    }
    if (validPage === "history") {
      loadSelections().then(loadHistory).catch((error) => setStatus("history-status", error.message, true));
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
  byId("live-connect").addEventListener("click", () => {
    window.open("/auth/login", "_blank", "noopener,noreferrer");
    setStatus("live-status", "Spotify sign-in opened in a new tab. Complete authorization there, then refresh live data.");
  });
  byId("history-controls").addEventListener("submit", (event) => {
    event.preventDefault();
    loadHistory();
  });
  byId("history-type").addEventListener("change", () => {
    populateHistoryEntities();
    loadHistory();
  });
  byId("history-metric").addEventListener("change", loadHistory);
  byId("history-range").addEventListener("change", loadHistory);
  byId("history-country").addEventListener("change", loadHistory);
  byId("history-entity").addEventListener("change", loadHistory);
  byId("history-refresh").addEventListener("click", loadHistory);
  byId("history-connect").addEventListener("click", () => {
    window.open("/auth/login", "_blank", "noopener,noreferrer");
    setStatus("history-status", "Spotify sign-in opened in a new tab. Complete authorization there, then refresh history.");
  });
  byId("history-backfill").addEventListener("click", backfillHistory);
  populateHistoryEntities();

  activatePage(location.hash.slice(1) || "charts");
})();
