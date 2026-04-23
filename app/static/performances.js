const els = {
  nickname: document.getElementById("perfNickname"),
  draftSelect: document.getElementById("perfDraftSelect"),
  performanceList: document.getElementById("performanceList"),
  performanceDetail: document.getElementById("performanceDetail"),
};

function renderPlaceholder(target, text) {
  target.innerHTML = "";
  const li = document.createElement("li");
  li.textContent = text;
  li.style.cursor = "default";
  target.appendChild(li);
}

async function loadDrafts() {
  const nickname = els.nickname.value.trim();
  if (!nickname) {
    els.draftSelect.innerHTML = "";
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "Enter nickname to load drafts";
    opt.disabled = true;
    opt.selected = true;
    els.draftSelect.appendChild(opt);
    renderPlaceholder(els.performanceList, "No drafts loaded.");
    return;
  }
  const res = await fetch(`/api/drafts?nickname=${encodeURIComponent(nickname)}`);
  const data = await res.json();
  if (!res.ok) {
    renderPlaceholder(els.performanceList, data.error || "Failed to load drafts.");
    return;
  }
  els.draftSelect.innerHTML = "";
  if (!data.items.length) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "No drafts yet";
    opt.disabled = true;
    opt.selected = true;
    els.draftSelect.appendChild(opt);
    renderPlaceholder(els.performanceList, "No performances yet.");
    return;
  }
  data.items.forEach((d) => {
    const opt = document.createElement("option");
    opt.value = d.id;
    opt.textContent = d.title;
    els.draftSelect.appendChild(opt);
  });
  await loadPerformances(els.draftSelect.value);
}

async function loadPerformances(draftId) {
  if (!draftId) {
    renderPlaceholder(els.performanceList, "Select a draft.");
    return;
  }
  const res = await fetch(`/api/performances?draft_id=${draftId}`);
  const data = await res.json();
  els.performanceList.innerHTML = "";
  if (!res.ok) {
    renderPlaceholder(els.performanceList, data.error || "Failed to load history.");
    return;
  }
  if (!data.items.length) {
    renderPlaceholder(els.performanceList, "No performances yet.");
    return;
  }
  data.items.forEach((item) => {
    const li = document.createElement("li");
    const time = item.created_at ? new Date(item.created_at).toLocaleString() : "";
    li.textContent = `${time} · ${item.status} · score ${item.score ?? "-"}`;
    li.addEventListener("click", () => loadPerformanceDetail(item.id));
    els.performanceList.appendChild(li);
  });
}

async function loadPerformanceDetail(performanceId) {
  if (!performanceId) return;
  const res = await fetch(`/api/performances/${performanceId}`);
  const data = await res.json();
  els.performanceDetail.innerHTML = "";
  if (!res.ok) {
    els.performanceDetail.textContent = data.error || "Failed to load detail.";
    return;
  }
  const header = document.createElement("div");
  header.className = "stage-line";
  header.textContent = `Status: ${data.status} · Score: ${data.score ?? "-"}`;
  els.performanceDetail.appendChild(header);
  data.events.forEach((event) => {
    const line = document.createElement("div");
    line.className = "stage-line";
    const role = document.createElement("div");
    role.className = `stage-role ${event.role}`;
    role.textContent = event.role;
    const body = document.createElement("div");
    body.textContent = event.text;
    line.appendChild(role);
    line.appendChild(body);
    els.performanceDetail.appendChild(line);
  });
}

els.nickname.addEventListener("blur", loadDrafts);
els.nickname.addEventListener("keydown", (e) => {
  if (e.key === "Enter") loadDrafts();
});
els.draftSelect.addEventListener("change", (e) => {
  loadPerformances(e.target.value);
});

renderPlaceholder(els.performanceList, "Enter nickname to load history.");
