const els = {
  nicknameInput: document.getElementById("nicknameInput"),
  draftSelect: document.getElementById("draftSelect"),
  newDraftBtn: document.getElementById("newDraftBtn"),
  renameDraftBtn: document.getElementById("renameDraftBtn"),
  archiveDraftBtn: document.getElementById("archiveDraftBtn"),
  renameModal: document.getElementById("renameModal"),
  renameInput: document.getElementById("renameInput"),
  renameCancelBtn: document.getElementById("renameCancelBtn"),
  renameConfirmBtn: document.getElementById("renameConfirmBtn"),
  bitsContainer: document.getElementById("bitsContainer"),
  addBitBtn: document.getElementById("addBitBtn"),
  topicInput: document.getElementById("topicInput"),
  genPunchlines: document.getElementById("genPunchlines"),
  punchlineList: document.getElementById("punchlineList"),
  suggestionList: document.getElementById("suggestionList"),
  ghost: document.createElement("div"),
  saveStatus: document.getElementById("saveStatus"),
  saveDraftBtn: document.getElementById("saveDraftBtn"),
  asrBtn: document.getElementById("asrBtn"),
  asrStatus: document.getElementById("asrStatus"),
  transcriptView: document.getElementById("transcriptView"),
  saveTranscriptBtn: document.getElementById("saveTranscriptBtn"),
  focusedNoteEmpty: document.getElementById("focusedNoteEmpty"),
  focusedNoteCard: document.getElementById("focusedNoteCard"),
  focusedNoteQuote: document.getElementById("focusedNoteQuote"),
  focusedNoteMeta: document.getElementById("focusedNoteMeta"),
  focusedNotePlayers: document.getElementById("focusedNotePlayers"),
  focusedNoteAdvice: document.getElementById("focusedNoteAdvice"),
  focusedNoteWhy: document.getElementById("focusedNoteWhy"),
  focusedNoteNext: document.getElementById("focusedNoteNext"),
  focusedNoteVideo: document.getElementById("focusedNoteVideo"),
  uploadRehearsalBtn: document.getElementById("uploadRehearsalBtn"),
  rehearsalAudioInput: document.getElementById("rehearsalAudioInput"),
  analyzeRehearsalBtn: document.getElementById("analyzeRehearsalBtn"),
  stylePresetSelect: document.getElementById("stylePresetSelect"),
  stylePresetInput: document.getElementById("stylePresetInput"),
  saveStylePresetBtn: document.getElementById("saveStylePresetBtn"),
  includeVideoRef: document.getElementById("includeVideoRef"),
  detectedStyle: document.getElementById("detectedStyle"),
  videoReferenceList: document.getElementById("videoReferenceList"),
  videoDatasetStatus: document.getElementById("videoDatasetStatus"),
  rehearsalStatus: document.getElementById("rehearsalStatus"),
  analysisScript: document.getElementById("analysisScript"),
  markerTimeline: document.getElementById("markerTimeline"),
  evidenceAudio: document.getElementById("evidenceAudio"),
  demoAudio: document.getElementById("demoAudio"),
  errorBanner: document.getElementById("errorBanner"),
  feedbackText: document.getElementById("feedbackText"),
  downloadFeedbackLink: document.getElementById("downloadFeedbackLink"),
  antiExamples: document.getElementById("antiExamples"),
  processMapLog: document.getElementById("processMapLog"),
  stageLog: document.getElementById("stageLog"),
  stageStatus: document.getElementById("stageStatus"),
  processMapTitle: document.getElementById("processMapTitle"),
  processMapViewBtn: document.getElementById("processMapViewBtn"),
  stageLogViewBtn: document.getElementById("stageLogViewBtn"),
  stagePrompt: document.getElementById("stagePrompt"),
  stageStartBtn: document.getElementById("stageStartBtn"),
  stageDismissBtn: document.getElementById("stageDismissBtn"),
  cancelPrompt: document.getElementById("cancelPrompt"),
  cancelSaveBtn: document.getElementById("cancelSaveBtn"),
  cancelDiscardBtn: document.getElementById("cancelDiscardBtn"),
  reviewLoader: document.getElementById("reviewLoader"),
  reviewText: document.getElementById("reviewText"),
  reviewAudio: document.getElementById("reviewAudio"),
  reviewPanelOverlay: document.getElementById("reviewPanelOverlay"),
  reviewPanel: document.querySelector(".review-panel"),
  teleprompterWrap: document.getElementById("teleprompterWrap"),
  teleprompterBody: document.getElementById("teleprompterBody"),
  editorWrap: document.querySelector(".editor-wrap"),
  backToEditFromTeleprompterBtn: document.getElementById("backToEditFromTeleprompterBtn"),
};

let currentDraftId = null;
let bits = [];
let selectedBits = [];
let appState = "editing"; // "editing" | "ready_to_perform"
let _sortableInstance = null;
let idleTimer = null;
let lastInputAt = Date.now();
const idleDelayMs = 1500;
let eventSource = null;
let suggestionInFlight = false;
let lastUserTypedAt = Date.now();
let suppressGhostClearOnce = false;
let currentSuggestion = ""; // text of the one suggestion currently shown in the active card
let performanceIdleTimer = null;
const performanceIdleMs = 2000;
let performanceRunning = false;
let activePerformanceId = null;
let cancelPending = false;
let pendingCancelAction = null;
let performancePromptArmed = false;
const ignoredPerformanceIds = new Set();
let reviewTypingTimer = null;
let asrRecorder = null;
let asrStream = null;
let asrChunks = [];
let asrRecording = false;
let browserSpeechRecognition = null;
let browserSpeechRecognitionAvailable = false;
let browserAsrTranscript = "";
let browserAsrInterim = "";
let browserAsrCommittedFinal = "";
let browserAsrLiveFinal = "";
let currentTranscript = ""; // live transcript from the latest recording/upload; never touches the bit cards
let lastRehearsalBlob = null;
let lastRehearsalFilename = "";
let lastRehearsalMarkers = [];
let lastUtterances = [];
let lastFocusNotes = [];
let lastVideoReferences = [];
let lastComedianMatches = [];
let lastMarkerFeedback = { summary: "", items: [] };
let lastProcessMap = null;
let lastAnalyzedScript = "";
let lastVideoDatasetStatus = null;
let activeMarkerId = null;
let activeUtteranceId = null;
let activeStageView = "process_map";
let lastFeedbackDownloadUrl = null;
let performedScriptRange = null;
let selectedMarkerRange = null;
const STYLE_PRESET_STORAGE_KEY = "talkshow_style_presets_v1";
const DEFAULT_STYLE_PRESETS = [
  "dry observational",
  "storytelling",
  "deadpan",
  "high-energy",
];


function setStageView(viewName) {
  activeStageView = viewName === "stage" ? "stage" : "process_map";
  if (els.processMapLog) els.processMapLog.classList.toggle("hidden", activeStageView !== "process_map");
  if (els.stageLog) els.stageLog.classList.toggle("hidden", activeStageView !== "stage");
  if (els.processMapViewBtn) els.processMapViewBtn.classList.toggle("is-active", activeStageView === "process_map");
  if (els.stageLogViewBtn) els.stageLogViewBtn.classList.toggle("is-active", activeStageView === "stage");
}

function sanitizeDisplayText(text) {
  if (!text) return "";
  return text
    .replace(/\*\*/g, "")
    .replace(/^#{1,6}\s*/gm, "")
    .replace(/`{1,3}/g, "")
    .trim();
}


function getSpeechRecognitionCtor() {
  return window.SpeechRecognition || window.webkitSpeechRecognition || null;
}

function recomputeBrowserTranscript() {
  browserAsrTranscript = sanitizeDisplayText(`${browserAsrCommittedFinal} ${browserAsrLiveFinal}`);
  return sanitizeDisplayText(`${browserAsrTranscript} ${browserAsrInterim}`);
}

function finalizeBrowserSpeechRecognitionBuffer() {
  const nextCommitted = sanitizeDisplayText(`${browserAsrCommittedFinal} ${browserAsrLiveFinal}`);
  browserAsrCommittedFinal = nextCommitted;
  browserAsrLiveFinal = "";
  browserAsrInterim = "";
  browserAsrTranscript = browserAsrCommittedFinal;
  return browserAsrTranscript;
}

function startBrowserSpeechRecognition() {
  const RecognitionCtor = getSpeechRecognitionCtor();
  browserSpeechRecognitionAvailable = !!RecognitionCtor;
  browserAsrTranscript = "";
  browserAsrInterim = "";
  browserAsrCommittedFinal = "";
  browserAsrLiveFinal = "";
  if (!RecognitionCtor) return;
  try {
    const recognition = new RecognitionCtor();
    recognition.lang = "en-US";
    recognition.continuous = true;
    recognition.interimResults = true;
    recognition.onresult = (event) => {
      let runFinalText = "";
      let runInterimText = "";
      for (let i = 0; i < event.results.length; i += 1) {
        const result = event.results[i];
        const transcript = sanitizeDisplayText(result?.[0]?.transcript || "");
        if (!transcript) continue;
        if (result.isFinal) {
          runFinalText += (runFinalText ? " " : "") + transcript;
        } else {
          runInterimText += (runInterimText ? " " : "") + transcript;
        }
      }
      browserAsrLiveFinal = runFinalText;
      browserAsrInterim = runInterimText;
      updateLiveTranscript(recomputeBrowserTranscript());
    };
    recognition.onerror = () => {};
    recognition.onend = () => {
      if (!asrRecording) return;
      finalizeBrowserSpeechRecognitionBuffer();
      updateLiveTranscript(getBrowserTranscriptText());
      try {
        recognition.start();
      } catch (err) {}
    };
    recognition.start();
    browserSpeechRecognition = recognition;
  } catch (err) {
    browserSpeechRecognition = null;
  }
}

function stopBrowserSpeechRecognition() {
  const recognition = browserSpeechRecognition;
  browserSpeechRecognition = null;
  finalizeBrowserSpeechRecognitionBuffer();
  if (!recognition) return;
  try {
    recognition.onend = null;
    recognition.stop();
  } catch (err) {}
}

function getBrowserTranscriptText() {
  return sanitizeDisplayText(`${browserAsrCommittedFinal} ${browserAsrLiveFinal} ${browserAsrInterim}`);
}

// ─── Bit Cards ────────────────────────────────────────────────────────────────

function generateBitId() {
  return "bit-" + Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
}

function getBitsText() {
  return bits.map((b) => b.content).join("\n\n");
}

function getSelectedBitsText() {
  if (selectedBits.length === 0) return getBitsText();
  return selectedBits.map((b) => b.content).join("\n\n");
}

function autoExpand(el) {
  el.style.height = "auto";
  el.style.height = el.scrollHeight + "px";
}

function notifyBitsChanged() {
  lastInputAt = Date.now();
  lastUserTypedAt = Date.now();
  performancePromptArmed = true;
  clearRehearsalAnalysis();
  setRehearsalStatus("Rehearsal: script changed, run analysis again");
  if (suppressGhostClearOnce) {
    suppressGhostClearOnce = false;
    scheduleIdleSuggestion();
    schedulePerformancePrompt();
    showStartPrompt(false);
    return;
  }
  setGhost("");
  scheduleIdleSuggestion();
  showStartPrompt(false);
  if (performanceRunning && !cancelPending) {
    cancelPending = true;
    showCancelPrompt(true);
    setStageStatus("Stage: canceled pending");
  }
  schedulePerformancePrompt();
}

// ─── Card helpers ─────────────────────────────────────────────────────────────

function makeHandle() {
  const handle = document.createElement("div");
  handle.className = "bit-card-handle";
  handle.title = "Drag to reorder";
  handle.setAttribute("aria-hidden", "true");
  handle.innerHTML =
    '<svg width="10" height="16" viewBox="0 0 10 16" fill="currentColor" aria-hidden="true">' +
    '<circle cx="3.5" cy="3"  r="1.5"/>' +
    '<circle cx="7.5" cy="3"  r="1.5"/>' +
    '<circle cx="3.5" cy="8"  r="1.5"/>' +
    '<circle cx="7.5" cy="8"  r="1.5"/>' +
    '<circle cx="3.5" cy="13" r="1.5"/>' +
    '<circle cx="7.5" cy="13" r="1.5"/>' +
    "</svg>";
  return handle;
}

function makeDeleteBtn(bit, card) {
  const delBtn = document.createElement("button");
  delBtn.className = "bit-card-delete";
  delBtn.type = "button";
  delBtn.title = bit.type === "segue" ? "Remove this transition" : "Remove this bit";
  delBtn.innerHTML = "&times;";
  delBtn.addEventListener("click", () => {
    if (bits.length === 1) {
      const ta = card.querySelector(".bit-card-textarea");
      if (ta) { ta.value = ""; bit.content = ""; autoExpand(ta); notifyBitsChanged(); }
      return;
    }
    bits = bits.filter((b) => b.id !== bit.id);
    renderBits(bits);
    notifyBitsChanged();
  });
  return delBtn;
}

function attachTextareaListeners(textarea, bit) {
  textarea.addEventListener("input", () => {
    bit.content = textarea.value;
    autoExpand(textarea);
    notifyBitsChanged();
  });
  textarea.addEventListener("keydown", (e) => {
    if (e.key === "Tab") {
      if (currentSuggestion) {
        e.preventDefault();
        const accepted = currentSuggestion;
        suppressGhostClearOnce = true;
        setSuggestion("");
        insertAtCaret(accepted + " ");
        if (currentDraftId) {
          fetch("/api/accept-suggestion", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ draft_id: currentDraftId, text: accepted }),
          });
        }
        refreshSuggestion();
      }
    } else if (e.key === "Escape") {
      if (currentSuggestion) {
        e.preventDefault();
        setSuggestion("");
      }
    } else if (e.key === "Enter") {
      if (!currentSuggestion && !suggestionInFlight) {
        suppressGhostClearOnce = true;
        refreshSuggestion();
      }
    }
  });
  textarea.addEventListener("dragover", (e) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
  });
  textarea.addEventListener("drop", (e) => {
    e.preventDefault();
    e.stopPropagation();
    textarea.focus();
    const dropText = e.dataTransfer.getData("text/plain");
    if (dropText) insertAtCaret(dropText + " ");
  });
}

function renderTagPills(container, bit, compact, onRemove) {
  container.innerHTML = "";
  if (!Array.isArray(bit.tags) || bit.tags.length === 0) return;
  bit.tags.forEach((tag, idx) => {
    const pill = document.createElement("span");
    pill.className = "bit-tag";
    pill.appendChild(document.createTextNode(tag));
    if (!compact && typeof onRemove === "function") {
      const rmBtn = document.createElement("button");
      rmBtn.className = "bit-tag-remove";
      rmBtn.type = "button";
      rmBtn.title = "Remove tag";
      rmBtn.textContent = "\u00d7";
      rmBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        bit.tags.splice(idx, 1);
        onRemove();
      });
      pill.appendChild(rmBtn);
    }
    container.appendChild(pill);
  });
}

function showTagInput(bit, footer, addTagBtn, onDone) {
  addTagBtn.style.display = "none";
  const input = document.createElement("input");
  input.className = "bit-card-tag-input";
  input.type = "text";
  input.placeholder = "Tag name\u2026";
  input.setAttribute("autocomplete", "off");
  input.setAttribute("maxlength", "32");
  footer.insertBefore(input, addTagBtn);
  input.focus();
  let committed = false;
  function commit() {
    if (committed) return;
    committed = true;
    const val = input.value.trim();
    if (val && !bit.tags.includes(val)) bit.tags.push(val);
    input.remove();
    addTagBtn.style.display = "";
    onDone();
  }
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") { e.preventDefault(); commit(); }
    if (e.key === "Escape") { committed = true; input.remove(); addTagBtn.style.display = ""; }
    e.stopPropagation();
  });
  input.addEventListener("blur", commit);
}

// ─── Bit card pulse (visual feedback on marker click) ─────────────────────────

function pulseCard(bitId) {
  if (!bitId) return;
  // Teleprompter highlight (performance mode)
  highlightTeleprompterBit(bitId);
  // Bit card flash (editing mode / fallback)
  if (!els.bitsContainer) return;
  const card = els.bitsContainer.querySelector(`.bit-card[data-id="${bitId}"]`);
  if (!card) return;
  card.scrollIntoView({ behavior: "smooth", block: "nearest" });
  card.classList.remove("bit-card--pulse");
  void card.offsetWidth;
  card.classList.add("bit-card--pulse");
  card.addEventListener("animationend", () => card.classList.remove("bit-card--pulse"), { once: true });
}

function pulseCardForMarker(marker) {
  // Resolve which bit the marker's time_range maps to via character offset in the concatenated script.
  const source = selectedBits.length > 0 ? selectedBits : bits;
  if (source.length === 0) return;

  // Use script_range char offset when available; otherwise fall back to proportional time mapping.
  let charOffset = null;
  if (marker.script_range && typeof marker.script_range.char_start === "number") {
    charOffset = marker.script_range.char_start;
  } else if (Array.isArray(marker.time_range) && marker.time_range.length >= 2) {
    const fullText = source.map((b) => b.content).join("\n\n");
    const totalDuration = lastRehearsalMarkers.reduce((max, m) =>
      Array.isArray(m.time_range) ? Math.max(max, m.time_range[1] || 0) : max, 0);
    if (totalDuration > 0) {
      charOffset = Math.floor((marker.time_range[0] / totalDuration) * fullText.length);
    }
  }

  if (charOffset === null) {
    // Fall back: pulse the first card
    pulseCard(source[0].id);
    return;
  }

  // Walk through bits to find which one owns this char offset
  let cursor = 0;
  for (const bit of source) {
    const end = cursor + bit.content.length;
    if (charOffset <= end) {
      pulseCard(bit.id);
      return;
    }
    cursor = end + 2; // +2 for the "\n\n" separator
  }
  // If offset exceeds total, pulse the last bit
  pulseCard(source[source.length - 1].id);
}

// ─── Performance state helpers ────────────────────────────────────────────────

function applyBitCardPerformanceState(card, bit) {
  if (appState === "ready_to_perform") {
    const isSelected = selectedBits.some((b) => b.id === bit.id);
    card.style.opacity = isSelected ? "" : "0.4";
    card.style.pointerEvents = isSelected ? "" : "none";
  } else {
    card.style.opacity = "";
    card.style.pointerEvents = "";
  }
}

function renderTeleprompter() {
  if (!els.teleprompterBody) return;
  els.teleprompterBody.innerHTML = "";
  const source = selectedBits.length > 0 ? selectedBits : bits;
  source.forEach((bit, idx) => {
    if (!bit.content.trim() && bit.type !== "segue") return;
    const block = document.createElement("div");
    block.className = bit.type === "segue"
      ? "teleprompter-bit teleprompter-bit--segue"
      : "teleprompter-bit";
    block.dataset.bitId = bit.id;

    if (bit.type !== "segue" && bit.title) {
      const label = document.createElement("div");
      label.className = "teleprompter-bit-label";
      label.textContent = bit.title || `Bit ${idx + 1}`;
      block.appendChild(label);
    }
    const text = document.createElement("p");
    text.className = "teleprompter-bit-text";
    text.textContent = bit.content;
    block.appendChild(text);
    els.teleprompterBody.appendChild(block);
  });
}

function highlightTeleprompterBit(bitId) {
  if (!els.teleprompterBody) return;
  // Remove existing highlights
  els.teleprompterBody.querySelectorAll(".teleprompter-bit--active").forEach((el) => {
    el.classList.remove("teleprompter-bit--active");
  });
  if (!bitId) return;
  const block = els.teleprompterBody.querySelector(`.teleprompter-bit[data-bit-id="${bitId}"]`);
  if (!block) return;
  block.classList.add("teleprompter-bit--active");
  block.scrollIntoView({ behavior: "smooth", block: "center" });
}

function setTeleprompterRecording(isRecording) {
  if (!els.teleprompterBody) return;
  els.teleprompterBody.classList.toggle("teleprompter-body--recording", isRecording);
}

function setAppState(newState) {
  appState = newState;
  const isPerforming = newState === "ready_to_perform";

  // ── Left panel: swap editor ↔ teleprompter ──────────────────────────────────
  if (els.editorWrap) els.editorWrap.classList.toggle("hidden", isPerforming);
  if (els.teleprompterWrap) els.teleprompterWrap.classList.toggle("hidden", !isPerforming);

  if (isPerforming) {
    renderTeleprompter();
  } else {
    setTeleprompterRecording(false);
    highlightTeleprompterBit(null);
  }

  // ── Right panel ─────────────────────────────────────────────────────────────
  if (els.reviewPanel) els.reviewPanel.classList.toggle("review-panel--disabled", !isPerforming);
  if (els.reviewPanelOverlay) els.reviewPanelOverlay.classList.toggle("hidden", isPerforming);

  // Buttons inside the review panel that should only be active in ready_to_perform
  const reviewBtns = [els.asrBtn, els.analyzeRehearsalBtn, els.uploadRehearsalBtn];
  reviewBtns.forEach((b) => { if (b) b.disabled = !isPerforming; });

  // ── Bit card opacity (still applied to the hidden editor, keeps state consistent) ──
  const cards = els.bitsContainer?.querySelectorAll(".bit-card");
  if (cards) {
    cards.forEach((card) => {
      const bitId = card.dataset.id;
      const bit = bits.find((b) => b.id === bitId);
      if (bit) applyBitCardPerformanceState(card, bit);
    });
  }
}

// ─── Card factory ─────────────────────────────────────────────────────────────

function createBitCard(bit) {
  const card = document.createElement("div");
  card.dataset.id = bit.id;

  // ── Segue card (simple, no title/tags/collapse) ──────────────────────────
  if (bit.type === "segue") {
    card.className = "bit-card bit-card--segue";
    const textarea = document.createElement("textarea");
    textarea.className = "bit-card-textarea";
    textarea.value = bit.content;
    textarea.placeholder = "Add a transition line\u2026";
    textarea.rows = 1;
    attachTextareaListeners(textarea, bit);

    const segueCheckbox = document.createElement("input");
    segueCheckbox.type = "checkbox";
    segueCheckbox.className = "bit-card-checkbox bit-card-checkbox--segue";
    segueCheckbox.title = "Select for performance";
    segueCheckbox.checked = bit.selectedForPerformance || false;
    segueCheckbox.addEventListener("change", (e) => {
      e.stopPropagation();
      bit.selectedForPerformance = segueCheckbox.checked;
    });
    segueCheckbox.addEventListener("click", (e) => e.stopPropagation());

    card.appendChild(makeHandle());
    card.appendChild(makeDeleteBtn(bit, card));
    card.appendChild(segueCheckbox);
    card.appendChild(textarea);
    applyBitCardPerformanceState(card, bit);
    return card;
  }

  // ── Bit card (header + collapsible body + tags footer) ───────────────────
  card.className = "bit-card";
  if (bit.isCollapsed) card.classList.add("bit-card--collapsed");

  // Header ─────────────────────────────────────────────────────────────────
  const header = document.createElement("div");
  header.className = "bit-card-header";

  // Row 1: drag-handle zone + checkbox + title + compact tags + chevron
  const headerMain = document.createElement("div");
  headerMain.className = "bit-card-header-main";

  const checkbox = document.createElement("input");
  checkbox.type = "checkbox";
  checkbox.className = "bit-card-checkbox";
  checkbox.title = "Select for performance";
  checkbox.checked = bit.selectedForPerformance || false;
  checkbox.addEventListener("change", (e) => {
    e.stopPropagation();
    bit.selectedForPerformance = checkbox.checked;
  });
  checkbox.addEventListener("click", (e) => e.stopPropagation());

  const titleInput = document.createElement("input");
  titleInput.className = "bit-card-title";
  titleInput.type = "text";
  titleInput.value = bit.title || "";
  titleInput.placeholder = "Untitled bit\u2026";
  titleInput.addEventListener("input", () => { bit.title = titleInput.value; });
  titleInput.addEventListener("click", (e) => e.stopPropagation());
  titleInput.addEventListener("keydown", (e) => e.stopPropagation());

  const tagsCompact = document.createElement("div");
  tagsCompact.className = "bit-card-tags-compact";

  const chevron = document.createElement("button");
  chevron.className = "bit-card-chevron";
  chevron.type = "button";
  chevron.title = "Collapse / expand";
  chevron.setAttribute("aria-label", "Toggle collapse");
  chevron.innerHTML =
    '<svg width="12" height="12" viewBox="0 0 12 12" fill="none" aria-hidden="true">' +
    '<path d="M2 4l4 4 4-4" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>' +
    "</svg>";

  headerMain.appendChild(checkbox);
  headerMain.appendChild(titleInput);
  headerMain.appendChild(tagsCompact);
  headerMain.appendChild(chevron);

  // Row 2: content preview (visible only when collapsed)
  const preview = document.createElement("div");
  preview.className = "bit-card-preview";

  header.appendChild(headerMain);
  header.appendChild(preview);

  // Body (collapsible) ─────────────────────────────────────────────────────
  const body = document.createElement("div");
  body.className = "bit-card-body";

  const textarea = document.createElement("textarea");
  textarea.className = "bit-card-textarea";
  textarea.value = bit.content;
  textarea.placeholder = "Write your bit here\u2026";
  textarea.rows = 1;
  attachTextareaListeners(textarea, bit);

  // Inline AI suggestions — single action bar below the textarea ──────────────
  const suggestionsSection = document.createElement("div");
  suggestionsSection.className = "bit-card-suggestions";
  suggestionsSection.style.display = "none";

  const suggestionBar = document.createElement("div");
  suggestionBar.className = "bit-card-suggestion-bar";
  suggestionBar.style.display = "none";

  const suggestionText = document.createElement("span");
  suggestionText.className = "bit-card-suggestion-text";

  const acceptBtn = document.createElement("button");
  acceptBtn.type = "button";
  acceptBtn.className = "bit-card-suggestion-accept";
  acceptBtn.title = "Accept (Tab)";
  acceptBtn.setAttribute("aria-label", "Accept suggestion");
  acceptBtn.textContent = "✓";

  const rejectBtn = document.createElement("button");
  rejectBtn.type = "button";
  rejectBtn.className = "bit-card-suggestion-reject";
  rejectBtn.title = "Dismiss (Esc)";
  rejectBtn.setAttribute("aria-label", "Dismiss suggestion");
  rejectBtn.textContent = "✕";

  acceptBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    const accepted = currentSuggestion;
    if (accepted) {
      setSuggestion("");
      insertAtCaret(accepted + " ");
      if (currentDraftId) {
        fetch("/api/accept-suggestion", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ draft_id: currentDraftId, text: accepted }),
        });
      }
      refreshSuggestion();
    }
    textarea.focus();
  });

  rejectBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    setSuggestion("");
    textarea.focus();
  });

  suggestionBar.appendChild(suggestionText);
  suggestionBar.appendChild(acceptBtn);
  suggestionBar.appendChild(rejectBtn);
  suggestionsSection.appendChild(suggestionBar);

  // When this card's textarea gains focus, redirect els.ghost to this card's bar
  textarea.addEventListener("focus", () => {
    els.ghost = suggestionBar;
    // Clear any stale suggestion shown on this card from a previous focus
    setSuggestion("");
  });

  // Footer (tags + add-tag) ─────────────────────────────────────────────────
  const footer = document.createElement("div");
  footer.className = "bit-card-footer";

  const tagsFull = document.createElement("div");
  tagsFull.className = "bit-card-tags-full";

  const addTagBtn = document.createElement("button");
  addTagBtn.className = "bit-card-add-tag";
  addTagBtn.type = "button";
  addTagBtn.textContent = "+ Tag";
  addTagBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    showTagInput(bit, footer, addTagBtn, refreshTags);
  });

  footer.appendChild(tagsFull);
  footer.appendChild(addTagBtn);
  body.appendChild(textarea);
  body.appendChild(suggestionsSection);
  body.appendChild(footer);

  // Tag refresh ─────────────────────────────────────────────────────────────
  function refreshTags() {
    renderTagPills(tagsFull, bit, false, refreshTags);
    renderTagPills(tagsCompact, bit, true, null);
  }

  // Preview text (first line of content, shown only when collapsed) ─────────
  function refreshPreview() {
    const text = (bit.content || "").trim();
    if (!text) {
      preview.textContent = "Write your bit here\u2026";
      preview.style.opacity = "0.45";
    } else {
      const firstLine = text.split("\n")[0];
      preview.textContent =
        firstLine.length > 72 ? firstLine.slice(0, 72) + "\u2026" : firstLine;
      preview.style.opacity = "";
    }
  }
  textarea.addEventListener("input", refreshPreview);

  // Collapse toggle ─────────────────────────────────────────────────────────
  function toggleCollapse() {
    bit.isCollapsed = !bit.isCollapsed;
    card.classList.toggle("bit-card--collapsed", bit.isCollapsed);
    refreshTags();
    if (!bit.isCollapsed) {
      requestAnimationFrame(() => autoExpand(textarea));
    }
  }

  header.addEventListener("click", (e) => {
    if (
      e.target === titleInput ||
      e.target.closest(".bit-card-chevron") ||
      e.target.closest(".bit-card-add-tag")
    ) return;
    toggleCollapse();
  });

  chevron.addEventListener("click", (e) => {
    e.stopPropagation();
    toggleCollapse();
  });

  card.appendChild(makeHandle());
  card.appendChild(makeDeleteBtn(bit, card));
  card.appendChild(header);
  card.appendChild(body);

  refreshTags();
  refreshPreview();
  if (!bit.isCollapsed) requestAnimationFrame(() => autoExpand(textarea));
  applyBitCardPerformanceState(card, bit);
  return card;
}

function createGapZone(insertAfterIndex) {
  const zone = document.createElement("div");
  zone.className = "bit-gap";

  const btn = document.createElement("button");
  btn.className = "bit-gap-btn";
  btn.type = "button";
  btn.innerHTML = "&#10024; Add Transition";
  btn.addEventListener("click", () => {
    const newSegue = { id: generateBitId(), content: "", type: "segue" };
    bits.splice(insertAfterIndex + 1, 0, newSegue);
    renderBits(bits);
    setTimeout(() => {
      const newCard = els.bitsContainer?.querySelector(`.bit-card[data-id="${newSegue.id}"]`);
      if (newCard) {
        const ta = newCard.querySelector(".bit-card-textarea");
        if (ta) ta.focus();
      }
    }, 30);
  });

  zone.appendChild(btn);
  return zone;
}

function renderBits(bitsData) {
  if (!els.bitsContainer) return;
  els.bitsContainer.innerHTML = "";
  bitsData.forEach((bit, idx) => {
    const card = createBitCard(bit);
    els.bitsContainer.appendChild(card);
    const ta = card.querySelector(".bit-card-textarea");
    if (ta) autoExpand(ta);
    if (idx < bitsData.length - 1) {
      els.bitsContainer.appendChild(createGapZone(idx));
    }
  });
  initSortable();
}

function addBit(content) {
  const newBit = { id: generateBitId(), content: content || "", type: "bit", title: "", tags: [], isCollapsed: false };
  bits.push(newBit);
  renderBits(bits);
  if (!content) {
    setTimeout(() => {
      const newCard = els.bitsContainer?.querySelector(`.bit-card[data-id="${newBit.id}"]`);
      if (newCard) newCard.querySelector(".bit-card-textarea")?.focus();
    }, 30);
  }
  return newBit;
}

function loadBitsFromContent(content) {
  const raw = content || "";
  const parts = raw ? raw.split(/\n\n+/) : [""];
  bits = parts.map((p) => ({ id: generateBitId(), content: p, type: "bit", title: "", tags: [], isCollapsed: false }));
  if (bits.length === 0) bits = [{ id: generateBitId(), content: "", type: "bit", title: "", tags: [], isCollapsed: false }];
  renderBits(bits);
}

function clearBits() {
  bits = [{ id: generateBitId(), content: "", type: "bit", title: "", tags: [], isCollapsed: false }];
  renderBits(bits);
}

function initSortable() {
  if (!els.bitsContainer) return;
  if (_sortableInstance) {
    _sortableInstance.destroy();
    _sortableInstance = null;
  }
  if (typeof Sortable === "undefined") return;
  _sortableInstance = new Sortable(els.bitsContainer, {
    animation: 150,
    handle: ".bit-card-handle",
    draggable: ".bit-card",
    ghostClass: "bit-sortable-ghost",
    dragClass: "bit-card-dragging",
    onEnd() {
      // Re-sync bits[] from DOM order of .bit-card elements,
      // ignoring .bit-gap elements which would corrupt raw child indices.
      const cards = els.bitsContainer.querySelectorAll(".bit-card");
      const newOrder = Array.from(cards).map((c) => c.dataset.id);
      bits = newOrder.map((id) => bits.find((b) => b.id === id)).filter(Boolean);
      // Re-render to restore gap zones to correct positions after the drag.
      renderBits(bits);
    },
  });
}

// ─────────────────────────────────────────────────────────────────────────────

// Show (or hide) the single suggestion action bar in the currently focused card.
function setSuggestion(text) {
  currentSuggestion = text || "";
  const bar = els.ghost; // els.ghost is redirected to the active card's bar on textarea focus
  if (!bar) return;
  const textEl = bar.querySelector?.(".bit-card-suggestion-text");
  if (textEl) textEl.textContent = currentSuggestion;
  const visible = !!currentSuggestion;
  bar.style.display = visible ? "flex" : "none";
  const section = bar.closest?.(".bit-card-suggestions");
  if (section) section.style.display = visible ? "block" : "none";
}

// Legacy entry-point used throughout the codebase.
// Informational placeholder messages are silently dropped; actual suggestions are shown.
function setGhost(text) {
  const isInfo = !text || text.startsWith("Start with") || text.startsWith("Waiting");
  setSuggestion(isInfo ? "" : text);
}

function setFeedback(payload) {
  if (Array.isArray(lastMarkerFeedback?.items) && lastMarkerFeedback.items.length) {
    return;
  }
  if (payload.feedback) {
    els.feedbackText.textContent = sanitizeDisplayText(payload.feedback);
  }
  if (Array.isArray(payload.matched_segments)) {
    els.antiExamples.innerHTML = "";
    payload.matched_segments.forEach((seg) => {
      const wrap = document.createElement("div");
      wrap.className = "segment-card";
      const title = document.createElement("div");
      title.className = "segment-title";
      title.textContent = sanitizeDisplayText(seg.segment);
      wrap.appendChild(title);
      if (Array.isArray(seg.examples)) {
        const list = document.createElement("div");
        list.className = "chips";
        seg.examples.forEach((ex) => {
          const chip = document.createElement("span");
          chip.textContent = sanitizeDisplayText(ex);
          list.appendChild(chip);
        });
        wrap.appendChild(list);
      }
      els.antiExamples.appendChild(wrap);
    });
  }
}

function revokeFeedbackDownloadUrl() {
  if (lastFeedbackDownloadUrl) {
    try {
      URL.revokeObjectURL(lastFeedbackDownloadUrl);
    } catch (err) {}
    lastFeedbackDownloadUrl = null;
  }
}

function buildFeedbackDownloadText() {
  const lines = [];
  const summary = sanitizeDisplayText(lastMarkerFeedback?.overall_summary || lastMarkerFeedback?.summary || "");
  const headline = sanitizeDisplayText(lastMarkerFeedback?.headline || "");
  const transcript = sanitizeDisplayText(currentTranscript || "");
  const script = sanitizeDisplayText(lastAnalyzedScript || getSelectedBitsText() || getBitsText() || "");
  if (headline) lines.push(headline);
  if (summary && summary !== headline) lines.push(summary);
  if (script) {
    lines.push("", "Script", script);
  }
  if (transcript) {
    lines.push("", "Transcript", transcript);
  }
  const notes = Array.isArray(lastFocusNotes) ? lastFocusNotes : [];
  const utteranceById = new Map((Array.isArray(lastUtterances) ? lastUtterances : []).map((utt) => [String(utt?.id || ""), utt]));
  if (notes.length) {
    lines.push("", "All feedback");
    notes.forEach((note, index) => {
      const utt = utteranceById.get(String(note?.utterance_id || ""));
      const quote = sanitizeDisplayText(utt?.text || note?.quote || note?.focus_span || "");
      const advice = sanitizeDisplayText(note?.advice || "");
      const why = sanitizeDisplayText(note?.why || "");
      const next = sanitizeDisplayText(note?.try_next || "");
      lines.push("", `Note ${index + 1}`);
      if (quote) lines.push(`Line: ${quote}`);
      if (advice) lines.push(`Advice: ${advice}`);
      if (why) lines.push(`Why: ${why}`);
      if (next) lines.push(`Next pass: ${next}`);
    });
  }
  const refs = includeVideoReferenceEnabled() ? (Array.isArray(lastVideoReferences) ? lastVideoReferences : []) : [];
  const matches = includeVideoReferenceEnabled() ? (Array.isArray(lastComedianMatches) ? lastComedianMatches : []) : [];
  if (refs.length || matches.length) {
    lines.push("", "Who to watch");
    if (refs.length) {
      refs.slice(0, 5).forEach((ref, index) => {
        const performer = sanitizeDisplayText(ref?.performer_name || ref?.title || "Stand-up reference");
        const watch = sanitizeDisplayText(ref?.watch_hint || ref?.learn_goal || ref?.reason || "");
        const action = sanitizeDisplayText(ref?.copy_action || "");
        lines.push("", `Recommendation ${index + 1}: ${performer}`);
        if (watch) lines.push(`What to study: ${watch}`);
        if (action) lines.push(`Try after watching: ${action}`);
      });
    } else {
      matches.slice(0, 5).forEach((item, index) => {
        const performer = sanitizeDisplayText(item?.performer_name || item?.name || "Recommended comedian");
        const why = sanitizeDisplayText(item?.reason || item?.why || item?.summary || "");
        const watch = sanitizeDisplayText(item?.watch_hint || item?.learn_goal || "");
        lines.push("", `Recommendation ${index + 1}: ${performer}`);
        if (why) lines.push(`Why this fit: ${why}`);
        if (watch) lines.push(`What to study: ${watch}`);
      });
    }
  }
  return lines.join("\n").trim();
}

function updateFeedbackDownloadLink() {
  if (!els.downloadFeedbackLink) return;
  revokeFeedbackDownloadUrl();
  const text = buildFeedbackDownloadText();
  if (!text) {
    els.downloadFeedbackLink.classList.add("hidden");
    els.downloadFeedbackLink.removeAttribute("href");
    return;
  }
  const blob = new Blob([text], { type: "text/plain;charset=utf-8" });
  lastFeedbackDownloadUrl = URL.createObjectURL(blob);
  els.downloadFeedbackLink.href = lastFeedbackDownloadUrl;
  els.downloadFeedbackLink.download = `rehearsal_feedback_${new Date().toISOString().slice(0, 10)}.txt`;
  els.downloadFeedbackLink.classList.remove("hidden");
}

function renderRehearsalFeedback(markerFeedback) {
  if (!els.feedbackText || !els.antiExamples) return;
  const payload = markerFeedback && typeof markerFeedback === "object" ? markerFeedback : {};
  const sections = Array.isArray(payload.priority_dimensions) ? payload.priority_dimensions : [];
  const plan = Array.isArray(payload.next_rehearsal_plan) ? payload.next_rehearsal_plan : [];
  const items = Array.isArray(payload.items) ? payload.items : [];
  const notes = Array.isArray(lastFocusNotes) ? lastFocusNotes : [];
  const utteranceById = new Map((Array.isArray(lastUtterances) ? lastUtterances : []).map((utt) => [String(utt?.id || ""), utt]));

  els.feedbackText.innerHTML = "";
  els.antiExamples.innerHTML = "";

  const appendParagraph = (text, className = "feedback-paragraph") => {
    const clean = sanitizeDisplayText(text || "");
    if (!clean) return;
    const p = document.createElement("p");
    p.className = className;
    p.textContent = clean;
    els.feedbackText.appendChild(p);
  };

  const appendSpacer = () => {
    const spacer = document.createElement("div");
    spacer.className = "feedback-spacer";
    els.feedbackText.appendChild(spacer);
  };

  const headline = sanitizeDisplayText(payload.headline || "");
  if (headline) appendParagraph(headline, "feedback-paragraph feedback-lead");

  const summary = sanitizeDisplayText(payload.overall_summary || payload.summary || "");
  if (summary && summary !== headline) appendParagraph(summary);

  if (notes.length) {
    notes.forEach((note, index) => {
      const utt = utteranceById.get(String(note?.utterance_id || ""));
      const quote = sanitizeDisplayText(utt?.text || note?.quote || note?.focus_span || "");
      const advice = sanitizeDisplayText(note?.advice || "");
      const why = sanitizeDisplayText(note?.why || "");
      const next = sanitizeDisplayText(note?.try_next || "");
      if (index > 0 || headline || summary) appendSpacer();
      appendParagraph(`Feedback ${index + 1}`, "feedback-paragraph feedback-subhead");
      if (quote) appendParagraph(`Line: ${quote}`);
      if (advice) appendParagraph(`Advice: ${advice}`);
      if (why) appendParagraph(`Why this matters: ${why}`);
      if (next) appendParagraph(`Try next: ${next}`);
    });
  } else if (items.length) {
    items.forEach((item, index) => {
      if (index > 0 || headline || summary) appendSpacer();
      appendParagraph(sanitizeDisplayText(item?.title || `Feedback ${index + 1}`), "feedback-paragraph feedback-subhead");
      [["Current read", item?.current_read], ["Why it matters", item?.why_it_matters], ["What to work on next", item?.coaching_priority || item?.next_rehearsal_goal]].forEach(([label, value]) => {
        const clean = sanitizeDisplayText(value || "");
        if (clean) appendParagraph(`${label}: ${clean}`);
      });
    });
  } else if (sections.length) {
    sections.forEach((section, index) => {
      if (index > 0 || headline || summary) appendSpacer();
      appendParagraph(sanitizeDisplayText(section.rubric_dimension || "Performance note"), "feedback-paragraph feedback-subhead");
      [["Current read", section.current_read], ["Why it matters", section.why_it_matters], ["What to work on next", section.what_to_work_on_next]].forEach(([label, value]) => {
        const clean = sanitizeDisplayText(value || "");
        if (clean) appendParagraph(`${label}: ${clean}`);
      });
    });
  }

  if (plan.length) {
    appendSpacer();
    appendParagraph("Next rehearsal focus", "feedback-paragraph feedback-subhead");
    plan.forEach((step, index) => {
      const clean = sanitizeDisplayText(step || "");
      if (clean) appendParagraph(`${index + 1}. ${clean}`);
    });
  }

  updateFeedbackDownloadLink();
}

function showError(message) {
  if (!els.errorBanner) return;
  els.errorBanner.textContent = message;
  els.errorBanner.classList.remove("hidden");
  setTimeout(() => {
    els.errorBanner.classList.add("hidden");
  }, 4000);
}

function setAsrStatus(text, recording) {
  if (els.asrStatus) {
    els.asrStatus.textContent = text;
  }
  if (els.asrBtn) {
    els.asrBtn.textContent = recording ? "Stop Voice Input" : "Start Voice Input";
    els.asrBtn.classList.toggle("recording", !!recording);
  }
  setTeleprompterRecording(!!recording);
}

function setTranscript(text) {
  currentTranscript = text || "";
  lastUtterances = [];
  lastFocusNotes = [];
  lastRehearsalMarkers = [];
  activeUtteranceId = null;
  activeMarkerId = null;
  performedScriptRange = null;
  selectedMarkerRange = null;
  renderTranscriptInline();
  renderFocusedNote(null, null);
  renderScriptHighlights("", null, null);
}

function updateLiveTranscript(text) {
  currentTranscript = text || "";
  renderTranscriptInline();
}

function setRehearsalStatus(text) {
  if (els.rehearsalStatus) {
    els.rehearsalStatus.textContent = text;
  }
}

function renderVideoDatasetStatus(payload) {
  lastVideoDatasetStatus = payload && typeof payload === "object" ? payload : null;
  if (!els.videoDatasetStatus) return;
  if (!lastVideoDatasetStatus) {
    els.videoDatasetStatus.textContent = "Dataset: unknown";
    return;
  }
  const status = sanitizeDisplayText(lastVideoDatasetStatus.status || "unknown");
  const processed = Number(lastVideoDatasetStatus.processed_files || 0);
  const pending = Number(lastVideoDatasetStatus.pending_files || 0);
  const failed = Number(lastVideoDatasetStatus.failed_files || 0);
  const referenceStatus = sanitizeDisplayText(lastVideoDatasetStatus.reference_status || "");
  const referenceFiles = Number(lastVideoDatasetStatus.reference_files || 0);
  const referenceSpans = Number(lastVideoDatasetStatus.reference_spans || 0);
  const root = sanitizeDisplayText(lastVideoDatasetStatus.dataset_root || "");
  let info = `Dataset: ${status} | processed ${processed}, pending ${pending}, failed ${failed}`;
  if (referenceStatus || referenceFiles || referenceSpans) {
    info += ` | references ${referenceStatus || "unknown"} (${referenceFiles} videos, ${referenceSpans} spans)`;
  }
  if (root) {
    info += ` | root: ${root}`;
  }
  if (lastVideoDatasetStatus.last_error && status === "error") {
    info += ` | ${sanitizeDisplayText(lastVideoDatasetStatus.last_error)}`;
  }
  els.videoDatasetStatus.textContent = info;
}

function isVideoDatasetReady(payload = lastVideoDatasetStatus) {
  if (!payload || typeof payload !== "object") return false;
  const status = String(payload.status || "").trim().toLowerCase();
  const pending = Number(payload.pending_files || 0);
  const referenceStatus = String(payload.reference_status || "").trim().toLowerCase();
  const referenceSpans = Number(payload.reference_spans || 0);
  if (referenceStatus === "ready" && referenceSpans > 0 && (status === "ready" || pending === 0)) return true;
  return status === "ready" && pending === 0;
}

function isVideoDatasetUsable(payload = lastVideoDatasetStatus) {
  if (!payload || typeof payload !== "object") return false;
  const status = String(payload.status || "").trim().toLowerCase();
  const processed = Number(payload.processed_files || 0);
  const pending = Number(payload.pending_files || 0);
  const total = processed + pending + Number(payload.failed_files || 0);
  const referenceStatus = String(payload.reference_status || "").trim().toLowerCase();
  const referenceFiles = Number(payload.reference_files || 0);
  const referenceSpans = Number(payload.reference_spans || 0);
  if ((referenceStatus === "ready" || referenceStatus === "indexing") && referenceSpans > 0) return true;
  if (referenceFiles >= 8) return true;
  if (status === "ready" && processed > 0) return true;
  if (processed >= 8) return true;
  if (total > 0 && processed / total >= 0.1) return true;
  return false;
}

function renderDetectedStyle(styleDetection) {
  if (!els.detectedStyle) return;
  const styleName = sanitizeDisplayText(styleDetection?.effective_style || styleDetection?.label || "");
  if (!styleName) {
    els.detectedStyle.textContent = "pending";
    return;
  }
  const confidence = Number(styleDetection?.confidence || 0);
  const confidencePct = Number.isFinite(confidence) ? Math.round(confidence * 100) : 0;
  els.detectedStyle.textContent = `${styleName} (${confidencePct}%)`;
}

function buildMarkerLookup(markers) {
  const lookup = new Map();
  (Array.isArray(markers) ? markers : []).forEach((marker) => {
    const markerId = String(marker?.id || "").trim();
    if (markerId) lookup.set(markerId, marker);
  });
  return lookup;
}

function markerTag(marker) {
  if (!marker) return "marker";
  return `${formatTimeRange(marker.time_range)} | ${sanitizeDisplayText(marker.issue_type || "delivery")}`;
}

function filterReferencesByMarker(items, markerId) {
  const refs = Array.isArray(items) ? items : [];
  if (!markerId) return refs;
  const filtered = refs.filter((item) => Array.isArray(item?.marker_ids) && item.marker_ids.some((id) => String(id) === String(markerId)));
  return filtered.length ? filtered : refs;
}

function getLinkedReferencesForMarker(markerId) {
  if (!markerId) return [];
  return (Array.isArray(lastVideoReferences) ? lastVideoReferences : []).filter((item) => Array.isArray(item?.marker_ids) && item.marker_ids.some((id) => String(id) === String(markerId)));
}

function createVideoReferenceCard(item, markerLookup, options = {}) {
  const tagName = options.tagName || "li";
  const card = document.createElement(tagName);
  card.className = "video-reference-item";
  if (options.compact) card.classList.add("video-reference-compact");
  const activeId = options.activeMarkerId || null;
  if (activeId && Array.isArray(item?.marker_ids) && item.marker_ids.some((id) => String(id) === String(activeId))) {
    card.classList.add("video-reference-active");
  }

  const isFocused = !!options.focusedMode;
  if (isFocused) {
    card.classList.add("video-reference-focused");
  }

  if (!isFocused) {
    const titleEl = document.createElement("div");
    titleEl.className = "video-reference-title";
    titleEl.textContent = sanitizeDisplayText(item.title || "Stand-up reference");
    card.appendChild(titleEl);
  } else {
    // In focused mode show a compact "chip" header: filename + time range
    const titleChip = document.createElement("div");
    titleChip.className = "vfg-title-chip";
    titleChip.textContent = sanitizeDisplayText(item.title || "Reference clip");
    card.appendChild(titleChip);
  }

  const previewUrl = String(item.preview_url || "").trim();
  let previewVideoUrl = String(item.preview_video_url || "").trim();
  const sourceUrl = String(item.source_url || "").trim();
  const watchUrl = String(item.watch_url || "").trim();
  const clipStart = Number(item.start_sec || 0);
  const clipEnd = Number(item.end_sec || 0);
  let attemptedSourceFallback = false;

  const previewWrap = document.createElement("div");
  previewWrap.className = "video-preview-wrap";
  if (isFocused) previewWrap.classList.add("video-preview-wrap--focused");
  const previewStatus = document.createElement("div");
  previewStatus.className = "video-reference-meta";
  previewWrap.appendChild(previewStatus);
  const video = document.createElement("video");
  video.className = "video-preview-player";
  video.muted = true;
  video.loop = true;
  video.autoplay = true;
  video.playsInline = true;
  video.controls = true;
  video.preload = "metadata";
  previewWrap.appendChild(video);
  card.appendChild(previewWrap);

  const setPreviewStatus = (message) => {
    previewStatus.textContent = sanitizeDisplayText(message || "");
  };

  const showExternalWatchOnly = () => {
    setPreviewStatus(watchUrl ? "Playable preview unavailable here. Open the source video for this clip." : "Preview unavailable for this clip.");
    try {
      video.pause();
      video.removeAttribute("src");
      video.load();
    } catch (err) {}
  };

  const setVideoSrc = (url, opts = {}) => {
    const clean = String(url || "").trim();
    if (!clean) return;
    const isSourceFallback = !!opts.isSourceFallback;
    const clipStartFromUrl = Number(opts.clipStartSec || clipStart || 0);
    const clipEndFromUrl = Number(opts.clipEndSec || clipEnd || 0);
    try {
      video.pause();
      video.removeAttribute("src");
      video.load();
    } catch (err) {}
    video.src = clean;
    video.load();
    setPreviewStatus(isSourceFallback ? "Showing the source video from the suggested moment." : "");
    video.addEventListener("loadeddata", () => {
      if (isSourceFallback && clipStartFromUrl > 0) {
        try { video.currentTime = clipStartFromUrl; } catch (err) {}
      }
      if (isSourceFallback && clipEndFromUrl > clipStartFromUrl) {
        const stopAtEnd = () => {
          if (video.currentTime >= clipEndFromUrl) {
            video.pause();
            video.currentTime = clipStartFromUrl;
          }
        };
        video.addEventListener("timeupdate", stopAtEnd);
      }
      video.play().catch(() => {});
    }, { once: true });
    video.addEventListener("error", () => {
      if (!isSourceFallback && sourceUrl && !attemptedSourceFallback) {
        attemptedSourceFallback = true;
        const fallbackUrl = `${sourceUrl}#t=${Math.max(0, clipStart).toFixed(2)},${Math.max(clipStart + 0.1, clipEnd).toFixed(2)}`;
        setPreviewStatus("Preview clip unavailable. Falling back to the source video segment.");
        setVideoSrc(fallbackUrl || sourceUrl, { isSourceFallback: true, clipStartSec: clipStart, clipEndSec: clipEnd });
        return;
      }
      setPreviewStatus(`Video unavailable: ${clean}`);
    }, { once: true });
  };

  if (previewVideoUrl) {
    setVideoSrc(previewVideoUrl);
  } else if (previewUrl.startsWith("/api/video-dataset/preview")) {
    fetch(previewUrl).then(async (res) => {
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data?.error || "preview request failed");
      previewVideoUrl = String(data?.preview_url || "").trim();
      if (!previewVideoUrl) throw new Error("preview url missing");
      setVideoSrc(previewVideoUrl);
    }).catch((err) => {
      if (sourceUrl && !attemptedSourceFallback) {
        attemptedSourceFallback = true;
        const fallbackUrl = `${sourceUrl}#t=${Math.max(0, clipStart).toFixed(2)},${Math.max(clipStart + 0.1, clipEnd).toFixed(2)}`;
        setPreviewStatus("Preview clip unavailable. Falling back to the source video segment.");
        setVideoSrc(fallbackUrl || sourceUrl, { isSourceFallback: true, clipStartSec: clipStart, clipEndSec: clipEnd });
        return;
      }
      setPreviewStatus(err?.message || "Preview unavailable for this clip.");
    });
  } else if (previewUrl.startsWith("/static/")) {
    setVideoSrc(previewUrl);
  } else if (sourceUrl) {
    attemptedSourceFallback = true;
    const fallbackUrl = `${sourceUrl}#t=${Math.max(0, clipStart).toFixed(2)},${Math.max(clipStart + 0.1, clipEnd).toFixed(2)}`;
    setVideoSrc(fallbackUrl || sourceUrl, { isSourceFallback: true, clipStartSec: clipStart, clipEndSec: clipEnd });
  } else if (watchUrl) {
    showExternalWatchOnly();
  }

  const performer = sanitizeDisplayText(item.performer_name || "");
  if (!isFocused && performer && !/^unknown comedian$/i.test(performer)) {
    const performerMeta = document.createElement("div");
    performerMeta.className = "video-reference-meta";
    performerMeta.textContent = `Comedian: ${performer}`;
    card.appendChild(performerMeta);
  }

  if (watchUrl) {
    const watchLink = document.createElement("a");
    watchLink.className = "video-reference-meta";
    watchLink.href = watchUrl;
    watchLink.target = "_blank";
    watchLink.rel = "noopener noreferrer";
    watchLink.textContent = "Open source video";
    card.appendChild(watchLink);
  }

  const reasonText = [sanitizeDisplayText(item.matched_reason || item.learn_goal || item.reason || ""), sanitizeDisplayText(item.comparison || "")].filter(Boolean).join(" ");

  if (isFocused) {
    // ── Focused mode: render contextual notes as an inline field guide ──────
    const fieldNotes = [
      { label: "Why this clip", value: reasonText },
      { label: "What to watch", value: sanitizeDisplayText(item.watch_hint || "") },
      { label: "Apply to your line", value: sanitizeDisplayText(item.user_focus_span || "") },
    ].filter((n) => n.value);

    if (fieldNotes.length) {
      const guide = document.createElement("div");
      guide.className = "video-field-guide";
      fieldNotes.forEach(({ label, value }) => {
        const row = document.createElement("div");
        row.className = "vfg-row";
        const lbl = document.createElement("span");
        lbl.className = "vfg-label";
        lbl.textContent = label;
        const val = document.createElement("span");
        val.className = "vfg-value";
        val.textContent = value;
        row.appendChild(lbl);
        row.appendChild(val);
        guide.appendChild(row);
      });
      card.appendChild(guide);
    }
    // imitation_steps and rehearsal_drill are intentionally NOT rendered here;
    // they are added to the Practice Steps column by renderFocusedNote.
  } else {
    // ── Standard mode: flat meta text blocks ─────────────────────────────────
    if (reasonText) {
      const reason = document.createElement("div");
      reason.className = "video-reference-meta";
      reason.textContent = reasonText;
      card.appendChild(reason);
    }

    [
      ["What to watch", sanitizeDisplayText(item.watch_hint || "")],
      ["Where to apply it in your line", sanitizeDisplayText(item.user_focus_span || "")],
      ["Moment to study in the demo", sanitizeDisplayText(item.demo_focus_span || "")],
      ["Supports this coaching advice", sanitizeDisplayText(item.supports_advice || "")],
      ["Try this on your line", sanitizeDisplayText(item.copy_action || "")],
      ["After watching", sanitizeDisplayText(item.rehearsal_drill || "")],
    ].forEach(([label, value]) => {
      if (!value) return;
      const block = document.createElement("div");
      block.className = "video-reference-meta";
      block.textContent = `${label}: ${value}`;
      card.appendChild(block);
    });

    const steps = Array.isArray(item.imitation_steps) ? item.imitation_steps : [];
    if (steps.length) {
      const stepList = document.createElement("ol");
      stepList.className = "feedback-list";
      steps.slice(0, 4).forEach((step) => {
        const li = document.createElement("li");
        li.textContent = sanitizeDisplayText(step || "");
        stepList.appendChild(li);
      });
      card.appendChild(stepList);
    }
  }

  const markerIds = Array.isArray(item.marker_ids) ? item.marker_ids : [];
  if (markerIds.length) {
    const markerMeta = document.createElement("div");
    markerMeta.className = "video-reference-meta";
    const labels = markerIds.map((id) => markerTag(markerLookup.get(String(id)))).filter(Boolean);
    markerMeta.textContent = `linked marker: ${labels.join(" / ")}`;
    card.appendChild(markerMeta);
  }

  return card;
}

function renderVideoReferences(items, enabled = true, markerId = null) {
  if (!els.videoReferenceList) return;
  els.videoReferenceList.innerHTML = "";
  const references = filterReferencesByMarker(items, markerId);
  if (!enabled) {
    const li = document.createElement("li");
    li.className = "video-reference-empty";
    li.textContent = "Comedian recommendations are turned off.";
    els.videoReferenceList.appendChild(li);
    return;
  }
  if (!references.length) {
    const li = document.createElement("li");
    li.className = "video-reference-empty";
    li.textContent = isVideoDatasetUsable()
      ? "No comedian recommendations yet."
      : "The reference library is still warming up. Comedian recommendations will appear after more clips are indexed.";
    els.videoReferenceList.appendChild(li);
    return;
  }
  const seen = new Set();
  references.forEach((item, index) => {
    const performer = sanitizeDisplayText(item?.performer_name || item?.comedian || "");
    const title = sanitizeDisplayText(item?.title || "");
    const reason = sanitizeDisplayText(item?.watch_hint || item?.learn_goal || item?.matched_reason || item?.reason || item?.supports_advice || "");
    const watchUrl = String(item?.watch_url || "").trim();
    const key = `${performer}||${title}`.toLowerCase();
    if (key && seen.has(key)) return;
    if (key) seen.add(key);
    const li = document.createElement("li");
    li.className = "video-reference-card";
    const name = performer || title || `Recommendation ${index + 1}`;
    const lines = [];
    lines.push(`Watch: ${name}`);
    if (title && title !== performer) lines.push(`Clip: ${title}`);
    if (reason) lines.push(`Why this one: ${reason}`);
    const apply = sanitizeDisplayText(item?.copy_action || item?.rehearsal_drill || item?.comparison || "");
    if (apply) lines.push(`What to learn: ${apply}`);
    li.innerHTML = lines.map((line) => `<div class="video-reference-meta">${escapeHtml(line)}</div>`).join("");
    if (watchUrl) {
      const link = document.createElement("a");
      link.className = "video-reference-meta";
      link.href = watchUrl;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.textContent = "Open source video";
      li.appendChild(link);
    }
    els.videoReferenceList.appendChild(li);
  });
}

function loadStylePresets() {
  try {
    const parsed = JSON.parse(localStorage.getItem(STYLE_PRESET_STORAGE_KEY) || "[]");
    const list = Array.isArray(parsed) ? parsed.filter(Boolean) : [];
    const merged = [...new Set([...DEFAULT_STYLE_PRESETS, ...list])];
    return merged;
  } catch (err) {
    return [...DEFAULT_STYLE_PRESETS];
  }
}

function persistStylePresets(presets) {
  localStorage.setItem(STYLE_PRESET_STORAGE_KEY, JSON.stringify(presets));
}

async function fetchStylePresetsFromServer(nickname) {
  if (!nickname) return [];
  try {
    const res = await fetch(`/api/style-presets?nickname=${encodeURIComponent(nickname)}`);
    const data = await res.json();
    if (!res.ok || !Array.isArray(data.items)) return [];
    return data.items
      .map((item) => String(item.name || "").trim().toLowerCase())
      .filter(Boolean);
  } catch (err) {
    return [];
  }
}

async function saveStylePresetToServer(nickname, name) {
  if (!nickname || !name) return false;
  try {
    const res = await fetch("/api/style-presets", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ nickname, name }),
    });
    return res.ok;
  } catch (err) {
    return false;
  }
}

async function syncStylePresetsForNickname() {
  const nickname = els.nicknameInput?.value.trim() || "";
  const localPresets = loadStylePresets();
  const remotePresets = await fetchStylePresetsFromServer(nickname);
  const merged = [...new Set([...DEFAULT_STYLE_PRESETS, ...remotePresets, ...localPresets])];
  persistStylePresets(merged);
  renderStylePresetOptions(merged, els.stylePresetSelect?.value || merged[0] || "");
  return merged;
}

async function fetchVideoDatasetStatus() {
  try {
    const res = await fetch("/api/video-dataset/status");
    const data = await res.json();
    if (!res.ok) {
      renderVideoDatasetStatus({ status: "error", last_error: data.error || "status fetch failed" });
      return;
    }
    renderVideoDatasetStatus(data);
  } catch (err) {
    renderVideoDatasetStatus({ status: "error", last_error: "network error" });
  }
}

function renderStylePresetOptions(presets, selected = "") {
  if (!els.stylePresetSelect) return;
  els.stylePresetSelect.innerHTML = "";
  presets.forEach((preset) => {
    const opt = document.createElement("option");
    opt.value = preset;
    opt.textContent = preset;
    if (preset === selected) {
      opt.selected = true;
    }
    els.stylePresetSelect.appendChild(opt);
  });
}

async function saveStylePreset() {
  if (!els.stylePresetInput || !els.stylePresetSelect) return;
  const raw = els.stylePresetInput.value.trim().toLowerCase();
  if (!raw) return;
  const presets = loadStylePresets();
  if (!presets.includes(raw)) {
    presets.push(raw);
    persistStylePresets(presets);
  }
  const nickname = els.nicknameInput?.value.trim() || "";
  if (nickname) {
    await saveStylePresetToServer(nickname, raw);
  }
  const synced = await syncStylePresetsForNickname();
  renderStylePresetOptions(synced, raw);
  els.stylePresetInput.value = "";
}

function setLastRehearsalAudio(blob, filename, sourceLabel) {
  lastRehearsalBlob = blob;
  lastRehearsalFilename = filename || "rehearsal.webm";
  setRehearsalStatus(`Rehearsal: ${sourceLabel} ready for analysis`);
}

function clearAudioPlayer(player) {
  if (!player) return;
  player.pause();
  player.removeAttribute("src");
  player.classList.add("hidden");
}

function clearRehearsalAnalysis(clearStatus = false) {
  lastRehearsalMarkers = [];
  lastUtterances = [];
  lastFocusNotes = [];
  lastVideoReferences = [];
  lastMarkerFeedback = { summary: "", items: [] };
  lastProcessMap = null;
  lastAnalyzedScript = "";
  performedScriptRange = null;
  selectedMarkerRange = null;
  activeMarkerId = null;
  activeUtteranceId = null;
  activeMarkerRow = null;
  stopAllMarkerAudio();
  renderDetectedStyle(null);
  renderTranscriptInline();
  renderFocusedNote(null, null);
  renderScriptHighlights("", null, null);
  renderRehearsalFeedback(lastMarkerFeedback);
  renderStudyVideos(lastVideoReferences, includeVideoReferenceEnabled());
  if (els.markerTimeline) els.markerTimeline.innerHTML = "";
  if (clearStatus) {
    setRehearsalStatus("Rehearsal: no focused notes yet");
  }
}

function normalizeRange(range, textLength) {
  if (!range) return null;
  const start = Math.max(0, Math.min(textLength, Number(range.char_start || 0)));
  const end = Math.max(start, Math.min(textLength, Number(range.char_end || start)));
  if (end <= start) return null;
  return { start, end };
}

function renderScriptHighlights(scriptText, performedRange, markerRange) {
  if (!els.analysisScript) return;
  const text = scriptText || "";
  if (!text) {
    els.analysisScript.classList.add("hidden");
    els.analysisScript.innerHTML = "";
    return;
  }
  const normalizedPerformed = normalizeRange(performedRange, text.length);
  const normalizedMarker = normalizeRange(markerRange, text.length);
  if (!normalizedPerformed && !normalizedMarker) {
    els.analysisScript.classList.add("hidden");
    els.analysisScript.innerHTML = "";
    return;
  }
  const points = new Set([0, text.length]);
  if (normalizedPerformed) { points.add(normalizedPerformed.start); points.add(normalizedPerformed.end); }
  if (normalizedMarker) { points.add(normalizedMarker.start); points.add(normalizedMarker.end); }
  const sortedPoints = Array.from(points).sort((a, b) => a - b);
  els.analysisScript.classList.remove("hidden");
  els.analysisScript.innerHTML = "";
  for (let i = 0; i < sortedPoints.length - 1; i += 1) {
    const start = sortedPoints[i];
    const end = sortedPoints[i + 1];
    const chunk = text.slice(start, end);
    if (!chunk) continue;
    const span = document.createElement("span");
    span.textContent = chunk;
    if (normalizedPerformed && start >= normalizedPerformed.start && end <= normalizedPerformed.end) span.classList.add("performed-range");
    if (normalizedMarker && start >= normalizedMarker.start && end <= normalizedMarker.end) span.classList.add("marker-range");
    els.analysisScript.appendChild(span);
  }
}

function formatTimeRange(range) {
  if (!Array.isArray(range) || range.length < 2) return "--:--";
  const start = Math.max(0, Number(range[0] || 0));
  const end = Math.max(0, Number(range[1] || 0));
  return `${start.toFixed(1)}s - ${end.toFixed(1)}s`;
}

// Currently open marker row element (for collapse-on-reclick)
let activeMarkerRow = null;

function stopAllMarkerAudio() {
  document.querySelectorAll(".marker-audio-player").forEach((a) => {
    a.pause();
    a.currentTime = 0;
  });
}

// ─── Client-side audio trimmer (Web Audio API) ────────────────────────────────
// Returns a blob: URL containing only the [startSec, endSec] slice of sourceUrl.
async function trimAudioToBlob(sourceUrl, startSec, endSec) {
  const ctx = new (window.AudioContext || window.webkitAudioContext)();
  try {
    const resp = await fetch(sourceUrl);
    const arrayBuf = await resp.arrayBuffer();
    const decoded = await ctx.decodeAudioData(arrayBuf);

    const sampleRate = decoded.sampleRate;
    const channels = decoded.numberOfChannels;
    const clipStart = Math.max(0, startSec);
    const clipEnd = Math.min(decoded.duration, endSec);
    const clipLen = Math.max(0, clipEnd - clipStart);
    if (clipLen <= 0) return sourceUrl;

    const startSample = Math.floor(clipStart * sampleRate);
    const clipSamples = Math.floor(clipLen * sampleRate);

    // Build a new offline-rendered buffer with just the slice
    const offCtx = new OfflineAudioContext(channels, clipSamples, sampleRate);
    const clipBuf = offCtx.createBuffer(channels, clipSamples, sampleRate);
    for (let ch = 0; ch < channels; ch++) {
      const src = decoded.getChannelData(ch).subarray(startSample, startSample + clipSamples);
      clipBuf.getChannelData(ch).set(src);
    }

    // Encode to WAV
    const wavBuf = encodeWav(clipBuf);
    return URL.createObjectURL(new Blob([wavBuf], { type: "audio/wav" }));
  } catch {
    return sourceUrl; // fallback to full file on any error
  } finally {
    ctx.close().catch(() => {});
  }
}

// Minimal PCM WAV encoder (16-bit, interleaved)
function encodeWav(audioBuffer) {
  const numCh = audioBuffer.numberOfChannels;
  const sampleRate = audioBuffer.sampleRate;
  const numSamples = audioBuffer.length;
  const bitsPerSample = 16;
  const bytesPerSample = bitsPerSample / 8;
  const blockAlign = numCh * bytesPerSample;
  const byteRate = sampleRate * blockAlign;
  const dataSize = numSamples * blockAlign;
  const buffer = new ArrayBuffer(44 + dataSize);
  const view = new DataView(buffer);

  function writeStr(offset, str) {
    for (let i = 0; i < str.length; i++) view.setUint8(offset + i, str.charCodeAt(i));
  }

  writeStr(0, "RIFF");
  view.setUint32(4, 36 + dataSize, true);
  writeStr(8, "WAVE");
  writeStr(12, "fmt ");
  view.setUint32(16, 16, true);        // PCM chunk size
  view.setUint16(20, 1, true);         // PCM format
  view.setUint16(22, numCh, true);
  view.setUint32(24, sampleRate, true);
  view.setUint32(28, byteRate, true);
  view.setUint16(32, blockAlign, true);
  view.setUint16(34, bitsPerSample, true);
  writeStr(36, "data");
  view.setUint32(40, dataSize, true);

  // Interleave channels
  let offset = 44;
  for (let i = 0; i < numSamples; i++) {
    for (let ch = 0; ch < numCh; ch++) {
      const s = Math.max(-1, Math.min(1, audioBuffer.getChannelData(ch)[i]));
      view.setInt16(offset, s < 0 ? s * 0x8000 : s * 0x7FFF, true);
      offset += 2;
    }
  }
  return buffer;
}

// ─── Compact player builder ───────────────────────────────────────────────────
// trimRange: [startSec, endSec] — if provided, audio is sliced client-side before playback
function buildCompactPlayer(label, url, otherPlayerGetter, trimRange) {
  const wrap = document.createElement("div");
  wrap.className = "cmp-player";

  const lbl = document.createElement("div");
  lbl.className = "cmp-player-label";
  lbl.textContent = label;
  wrap.appendChild(lbl);

  if (!url) {
    const na = document.createElement("div");
    na.className = "cmp-player-na";
    na.textContent = "Not available";
    wrap.appendChild(na);
    return { wrap, audio: null };
  }

  const audio = document.createElement("audio");
  audio.className = "marker-audio-player";
  audio.preload = "none";

  // Custom controls
  const controls = document.createElement("div");
  controls.className = "cmp-controls";

  const playBtn = document.createElement("button");
  playBtn.type = "button";
  playBtn.className = "cmp-play-btn";
  playBtn.innerHTML = "&#9654;"; // ▶

  const bar = document.createElement("div");
  bar.className = "cmp-progress-bar";
  const fill = document.createElement("div");
  fill.className = "cmp-progress-fill";
  bar.appendChild(fill);

  const time = document.createElement("span");
  time.className = "cmp-time";
  time.textContent = "0:00";

  // Loading indicator (shown while trimming)
  const loadingDot = document.createElement("span");
  loadingDot.className = "cmp-loading";
  loadingDot.textContent = "…";
  loadingDot.style.display = "none";

  controls.appendChild(playBtn);
  controls.appendChild(bar);
  controls.appendChild(time);
  controls.appendChild(loadingDot);
  wrap.appendChild(controls);

  let resolvedUrl = url;
  let urlReady = false;

  async function ensureUrl() {
    if (urlReady) return;
    if (trimRange && Array.isArray(trimRange) && trimRange.length >= 2) {
      loadingDot.style.display = "";
      playBtn.disabled = true;
      resolvedUrl = await trimAudioToBlob(url, trimRange[0], trimRange[1]);
      loadingDot.style.display = "none";
      playBtn.disabled = false;
    }
    audio.src = resolvedUrl;
    urlReady = true;
  }

  // Play / pause toggle
  playBtn.addEventListener("click", async () => {
    await ensureUrl();
    const other = otherPlayerGetter?.();
    if (audio.paused) {
      if (other && !other.paused) other.pause();
      audio.play().catch(() => {});
    } else {
      audio.pause();
    }
  });

  audio.addEventListener("play", () => { playBtn.innerHTML = "&#9646;&#9646;"; });
  audio.addEventListener("pause", () => { playBtn.innerHTML = "&#9654;"; });
  audio.addEventListener("ended", () => { playBtn.innerHTML = "&#9654;"; fill.style.width = "0%"; });

  audio.addEventListener("timeupdate", () => {
    if (!audio.duration) return;
    const pct = (audio.currentTime / audio.duration) * 100;
    fill.style.width = `${pct}%`;
    const s = Math.floor(audio.currentTime);
    time.textContent = `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
  });

  // Seek on click (only after url resolved)
  bar.addEventListener("click", (e) => {
    if (!urlReady || !audio.duration) return;
    const rect = bar.getBoundingClientRect();
    const ratio = (e.clientX - rect.left) / rect.width;
    audio.currentTime = ratio * audio.duration;
  });

  wrap.appendChild(audio);
  return { wrap, audio };
}

function clearTranscriptHighlight() {
  els.transcriptView?.querySelectorAll(".transcript-utterance--active").forEach((el) => {
    el.classList.remove("transcript-utterance--active");
  });
}

function highlightFocusedUtterance(utteranceId) {
  clearTranscriptHighlight();
  if (!utteranceId || !els.transcriptView) return;
  const span = els.transcriptView.querySelector(`.transcript-utterance[data-utterance-id="${String(utteranceId)}"]`);
  if (!span) return;
  span.classList.add("transcript-utterance--active");
  span.scrollIntoView({ behavior: "smooth", block: "nearest", inline: "nearest" });
}

function renderFocusedNote(note, utterance = null) {
  if (!els.focusedNoteCard) return;
  const hasNote = !!(note && utterance);
  els.focusedNoteCard.classList.toggle("hidden", !hasNote);
  if (els.focusedNoteEmpty) {
    els.focusedNoteEmpty.classList.toggle("hidden", hasNote);
  }
  if (!hasNote) {
    if (els.focusedNoteQuote) els.focusedNoteQuote.textContent = "";
    if (els.focusedNoteMeta) els.focusedNoteMeta.textContent = "";
    if (els.focusedNotePlayers) els.focusedNotePlayers.innerHTML = "";
    if (els.focusedNoteAdvice) els.focusedNoteAdvice.textContent = "";
    if (els.focusedNoteWhy) els.focusedNoteWhy.textContent = "";
    if (els.focusedNoteNext) els.focusedNoteNext.innerHTML = "";
    if (els.focusedNoteVideo) els.focusedNoteVideo.innerHTML = "";
    return;
  }

  if (els.focusedNoteQuote) {
    els.focusedNoteQuote.textContent = sanitizeDisplayText(utterance.text || note.quote || "");
  }
  if (els.focusedNoteMeta) {
    const tags = Array.isArray(note.delivery_tags) ? note.delivery_tags.filter(Boolean) : [];
    const parts = [
      sanitizeDisplayText(note.comedy_function || ""),
      sanitizeDisplayText(note.joke_role || note.focus_type || ""),
      formatTimeRange(note.time_range || utterance.time_range || null),
      tags.length ? tags.join(" • ") : "",
    ].filter(Boolean);
    els.focusedNoteMeta.textContent = parts.join(" · ");
  }
  if (els.focusedNotePlayers) {
    els.focusedNotePlayers.innerHTML = "";
    const evidence = buildCompactPlayer(
      "Your delivery",
      note.evidence_audio_url || "",
      () => null,
      Array.isArray(note.time_range) ? note.time_range : null
    );
    els.focusedNotePlayers.appendChild(evidence.wrap || evidence);
  }
  if (els.focusedNoteAdvice) {
    els.focusedNoteAdvice.textContent = sanitizeDisplayText(note.advice || "");
  }
  if (els.focusedNoteWhy) {
    const why = sanitizeDisplayText(note.why || "");
    els.focusedNoteWhy.textContent = why || "";
  }
  if (els.focusedNoteNext) {
    els.focusedNoteNext.innerHTML = "";
    const raw = note.try_next || "";
    let steps = [];
    if (Array.isArray(raw)) {
      steps = raw.map((s) => sanitizeDisplayText(String(s))).filter(Boolean);
    } else if (typeof raw === "string" && raw.trim()) {
      steps = sanitizeDisplayText(raw)
        .split(/(?<=[.!?])\s+|\n+/)
        .map((s) => s.trim().replace(/[.!?]$/, "").trim())
        .filter(Boolean);
      if (steps.length === 0) steps = [sanitizeDisplayText(raw.trim())];
    }
    steps.forEach((step, i) => {
      const item = document.createElement("label");
      item.className = "fdn-step-item";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.className = "fdn-step-checkbox";
      cb.id = `fdn-step-${i}`;
      cb.addEventListener("change", () => {
        text.classList.toggle("fdn-step-text--done", cb.checked);
      });
      const text = document.createElement("span");
      text.className = "fdn-step-text";
      text.textContent = step;
      item.appendChild(cb);
      item.appendChild(text);
      els.focusedNoteNext.appendChild(item);
    });
    if (!steps.length) {
      els.focusedNoteNext.textContent = "";
    }
  }
  if (els.focusedNoteVideo) {
    els.focusedNoteVideo.innerHTML = "";
    const enabled = !!els.includeVideoRef?.checked;
    const ref = enabled ? (note.video_reference || (Array.isArray(note.video_references) ? note.video_references[0] : null)) : null;
    if (ref) {
      els.focusedNoteVideo.appendChild(
        createVideoReferenceCard(ref, new Map(), { tagName: "div", compact: true, focusedMode: true })
      );

      // Append video-specific practice steps to the Practice Steps column
      if (els.focusedNoteNext) {
        const videoSteps = [];
        if (ref.rehearsal_drill) videoSteps.push(sanitizeDisplayText(ref.rehearsal_drill));
        if (Array.isArray(ref.imitation_steps)) {
          ref.imitation_steps.forEach((s) => {
            const t = sanitizeDisplayText(String(s || ""));
            if (t) videoSteps.push(t);
          });
        }
        if (videoSteps.length) {
          const sep = document.createElement("div");
          sep.className = "fdn-steps-sep";
          sep.textContent = "From reference video:";
          els.focusedNoteNext.appendChild(sep);
          const existingCount = els.focusedNoteNext.querySelectorAll(".fdn-step-item").length;
          videoSteps.forEach((stepText, i) => {
            const stepItem = document.createElement("label");
            stepItem.className = "fdn-step-item";
            const cb = document.createElement("input");
            cb.type = "checkbox";
            cb.className = "fdn-step-checkbox";
            cb.id = `fdn-vstep-${existingCount + i}`;
            const stepLabel = document.createElement("span");
            stepLabel.className = "fdn-step-text";
            stepLabel.textContent = stepText;
            cb.addEventListener("change", () => {
              stepLabel.classList.toggle("fdn-step-text--done", cb.checked);
            });
            stepItem.appendChild(cb);
            stepItem.appendChild(stepLabel);
            els.focusedNoteNext.appendChild(stepItem);
          });
        }
      }
    } else if (enabled) {
      const empty = document.createElement("div");
      empty.className = "focused-note-video-empty";
      empty.textContent = isVideoDatasetUsable()
        ? "No supporting video clip matched this delivery moment yet."
        : "Reference clips will appear after more of the dataset is indexed.";
      els.focusedNoteVideo.appendChild(empty);
    }
  }
}

function activateFocusNote(note, utterance) {
  const utteranceId = String(utterance?.id || "").trim() || null;
  const sameSelection = activeUtteranceId && activeUtteranceId === utteranceId;
  stopAllMarkerAudio();
  if (sameSelection) {
    activeUtteranceId = null;
    activeMarkerId = null;
    selectedMarkerRange = null;
    clearTranscriptHighlight();
    renderFocusedNote(null, null);
    renderScriptHighlights(lastAnalyzedScript || getSelectedBitsText(), performedScriptRange, null);
    renderRehearsalFeedback(lastMarkerFeedback);
    renderStudyVideos(lastVideoReferences, !!els.includeVideoRef?.checked);
    return;
  }
  activeUtteranceId = utteranceId;
  activeMarkerId = note?.marker_id ? String(note.marker_id) : null;
  selectedMarkerRange = utterance?.script_range || null;
  highlightFocusedUtterance(utteranceId);
  renderFocusedNote(note, utterance);
  renderScriptHighlights(lastAnalyzedScript || getSelectedBitsText(), performedScriptRange, selectedMarkerRange);
  renderRehearsalFeedback(lastMarkerFeedback);
  renderStudyVideos(lastVideoReferences, !!els.includeVideoRef?.checked);
}

function findMissedScriptSections(script, coveredRanges) {
  if (!script || !coveredRanges.length) return [];
  // Merge overlapping ranges first
  const sorted = [...coveredRanges].sort((a, b) => a.start - b.start);
  const merged = [];
  for (const r of sorted) {
    if (merged.length && r.start <= merged[merged.length - 1].end) {
      merged[merged.length - 1].end = Math.max(merged[merged.length - 1].end, r.end);
    } else {
      merged.push({ ...r });
    }
  }
  const missed = [];
  let cursor = 0;
  for (const r of merged) {
    if (r.start > cursor) {
      const text = script.slice(cursor, r.start).trim();
      if (text) missed.push(text);
    }
    cursor = Math.max(cursor, r.end);
  }
  if (cursor < script.length) {
    const text = script.slice(cursor).trim();
    if (text) missed.push(text);
  }
  return missed;
}

function renderTranscriptInline() {
  if (!els.transcriptView) return;
  activeMarkerRow = null;
  els.transcriptView.innerHTML = "";

  const utterances = Array.isArray(lastUtterances) && lastUtterances.length
    ? lastUtterances : [];
  const fallbackText = currentTranscript || "";

  const hasContent = !!(utterances.length || fallbackText);
  if (els.saveTranscriptBtn) {
    els.saveTranscriptBtn.classList.toggle("hidden", !hasContent);
  }

  if (!utterances.length && !fallbackText) {
    els.transcriptView.textContent = "Your recorded transcript will appear here…";
    els.transcriptView.classList.add("transcript-empty");
    return;
  }
  els.transcriptView.classList.remove("transcript-empty");

  // Fallback: no utterances yet, show plain raw transcript
  if (!utterances.length) {
    const block = document.createElement("div");
    block.className = "transcript-text-block";
    block.textContent = fallbackText;
    els.transcriptView.appendChild(block);
    return;
  }

  const noteByUtteranceId = new Map();
  (Array.isArray(lastFocusNotes) ? lastFocusNotes : []).forEach((note) => {
    const uttId = String(note?.utterance_id || "").trim();
    if (uttId) noteByUtteranceId.set(uttId, note);
  });

  // Build covered char-ranges from utterances that align to the script
  const coveredRanges = utterances
    .filter((u) => u?.script_range?.char_start != null)
    .map((u) => ({
      start: Number(u.script_range.char_start),
      end: Number(u.script_range.char_end ?? u.script_range.char_start),
    }))
    .filter((r) => r.end > r.start);

  // ── Spoken utterances (green = matched, yellow = improv) ──────────────────
  const textBlock = document.createElement("div");
  textBlock.className = "transcript-text-block transcript-text-block--utterances";

  utterances.forEach((utt) => {
    const uttId = String(utt?.id || "").trim();
    const hasScriptRange = utt?.script_range?.char_start != null;
    const note = noteByUtteranceId.get(uttId);

    const span = document.createElement("span");
    span.className = "transcript-utterance";
    span.dataset.utteranceId = uttId;
    span.textContent = `${sanitizeDisplayText(utt?.text || "")} `;

    if (hasScriptRange) {
      span.classList.add("transcript-utterance--matched");   // green: matches script
    } else {
      span.classList.add("transcript-utterance--improv");    // yellow: improvised
    }

    if (note) {
      // focus-note click still works on top of the colour class
      span.classList.add("transcript-utterance--focus");
      span.addEventListener("click", () => activateFocusNote(note, utt));
    }

    textBlock.appendChild(span);
  });

  els.transcriptView.appendChild(textBlock);

  // ── Missed script sections (gray-red) ─────────────────────────────────────
  const script = lastAnalyzedScript || getSelectedBitsText() || getBitsText() || "";
  if (script && coveredRanges.length > 0) {
    const missed = findMissedScriptSections(script, coveredRanges);
    if (missed.length) {
      const missedWrap = document.createElement("div");
      missedWrap.className = "transcript-missed-wrap";
      const missedLabel = document.createElement("div");
      missedLabel.className = "transcript-missed-label";
      missedLabel.textContent = "Missed from script:";
      missedWrap.appendChild(missedLabel);
      const missedBlock = document.createElement("div");
      missedBlock.className = "transcript-text-block transcript-text-block--utterances";
      missed.forEach((text) => {
        const span = document.createElement("span");
        span.className = "transcript-utterance transcript-utterance--missed"; // gray-red
        span.textContent = text + " ";
        missedBlock.appendChild(span);
      });
      missedWrap.appendChild(missedBlock);
      els.transcriptView.appendChild(missedWrap);
    }
  }

  if (activeUtteranceId) {
    highlightFocusedUtterance(activeUtteranceId);
  }
}

async function transcribeAsrBlob(blob) {
  if (!blob || blob.size === 0) {
    setAsrStatus("Mic: no audio captured", false);
    return;
  }
  setAsrStatus("Mic: transcribing...", false);
  const browserTranscript = getBrowserTranscriptText();
  const formData = new FormData();
  formData.append("audio", blob, "voice-input.webm");
  if (browserTranscript) {
    formData.append("fallback_text", browserTranscript);
  }
  try {
    const res = await fetch("/api/asr/transcribe", {
      method: "POST",
      body: formData,
    });
    const data = await res.json();
    if (!res.ok) {
      if ((data.error || "").includes("OPENAI_API_KEY is not set") && browserTranscript) {
        setTranscript(browserTranscript);
        setAsrStatus("Mic: transcript ready (browser)", false);
        return;
      }
      showError(data.error || "ASR failed.");
      setAsrStatus("Mic: idle", false);
      return;
    }
    const transcript = sanitizeDisplayText(data.text || browserTranscript || "");
    if (!transcript) {
      setAsrStatus("Mic: empty result", false);
      return;
    }
    setTranscript(transcript);
    const sourceLabel = data.source === "browser-fallback" ? "Mic: transcript ready (browser)" : "Mic: transcript ready";
    setAsrStatus(sourceLabel, false);
  } catch (err) {
    if (browserTranscript) {
      setTranscript(browserTranscript);
      setAsrStatus("Mic: transcript ready (browser)", false);
      return;
    }
    showError("Network error while transcribing audio.");
    setAsrStatus("Mic: idle", false);
  }
}

async function toggleAsrRecording() {
  if (!els.asrBtn) return;
  if (asrRecording && asrRecorder) {
    setAsrStatus("Mic: stopping...", false);
    stopBrowserSpeechRecognition();
    asrRecorder.stop();
    return;
  }
  if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
    showError("Your browser does not support microphone recording.");
    return;
  }
  try {
    asrStream = await navigator.mediaDevices.getUserMedia({ audio: true });
    asrChunks = [];
    browserAsrTranscript = "";
    browserAsrInterim = "";
    browserAsrCommittedFinal = "";
    browserAsrLiveFinal = "";
    setTranscript(""); // clear previous transcript when a new recording starts
    asrRecorder = new MediaRecorder(asrStream);
    asrRecorder.ondataavailable = (e) => {
      if (e.data && e.data.size > 0) {
        asrChunks.push(e.data);
      }
    };
    asrRecorder.onstop = async () => {
      asrRecording = false;
      if (asrStream) {
        asrStream.getTracks().forEach((track) => track.stop());
      }
      const blob = new Blob(asrChunks, { type: asrRecorder.mimeType || "audio/webm" });
      setLastRehearsalAudio(blob, "voice-input.webm", "microphone take");
      asrRecorder = null;
      asrStream = null;
      asrChunks = [];
      await transcribeAsrBlob(blob);
    };
    asrRecorder.start();
    asrRecording = true;
    startBrowserSpeechRecognition();
    const recordingLabel = browserSpeechRecognitionAvailable ? "Mic: recording... (browser transcript enabled)" : "Mic: recording...";
    setAsrStatus(recordingLabel, true);
  } catch (err) {
    showError("Cannot access microphone.");
    setAsrStatus("Mic: idle", false);
  }
}

function includeVideoReferenceEnabled() {
  return els.includeVideoRef ? !!els.includeVideoRef.checked : true;
}

async function analyzeRehearsalTake() {
  const script = getSelectedBitsText().trim() || getBitsText().trim() || "";
  if (!lastRehearsalBlob) {
    showError("Record or upload an audio take first.");
    return;
  }
  setRehearsalStatus("Rehearsal: analyzing...");
  const formData = new FormData();
  if (currentDraftId) formData.append("draft_id", String(currentDraftId));
  formData.append("script", script);
  const includeVideoReference = includeVideoReferenceEnabled();
  formData.append("include_video_dataset", includeVideoReference ? "1" : "0");
  if (currentTranscript && currentTranscript.trim()) {
    formData.append("transcript_text", currentTranscript.trim());
  }
  formData.append("audio", lastRehearsalBlob, lastRehearsalFilename || "rehearsal.webm");
  try {
    const res = await fetch("/api/rehearsal/analyze", { method: "POST", body: formData });
    const data = await res.json();
    if (!res.ok) {
      showError(data.error || "Rehearsal analysis failed.");
      setRehearsalStatus("Rehearsal: analysis failed");
      return;
    }
    applyRehearsalAnalysis(data, "http");
  } catch (err) {
    showError("Network error while analyzing rehearsal.");
    setRehearsalStatus("Rehearsal: analysis failed");
  }
}

function applyRehearsalAnalysis(payload, source = "http") {
  performedScriptRange = payload?.alignment?.performed_script_range || null;
  selectedMarkerRange = null;
  activeMarkerId = null;
  activeUtteranceId = null;
  lastAnalyzedScript = sanitizeDisplayText(payload?.script || getSelectedBitsText() || getBitsText());
  lastRehearsalMarkers = Array.isArray(payload?.markers) ? payload.markers : [];
  lastUtterances = Array.isArray(payload?.utterances) ? payload.utterances : [];
  lastFocusNotes = Array.isArray(payload?.focus_notes) ? payload.focus_notes : [];
  lastVideoReferences = Array.isArray(payload?.video_references) ? payload.video_references : [];
  lastComedianMatches = Array.isArray(payload?.comedian_matches) ? payload.comedian_matches : [];
  lastMarkerFeedback = payload?.feedback && typeof payload.feedback === "object" ? payload.feedback : { summary: "", items: [] };
  lastProcessMap = payload?.process_map && typeof payload.process_map === "object" ? payload.process_map : null;
  renderDetectedStyle(payload?.style_detection || null);
  renderVideoDatasetStatus(payload?.video_dataset_status || null);
  renderTranscriptInline();
  renderFocusedNote(null, null);
  renderScriptHighlights(lastAnalyzedScript, performedScriptRange, null);
  renderRehearsalFeedback(lastMarkerFeedback);
  renderStudyVideos(lastVideoReferences, includeVideoReferenceEnabled());
  const suffix = source === "sse" ? " (live)" : "";
  const styleLabel = sanitizeDisplayText(payload?.style_detection?.effective_style || "general");
  setRehearsalStatus(`Rehearsal: ${lastFocusNotes.length} focused notes ready | style: ${styleLabel}${suffix}`);
}

function setStageStatus(text) {
  if (!els.stageStatus) return;
  els.stageStatus.textContent = text;
}

function clearStageLog() {
  if (!els.stageLog) return;
  els.stageLog.innerHTML = "";
}

function clearProcessMapLog() {
  if (!els.processMapLog) return;
  els.processMapLog.innerHTML = "";
}

function renderStudyVideos(videoReferences = [], enabled = true) {
  if (els.processMapTitle) {
    els.processMapTitle.textContent = "Who to Watch";
  }
  if (els.stageStatus) {
    els.stageStatus.textContent = enabled
      ? "Here are a few comedians worth studying before your next pass."
      : "Reference suggestions are turned off for this pass.";
  }
  if (!els.processMapLog) return;
  els.processMapLog.innerHTML = "";
  const refs = Array.isArray(videoReferences) ? videoReferences : [];
  const matches = Array.isArray(lastComedianMatches) ? lastComedianMatches : [];

  const addCard = (title, bodyLines = []) => {
    const card = document.createElement("div");
    card.className = "stage-line";
    const heading = document.createElement("div");
    heading.className = "stage-role coach";
    heading.textContent = sanitizeDisplayText(title || "Recommendation");
    card.appendChild(heading);
    const body = document.createElement("div");
    body.textContent = bodyLines.filter(Boolean).join(" ");
    card.appendChild(body);
    els.processMapLog.appendChild(card);
  };

  if (!enabled) {
    addCard("Who to Watch", ["Reference suggestions are off for this pass."]);
    return;
  }

  if (refs.length) {
    refs.slice(0, 3).forEach((ref, index) => {
      const performer = sanitizeDisplayText(ref?.performer_name || ref?.title || `Recommendation ${index + 1}`);
      const why = sanitizeDisplayText(ref?.reason || ref?.learn_goal || "");
      const watch = sanitizeDisplayText(ref?.watch_hint || "");
      const action = sanitizeDisplayText(ref?.copy_action || "");
      addCard(performer, [why, watch, action].filter(Boolean));
    });
    return;
  }

  if (matches.length) {
    matches.slice(0, 3).forEach((item, index) => {
      const performer = sanitizeDisplayText(item?.performer_name || item?.name || `Recommendation ${index + 1}`);
      const why = sanitizeDisplayText(item?.reason || item?.why || item?.summary || "");
      const watch = sanitizeDisplayText(item?.watch_hint || item?.learn_goal || item?.what_to_study || "");
      addCard(performer, [why, watch].filter(Boolean));
    });
    return;
  }

  addCard("Who to Watch", ["Recommendations will appear here after this pass is analyzed."]);
}

function renderProcessMap(processMap, markerId = null) {
  renderStudyVideos(lastVideoReferences, !!els.includeVideoRef?.checked);
}

function appendStageEvent(role, text, score) {
  if (!els.stageLog) return;
  const line = document.createElement("div");
  line.className = "stage-line";
  const roleEl = document.createElement("div");
  roleEl.className = `stage-role ${role}`;
  roleEl.textContent = role;
  const body = document.createElement("div");
  body.textContent = text;
  line.appendChild(roleEl);
  line.appendChild(body);
  if (typeof score === "number") {
    const scoreEl = document.createElement("div");
    scoreEl.className = "stage-score";
    scoreEl.textContent = `Score: ${score}/10`;
    line.appendChild(scoreEl);
  }
  els.stageLog.appendChild(line);
}

function showStartPrompt(show) {
  if (!els.stagePrompt) return;
  if (show) {
    els.stagePrompt.classList.remove("hidden");
    setStageStatus("Stage: ready");
  } else {
    els.stagePrompt.classList.add("hidden");
  }
}

function showCancelPrompt(show) {
  if (!els.cancelPrompt) return;
  if (show) {
    els.cancelPrompt.classList.remove("hidden");
    setStageStatus("Stage: cancel pending");
  } else {
    els.cancelPrompt.classList.add("hidden");
  }
}

function setReviewLoading(loading) {
  if (!els.reviewLoader) return;
  if (loading) {
    els.reviewLoader.classList.remove("hidden");
  } else {
    els.reviewLoader.classList.add("hidden");
  }
}

function stopReviewTyping() {
  if (reviewTypingTimer) {
    clearInterval(reviewTypingTimer);
    reviewTypingTimer = null;
  }
}

function resetReview() {
  if (!els.reviewText) return;
  stopReviewTyping();
  els.reviewText.textContent = "";
  if (els.reviewAudio) {
    els.reviewAudio.pause();
    els.reviewAudio.removeAttribute("src");
    els.reviewAudio.classList.add("hidden");
  }
}

function typeReviewText(text) {
  if (!els.reviewText) return;
  stopReviewTyping();
  let index = 0;
  els.reviewText.textContent = "";
  reviewTypingTimer = setInterval(() => {
    index += 1;
    els.reviewText.textContent = text.slice(0, index);
    if (index >= text.length) {
      clearInterval(reviewTypingTimer);
      reviewTypingTimer = null;
    }
  }, 18);
}

async function requestPerformanceReview(performanceId) {
  if (!performanceId) return;
  resetReview();
  setReviewLoading(true);
  try {
    const res = await fetch("/api/performance/review", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ performance_id: performanceId }),
    });
    const data = await res.json();
    if (!res.ok) {
      setReviewLoading(false);
      showError(data.error || "Review failed.");
      return;
    }
    setReviewLoading(false);
    const text = sanitizeDisplayText(data.text || "");
    if (data.audio_url && els.reviewAudio) {
      els.reviewAudio.src = data.audio_url;
      els.reviewAudio.classList.remove("hidden");
      els.reviewAudio.currentTime = 0;
      els.reviewAudio.play().catch(() => {});
    }
    if (text) {
      typeReviewText(text);
    }
  } catch (err) {
    setReviewLoading(false);
    showError("Network error while generating review.");
  }
}

function schedulePerformancePrompt() {
  if (performanceIdleTimer) clearTimeout(performanceIdleTimer);
  if (!performancePromptArmed) return;
  performanceIdleTimer = setTimeout(() => {
    if (!performancePromptArmed) return;
    if (performanceRunning || cancelPending) return;
    if (!getBitsText().trim()) return;
    performancePromptArmed = false;
    showStartPrompt(true);
  }, performanceIdleMs);
}

async function ensureDraftExists(defaultTitle = "Auto Draft") {
  if (currentDraftId) return currentDraftId;
  const nickname = els.nicknameInput.value.trim();
  if (!nickname) {
    showError("Please enter a nickname.");
    return null;
  }
  try {
    const res = await fetch("/api/drafts", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ nickname, title: defaultTitle }),
    });
    const data = await res.json();
    if (!res.ok) {
      showError(data.error || "Failed to create draft.");
      return null;
    }
    currentDraftId = data.draft_id;
    connectSSE();
    await saveCurrentDraft();
    await loadDrafts();
    els.draftSelect.value = String(currentDraftId);
    return currentDraftId;
  } catch (err) {
    showError("Network error while creating draft.");
    return null;
  }
}

async function saveDraftNow() {
  const draftId = await ensureDraftExists("Quick Save");
  if (!draftId) return;
  await saveCurrentDraft(true);
}

async function startPerformance() {
  if (performanceRunning) return;
  const text = (getSelectedBitsText().trim() || getBitsText().trim());
  if (!text) return;
  if (!currentDraftId) {
    const ensuredId = await ensureDraftExists();
    if (!ensuredId) return;
  }
  performancePromptArmed = false;
  resetReview();
  showStartPrompt(false);
  showCancelPrompt(false);
  cancelPending = false;
  clearStageLog();
  setStageStatus("Stage: performing...");
  performanceRunning = true;
  activePerformanceId = null;
  try {
    const res = await fetch("/api/performance/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ draft_id: currentDraftId, text }),
    });
    const data = await res.json();
    if (!res.ok) {
      performanceRunning = false;
      showError(data.error || "Performance failed.");
      return;
    }
    if (!activePerformanceId) {
      activePerformanceId = data.performance_id;
    }
    if (pendingCancelAction) {
      const action = pendingCancelAction;
      pendingCancelAction = null;
      cancelPerformance(action === "save");
    }
  } catch (err) {
    performanceRunning = false;
    showError("Network error while starting performance.");
  }
}

async function cancelPerformance(save) {
  if (!activePerformanceId) {
    pendingCancelAction = save ? "save" : "discard";
    showCancelPrompt(false);
    cancelPending = false;
    setStageStatus("Stage: idle");
    return;
  }
  try {
    await fetch("/api/performance/cancel", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ performance_id: activePerformanceId, save }),
    });
  } catch (err) {
    showError("Network error while canceling performance.");
  } finally {
    performanceRunning = false;
    cancelPending = false;
    if (activePerformanceId) {
      ignoredPerformanceIds.add(activePerformanceId);
    }
    activePerformanceId = null;
    showCancelPrompt(false);
    setStageStatus("Stage: idle");
    resetReview();
    schedulePerformancePrompt();
  }
}

function sanitizeSuggestionText(text) {
  if (!text) return "";
  return text
    .replace(/^\s*\d+\s*[\.\)\:\-]\s*/g, "")
    .replace(/\*\*/g, "")
    .trim();
}


function addListItem(listEl, text) {
  if (!listEl) return;
  const cleanText = sanitizeSuggestionText(text);
  if (!cleanText) return;
  const li = document.createElement("li");
  li.textContent = cleanText;
  li.draggable = true;
  li.addEventListener("dragstart", (e) => {
    if (document.activeElement && document.activeElement.blur) document.activeElement.blur();
    e.dataTransfer.effectAllowed = "copy";
    e.dataTransfer.setData("text/plain", cleanText);
  });
  listEl.prepend(li);
}

function insertAtCaret(text) {
  const cleanText = sanitizeSuggestionText(text);
  if (!cleanText) return;

  const activeTextarea = document.querySelector(".bit-card-textarea:focus");
  if (activeTextarea) {
    const start = activeTextarea.selectionStart;
    const end = activeTextarea.selectionEnd;
    activeTextarea.value =
      activeTextarea.value.substring(0, start) + cleanText + activeTextarea.value.substring(end);
    activeTextarea.selectionStart = activeTextarea.selectionEnd = start + cleanText.length;
    activeTextarea.dispatchEvent(new Event("input", { bubbles: true }));
    autoExpand(activeTextarea);
    return;
  }

  if (!els.bitsContainer) return;
  if (bits.length === 0) {
    addBit(cleanText);
    return;
  }
  const lastBit = bits[bits.length - 1];
  lastBit.content = (lastBit.content ? lastBit.content + " " : "") + cleanText;
  const cards = els.bitsContainer.querySelectorAll(".bit-card");
  const lastCard = cards[cards.length - 1];
  if (lastCard) {
    const ta = lastCard.querySelector(".bit-card-textarea");
    if (ta) {
      ta.value = lastBit.content;
      autoExpand(ta);
    }
  }
  notifyBitsChanged();
}

function isWriterTarget(target) {
  if (!target || !els.bitsContainer) return false;
  return els.bitsContainer === target || els.bitsContainer.contains(target);
}

async function createDraft() {
  const nickname = els.nicknameInput.value.trim();
  if (!nickname) {
    alert("Please enter a nickname.");
    return;
  }
  const title = prompt("Draft title?");
  if (!title) return;
  try {
    const res = await fetch("/api/drafts", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ nickname, title }),
    });
    const data = await res.json();
    if (res.ok) {
      currentDraftId = data.draft_id;
      await loadDrafts();
      els.draftSelect.value = String(currentDraftId);
      await loadDraft(currentDraftId);
    } else {
      showError(data.error || "Failed to create draft.");
    }
  } catch (err) {
    showError("Network error while creating draft.");
  }
}

async function refreshSuggestion() {
  if (suggestionInFlight) return;
  const draftText = getBitsText().trim();
  if (!draftText) {
    setGhost("Start with a line, then I will suggest a follow-up.");
    return;
  }
  suggestionInFlight = true;
  const requestStartedAt = Date.now();
  try {
    const res = await fetch("/api/suggestions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ draft: draftText }),
    });
    const data = await res.json();
    if (res.ok) {
      if (lastUserTypedAt > requestStartedAt) {
        return;
      }
      const suggestion = data.suggestion || "";
      setGhost(suggestion);
      if (suggestion) addListItem(els.suggestionList, suggestion);
    } else {
      showError(data.error || "Suggestion failed.");
    }
  } catch (err) {
    showError("Network error while fetching suggestion.");
  } finally {
    suggestionInFlight = false;
  }
}

async function generatePunchlines() {
  const topic = els.topicInput?.value.trim();
  if (!topic) return;
  try {
    const res = await fetch("/api/punchlines", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ topic }),
    });
    const data = await res.json();
    if (res.ok) {
      if (els.punchlineList) els.punchlineList.innerHTML = "";
      (Array.isArray(data.items) ? data.items : []).forEach((item) => addListItem(els.punchlineList, item));
    } else {
      showError(data.error || "Punchline generation failed.");
    }
  } catch (err) {
    showError("Network error while generating punchlines.");
  }
}

async function loadDrafts() {
  const nickname = els.nicknameInput.value.trim();
  if (!nickname) {
    const localPresets = loadStylePresets();
    renderStylePresetOptions(localPresets, localPresets[0] || "");
    els.draftSelect.innerHTML = "";
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "Enter nickname to load drafts";
    opt.disabled = true;
    opt.selected = true;
    els.draftSelect.appendChild(opt);
    return;
  }
  await syncStylePresetsForNickname();
  const res = await fetch(`/api/drafts?nickname=${encodeURIComponent(nickname)}`);
  const data = await res.json();
  if (!res.ok) {
    showError(data.error || "Failed to load drafts.");
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
    return;
  }
  data.items.forEach((d) => {
    const opt = document.createElement("option");
    opt.value = d.id;
    opt.textContent = d.title;
    els.draftSelect.appendChild(opt);
  });
  if (!currentDraftId && data.items.length) {
    await loadDraft(data.items[0].id);
    els.draftSelect.value = String(data.items[0].id);
  }
}

async function loadDraft(draftId) {
  const nickname = els.nicknameInput.value.trim();
  if (!nickname) return;
  const res = await fetch(
    `/api/drafts/${draftId}?nickname=${encodeURIComponent(nickname)}`
  );
  const data = await res.json();
  if (res.ok) {
    currentDraftId = data.id;
    loadBitsFromContent(data.content || "");
    connectSSE();
    clearStageLog();
    setStageStatus("Stage: idle");
    showStartPrompt(false);
    showCancelPrompt(false);
    performanceRunning = false;
    activePerformanceId = null;
    cancelPending = false;
    resetReview();
    clearRehearsalAnalysis(true);
    lastRehearsalBlob = null;
    lastRehearsalFilename = "";
    setRehearsalStatus("Rehearsal: no audio selected");
  } else {
    showError(data.error || "Failed to load draft.");
  }
}

async function renameDraft() {
  const title = els.renameInput.value.trim();
  if (!title || !currentDraftId) return;
  const nickname = els.nicknameInput.value.trim();
  const res = await fetch(`/api/drafts/${currentDraftId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ nickname, title }),
  });
  const data = await res.json();
  if (res.ok) {
    els.renameModal.classList.add("hidden");
    await loadDrafts();
    els.draftSelect.value = String(currentDraftId);
  } else {
    showError(data.error || "Rename failed.");
  }
}

async function archiveDraft() {
  if (!currentDraftId) return;
  const nickname = els.nicknameInput.value.trim();
  const res = await fetch(`/api/drafts/${currentDraftId}/archive`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ nickname }),
  });
  const data = await res.json();
  if (res.ok) {
    currentDraftId = null;
    clearBits();
    await loadDrafts();
  } else {
    showError(data.error || "Archive failed.");
  }
}

async function triggerAnalysis() {
  if (!currentDraftId) return;
  const draftText = getBitsText().trim();
  if (!draftText) return;
  try {
    await fetch("/api/analysis", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ draft_id: currentDraftId, draft: draftText }),
    });
  } catch (err) {
    showError("Network error while analyzing draft.");
  }
}

function scheduleIdleSuggestion() {
  if (idleTimer) clearTimeout(idleTimer);
  idleTimer = setTimeout(() => {
    if (!currentSuggestion) {
      refreshSuggestion();
    }
    triggerAnalysis();
  }, idleDelayMs);
}

els.newDraftBtn.addEventListener("click", createDraft);
els.renameDraftBtn.addEventListener("click", () => {
  if (!currentDraftId) return;
  els.renameInput.value = els.draftSelect.selectedOptions[0]?.textContent || "";
  els.renameModal.classList.remove("hidden");
});
els.renameCancelBtn.addEventListener("click", () => {
  els.renameModal.classList.add("hidden");
});
els.renameConfirmBtn.addEventListener("click", renameDraft);
els.archiveDraftBtn.addEventListener("click", archiveDraft);
els.nicknameInput.addEventListener("blur", loadDrafts);
els.nicknameInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    loadDrafts();
  }
});
els.draftSelect.addEventListener("change", async (e) => {
  if (!e.target.value) return;
  await saveCurrentDraft();
  await loadDraft(e.target.value);
});
els.draftSelect.addEventListener("focus", loadDrafts);
els.draftSelect.addEventListener("click", loadDrafts);
if (els.stageStartBtn) {
  els.stageStartBtn.addEventListener("click", () => {
    startPerformance();
  });
}
if (els.stageDismissBtn) {
  els.stageDismissBtn.addEventListener("click", () => {
    showStartPrompt(false);
    performancePromptArmed = false;
    schedulePerformancePrompt();
  });
}
if (els.cancelSaveBtn) {
  els.cancelSaveBtn.addEventListener("click", () => {
    cancelPerformance(true);
  });
}
if (els.cancelDiscardBtn) {
  els.cancelDiscardBtn.addEventListener("click", () => {
    cancelPerformance(false);
  });
}
if (els.asrBtn) {
  els.asrBtn.addEventListener("click", () => {
    toggleAsrRecording();
  });
}
if (els.uploadRehearsalBtn && els.rehearsalAudioInput) {
  els.uploadRehearsalBtn.addEventListener("click", () => {
    els.rehearsalAudioInput.click();
  });
  els.rehearsalAudioInput.addEventListener("change", (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setTranscript(""); // clear previous transcript when a new file is uploaded
    setLastRehearsalAudio(file, file.name || "uploaded-audio.wav", "uploaded take");
  });
}
if (els.analyzeRehearsalBtn) {
  els.analyzeRehearsalBtn.addEventListener("click", () => {
    analyzeRehearsalTake();
  });
}
if (els.saveDraftBtn) {
  els.saveDraftBtn.addEventListener("click", () => {
    void saveDraftNow();
  });
}
if (els.includeVideoRef) {
  els.includeVideoRef.addEventListener("change", () => {
    const currentNote = (Array.isArray(lastFocusNotes) ? lastFocusNotes : []).find((item) => String(item?.utterance_id || "") === String(activeUtteranceId || ""));
    const currentUtterance = (Array.isArray(lastUtterances) ? lastUtterances : []).find((item) => String(item?.id || "") === String(activeUtteranceId || ""));
    renderFocusedNote(currentNote || null, currentUtterance || null);
    renderStudyVideos(lastVideoReferences, includeVideoReferenceEnabled());
    updateFeedbackDownloadLink();
  });
}
if (els.genPunchlines) {
  els.genPunchlines.addEventListener("click", () => {
    void generatePunchlines();
  });
}
if (els.processMapViewBtn) {
  els.processMapViewBtn.addEventListener("click", () => {
    setStageView("process_map");
    renderProcessMap(lastProcessMap, activeMarkerId);
  });
}
if (els.stageLogViewBtn) {
  els.stageLogViewBtn.addEventListener("click", () => {
    setStageView("stage");
    setStageStatus(performanceRunning ? "Stage: performing..." : "Stage: idle");
  });
}
if (els.saveStylePresetBtn) {
  els.saveStylePresetBtn.addEventListener("click", () => {
    void saveStylePreset();
  });
}
if (els.stylePresetInput) {
  els.stylePresetInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      void saveStylePreset();
    }
  });
}

if (els.addBitBtn) {
  els.addBitBtn.addEventListener("click", () => {
    addBit("");
  });
}

const preparePerformanceBtn = document.getElementById("preparePerformanceBtn");
if (preparePerformanceBtn) {
  preparePerformanceBtn.addEventListener("click", () => {
    if (appState === "ready_to_perform") {
      setAppState("editing");
    } else {
      // If writing area is empty or no bits exist, create one blank bit so the
      // teleprompter and recording always work (user can perform/record freely).
      if (bits.length === 0) {
        bits = [{ id: generateBitId(), content: "", type: "bit", title: "", tags: [], isCollapsed: false }];
        renderBits(bits);
      }
      selectedBits = bits.filter((b) => b.selectedForPerformance);
      // When nothing was checked, fall through to all-bits mode (getSelectedBitsText handles this)
      setAppState("ready_to_perform");
    }
  });
}

if (els.backToEditFromTeleprompterBtn) {
  els.backToEditFromTeleprompterBtn.addEventListener("click", () => {
    setAppState("editing");
  });
}

// ── Save transcript → Writing Area ───────────────────────────────────────────
if (els.saveTranscriptBtn) {
  els.saveTranscriptBtn.addEventListener("click", () => {
    // Collect text to save: prefer improv utterances (no script_range), fallback to full transcript
    let textToSave = "";

    const improvUtterances = (Array.isArray(lastUtterances) ? lastUtterances : [])
      .filter((u) => u?.script_range?.char_start == null && (u?.text || "").trim());

    if (improvUtterances.length) {
      // Only improv parts (yellow) – what the user ad-libbed
      textToSave = improvUtterances.map((u) => sanitizeDisplayText(u.text || "")).join(" ");
    } else if (lastUtterances.length) {
      // All utterances concatenated (full performance transcript)
      textToSave = lastUtterances.map((u) => sanitizeDisplayText(u.text || "")).join(" ");
    } else {
      textToSave = currentTranscript || "";
    }

    textToSave = textToSave.trim();
    if (!textToSave) return;

    addBit(textToSave);

    // If in performance mode, refresh the teleprompter so the new bit appears immediately
    if (appState === "ready_to_perform") {
      renderTeleprompter();
    }

    // Brief visual feedback on the button
    const btn = els.saveTranscriptBtn;
    const original = btn.innerHTML;
    btn.innerHTML = `<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M2 6l3 3 5-5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg> Saved!`;
    btn.disabled = true;
    setTimeout(() => {
      btn.innerHTML = original;
      btn.disabled = false;
    }, 1800);
  });
}

// Apply initial state so the review panel starts disabled
setAppState("editing");

document.addEventListener(
  "drop",
  (e) => {
    if (isWriterTarget(e.target)) return;
    e.preventDefault();
    e.stopPropagation();
  },
  true
);

document.addEventListener(
  "dragover",
  (e) => {
    if (isWriterTarget(e.target)) return;
    e.preventDefault();
    e.stopPropagation();
    e.dataTransfer.dropEffect = "none";
  },
  true
);

document.addEventListener("drop", (e) => {
  if (isWriterTarget(e.target)) return;
  e.preventDefault();
});

document.addEventListener("keydown", (e) => {
  if (!(e.ctrlKey || e.metaKey)) return;
  if (String(e.key).toLowerCase() !== "s") return;
  e.preventDefault();
  void saveDraftNow();
});

setInterval(async () => {
  if (!currentDraftId) return;
  await saveCurrentDraft();
}, 5000);

setInterval(() => {
  void fetchVideoDatasetStatus();
}, 15000);

bits = [{ id: generateBitId(), content: "", type: "bit", title: "", tags: [], isCollapsed: false }];
renderBits(bits);
setGhost("Waiting for a quiet moment...");
setAsrStatus("Mic: idle", false);
const initialStylePresets = loadStylePresets();
renderStylePresetOptions(initialStylePresets, initialStylePresets[0] || "");
syncStylePresetsForNickname();
renderDetectedStyle(null);
renderVideoReferences([], true);
renderRehearsalFeedback(lastMarkerFeedback, null);
renderProcessMap(lastProcessMap, null);
setStageView("process_map");
fetchVideoDatasetStatus();
setRehearsalStatus("Rehearsal: no audio selected");
clearRehearsalAnalysis();

async function saveCurrentDraft(manual = false) {
  if (!currentDraftId) return;
  const content = getBitsText();
  try {
    const res = await fetch("/api/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ draft_id: currentDraftId, content }),
    });
    if (res.ok) {
      const stamp = new Date().toLocaleTimeString();
      els.saveStatus.textContent = `${manual ? "Saved" : "Autosave"}: ${stamp}`;
      return;
    }
    if (manual) {
      showError("Failed to save draft.");
    }
  } catch (err) {
    showError(manual ? "Network error while saving draft." : "Network error while autosaving.");
  }
}

function connectSSE() {
  if (!currentDraftId) return;
  if (eventSource) {
    eventSource.close();
  }
  eventSource = new EventSource(`/api/stream?draft_id=${currentDraftId}`);
  eventSource.addEventListener("feedback", (e) => {
    try {
      const payload = JSON.parse(e.data);
      setFeedback(payload);
    } catch (err) {
      showError("Failed to parse feedback.");
    }
  });
  eventSource.addEventListener("process_map", (e) => {
    try {
      const payload = JSON.parse(e.data);
      lastProcessMap = payload && typeof payload === "object" ? payload : null;
      renderProcessMap(lastProcessMap, activeMarkerId);
    } catch (err) {
      showError("Failed to parse process map.");
    }
  });
  eventSource.addEventListener("rehearsal_analysis", (e) => {
    try {
      const payload = JSON.parse(e.data);
      if (payload.draft_id && currentDraftId && String(payload.draft_id) !== String(currentDraftId)) {
        return;
      }
      applyRehearsalAnalysis(payload, "sse");
    } catch (err) {
      showError("Failed to parse rehearsal analysis.");
    }
  });
  eventSource.addEventListener("stage_event", (e) => {
    try {
      const payload = JSON.parse(e.data);
      if (payload.performance_id && ignoredPerformanceIds.has(payload.performance_id)) return;
      if (!activePerformanceId) {
        activePerformanceId = payload.performance_id || null;
        if (pendingCancelAction) {
          const action = pendingCancelAction;
          pendingCancelAction = null;
          cancelPerformance(action === "save");
        }
      }
      performanceRunning = true;
      if (activePerformanceId && payload.performance_id && payload.performance_id !== activePerformanceId) return;
      appendStageEvent(payload.role || "system", payload.text || "", payload.score);
      if (activeStageView === "stage") setStageStatus("Stage: performing...");
    } catch (err) {
      showError("Failed to parse stage event.");
    }
  });
  eventSource.addEventListener("stage_end", (e) => {
    try {
      const payload = JSON.parse(e.data);
      if (payload.performance_id && ignoredPerformanceIds.has(payload.performance_id)) return;
      if (activePerformanceId && payload.performance_id && payload.performance_id !== activePerformanceId) return;
      performanceRunning = false;
      cancelPending = false;
      if (activeStageView === "stage") setStageStatus(`Stage: done (score ${payload.score ?? "-"})`);
      const reviewId = payload.performance_id || activePerformanceId;
      if (payload.status === "completed") requestPerformanceReview(reviewId);
      activePerformanceId = null;
      schedulePerformancePrompt();
    } catch (err) {
      showError("Failed to parse stage end.");
    }
  });
  eventSource.onerror = () => {
    showError("Realtime connection lost.");
  };
}

document.querySelectorAll(".btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    btn.classList.remove("btn-click");
    void btn.offsetWidth;
    btn.classList.add("btn-click");
  });
});

// ─── Feedback Drawer ──────────────────────────────────────────────────────────

(function initDrawer() {
  const drawer = document.getElementById("feedbackDrawer");
  const handle = document.getElementById("drawerHandle");
  if (!drawer || !handle) return;

  let open = false;

  handle.addEventListener("click", () => {
    open = !open;
    drawer.classList.toggle("open", open);
    handle.classList.toggle("shifted", open);
  });
})();

// ─── Resizable Splitter ───────────────────────────────────────────────────────

(function initSplitter() {
  const splitter = document.getElementById("splitter");
  const layout = document.querySelector(".layout");
  if (!splitter || !layout) return;

  let dragging = false;
  let startX = 0;
  let startPct = 56;

  function getPct() {
    const raw = getComputedStyle(layout).getPropertyValue("--col-left").trim();
    return parseFloat(raw) || 56;
  }

  splitter.addEventListener("pointerdown", (e) => {
    dragging = true;
    startX = e.clientX;
    startPct = getPct();
    splitter.setPointerCapture(e.pointerId);
    splitter.classList.add("dragging");
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
    e.preventDefault();
  });

  splitter.addEventListener("pointermove", (e) => {
    if (!dragging) return;
    const totalW = layout.getBoundingClientRect().width;
    const dx = e.clientX - startX;
    const newPct = startPct + (dx / totalW) * 100;
    const clamped = Math.min(70, Math.max(30, newPct));
    layout.style.setProperty("--col-left", clamped.toFixed(1) + "%");
  });

  splitter.addEventListener("pointerup", () => {
    dragging = false;
    splitter.classList.remove("dragging");
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
  });

  splitter.addEventListener("pointercancel", () => {
    dragging = false;
    splitter.classList.remove("dragging");
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
  });
})();
