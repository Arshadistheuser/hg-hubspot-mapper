// HG Insights → HubSpot Mapper — Frontend Logic

let currentJobId = null;
let currentTotalRecords = 0;
let pollInterval = null;

// ------------------------------------------------------------------ //
//  Initialization
// ------------------------------------------------------------------ //

document.addEventListener("DOMContentLoaded", () => {
    checkConnection();
    setupDropZone();
});

async function checkConnection() {
    const el = document.getElementById("connection-status");
    try {
        const resp = await fetch("/api/test-connection");
        const data = await resp.json();
        if (data.status === "ok") {
            el.innerHTML = '<span class="status-dot connected"></span> HubSpot Connected';
            el.classList.remove("text-gray-400");
            el.classList.add("text-green-600");
        } else {
            el.innerHTML = '<span class="status-dot error"></span> Connection Failed';
            el.classList.add("text-red-600");
        }
    } catch {
        el.innerHTML = '<span class="status-dot error"></span> Server Error';
        el.classList.add("text-red-600");
    }
}

// ------------------------------------------------------------------ //
//  Drag & Drop / File Input
// ------------------------------------------------------------------ //

function setupDropZone() {
    const zone = document.getElementById("drop-zone");
    const input = document.getElementById("file-input");

    zone.addEventListener("click", () => input.click());

    zone.addEventListener("dragover", (e) => {
        e.preventDefault();
        zone.classList.add("drag-over");
    });

    zone.addEventListener("dragleave", () => {
        zone.classList.remove("drag-over");
    });

    zone.addEventListener("drop", (e) => {
        e.preventDefault();
        zone.classList.remove("drag-over");
        if (e.dataTransfer.files.length) {
            uploadFile(e.dataTransfer.files[0]);
        }
    });

    input.addEventListener("change", () => {
        if (input.files.length) {
            uploadFile(input.files[0]);
        }
    });
}

// ------------------------------------------------------------------ //
//  Upload
// ------------------------------------------------------------------ //

async function uploadFile(file) {
    const errorEl = document.getElementById("upload-error");
    const infoEl = document.getElementById("file-info");
    errorEl.classList.add("hidden");
    infoEl.classList.add("hidden");
    document.getElementById("preview-section").classList.add("hidden");
    document.getElementById("mapping-section").classList.add("hidden");

    const formData = new FormData();
    formData.append("file", file);

    try {
        const resp = await fetch("/api/upload", { method: "POST", body: formData });
        const text = await resp.text();
        let data;
        try {
            data = JSON.parse(text);
        } catch {
            errorEl.textContent = "Server error: " + text.substring(0, 200);
            errorEl.classList.remove("hidden");
            return;
        }

        if (!resp.ok) {
            errorEl.textContent = data.detail || "Upload failed";
            errorEl.classList.remove("hidden");
            return;
        }

        currentJobId = data.job_id;
        currentTotalRecords = data.total_records;
        document.getElementById("file-name").textContent = data.filename;
        document.getElementById("file-records").textContent = `${data.total_records} records found`;
        infoEl.classList.remove("hidden");

        // Show mapping reference and preview
        document.getElementById("mapping-section").classList.remove("hidden");
        showPreview(data.preview, data.total_records);
    } catch (err) {
        errorEl.textContent = "Failed to upload file: " + err.message;
        errorEl.classList.remove("hidden");
    }
}

// ------------------------------------------------------------------ //
//  Preview with Checkboxes
// ------------------------------------------------------------------ //

function showPreview(preview, totalRecords) {
    const section = document.getElementById("preview-section");
    const tbody = document.getElementById("preview-tbody");
    const countEl = document.getElementById("preview-count");

    tbody.innerHTML = "";

    preview.forEach((rec) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td class="px-3 py-1.5 text-center">
                <input type="checkbox" class="row-checkbox" data-row="${rec.row}" onchange="updateSelectedCount()">
            </td>
            <td class="px-3 py-1.5">${esc(rec.row)}</td>
            <td class="px-3 py-1.5">${esc(rec.company)}</td>
            <td class="px-3 py-1.5">${esc(rec.domain)}</td>
            <td class="px-3 py-1.5">${esc(rec.technology)}</td>
            <td class="px-3 py-1.5">${esc(rec.source)}</td>
        `;
        tbody.appendChild(tr);
    });

    const showing = Math.min(preview.length, 50);
    countEl.textContent = `Showing ${showing} of ${totalRecords} total`;

    // Reset select-all and counts
    document.getElementById("select-all").checked = false;
    updateSelectedCount();

    section.classList.remove("hidden");
}

function toggleSelectAll(checkbox) {
    const checkboxes = document.querySelectorAll(".row-checkbox");
    checkboxes.forEach((cb) => { cb.checked = checkbox.checked; });
    updateSelectedCount();
}

function updateSelectedCount() {
    const checked = document.querySelectorAll(".row-checkbox:checked");
    const count = checked.length;
    const el = document.getElementById("selected-count");
    const btn = document.getElementById("approve-btn");

    el.textContent = `${count} selected`;

    if (count > 0) {
        btn.disabled = false;
        if (count === currentTotalRecords || (count === document.querySelectorAll(".row-checkbox").length && currentTotalRecords > 50)) {
            btn.textContent = `Approve & Update All ${currentTotalRecords} Records`;
        } else {
            btn.textContent = `Approve & Update ${count} Selected`;
        }
    } else {
        btn.disabled = true;
        btn.textContent = "Select records to update";
    }
}

// ------------------------------------------------------------------ //
//  Approve & Process
// ------------------------------------------------------------------ //

async function approveAndProcess() {
    if (!currentJobId) return;

    const btn = document.getElementById("approve-btn");
    btn.disabled = true;
    btn.textContent = "Starting...";

    // Collect selected row numbers
    const checked = document.querySelectorAll(".row-checkbox:checked");
    const allCheckboxes = document.querySelectorAll(".row-checkbox");
    const selectAll = checked.length === allCheckboxes.length;

    // If all preview rows are checked AND there are more records beyond preview,
    // send null (process all). Otherwise send specific row numbers.
    let selectedRows = null;
    if (!selectAll || currentTotalRecords === checked.length) {
        // Send specific rows only when not all are selected
        if (!selectAll) {
            selectedRows = Array.from(checked).map((cb) => parseInt(cb.dataset.row));
        }
    }

    try {
        const resp = await fetch(`/api/process/${currentJobId}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ selected_rows: selectedRows }),
        });
        if (!resp.ok) {
            const data = await resp.json();
            alert(data.detail || "Failed to start processing");
            btn.disabled = false;
            updateSelectedCount();
            return;
        }

        // Hide preview & mapping, show progress
        document.getElementById("preview-section").classList.add("hidden");
        document.getElementById("mapping-section").classList.add("hidden");
        document.getElementById("progress-section").classList.remove("hidden");

        // Start polling
        pollInterval = setInterval(pollStatus, 1000);
    } catch (err) {
        alert("Error: " + err.message);
        btn.disabled = false;
        updateSelectedCount();
    }
}

async function pollStatus() {
    if (!currentJobId) return;

    try {
        const resp = await fetch(`/api/status/${currentJobId}`);
        const data = await resp.json();

        const pct = data.progress || 0;
        document.getElementById("progress-bar").style.width = pct + "%";
        document.getElementById("progress-pct").textContent = pct + "%";
        document.getElementById("progress-text").textContent =
            pct < 100 ? "Processing records..." : "Finalizing...";

        if (data.status === "completed" || data.status === "error") {
            clearInterval(pollInterval);
            pollInterval = null;
            showResults(data.results);
        }
    } catch {
        // Silently retry
    }
}

// ------------------------------------------------------------------ //
//  Results
// ------------------------------------------------------------------ //

function showResults(results) {
    document.getElementById("progress-section").classList.add("hidden");
    const section = document.getElementById("results-section");
    section.classList.remove("hidden");

    if (results.error) {
        document.getElementById("stat-total").textContent = "Error";
        document.getElementById("stat-matched").textContent = "-";
        document.getElementById("stat-updated").textContent = "-";
        document.getElementById("stat-failed").textContent = results.error;
        return;
    }

    document.getElementById("stat-total").textContent = results.total_processed;
    document.getElementById("stat-matched").textContent = results.accounts_matched;
    document.getElementById("stat-updated").textContent = results.accounts_updated;
    document.getElementById("stat-failed").textContent = results.failed_matches;

    // Errors table
    const errors = results.errors || [];
    if (errors.length > 0) {
        const container = document.getElementById("errors-container");
        container.classList.remove("hidden");
        const tbody = document.getElementById("errors-tbody");
        tbody.innerHTML = "";
        errors.forEach((err) => {
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td class="px-3">${esc(err.row)}</td>
                <td class="px-3">${esc(err.company)}</td>
                <td class="px-3">${esc(err.domain)}</td>
                <td class="px-3">${esc(err.technology)}</td>
                <td class="px-3 reason-cell">${esc(err.reason)}</td>
            `;
            tbody.appendChild(tr);
        });
    }

    // Success table
    const successes = results.successes || [];
    if (successes.length > 0) {
        const container = document.getElementById("success-container");
        container.classList.remove("hidden");
        const tbody = document.getElementById("success-tbody");
        tbody.innerHTML = "";
        successes.forEach((s) => {
            const tr = document.createElement("tr");
            tr.className = "success-row";
            tr.innerHTML = `
                <td class="px-3">${esc(s.row)}</td>
                <td class="px-3">${esc(s.company)}</td>
                <td class="px-3">${esc(s.domain)}</td>
                <td class="px-3">${esc(s.technology)}</td>
                <td class="px-3">${esc(s.hubspot_company || "")}</td>
            `;
            tbody.appendChild(tr);
        });
    }
}

// ------------------------------------------------------------------ //
//  Download errors
// ------------------------------------------------------------------ //

function downloadErrors() {
    if (!currentJobId) return;
    window.location.href = `/api/download-errors/${currentJobId}`;
}

// ------------------------------------------------------------------ //
//  Reset
// ------------------------------------------------------------------ //

function resetApp() {
    currentJobId = null;
    currentTotalRecords = 0;
    if (pollInterval) clearInterval(pollInterval);

    document.getElementById("file-info").classList.add("hidden");
    document.getElementById("upload-error").classList.add("hidden");
    document.getElementById("mapping-section").classList.add("hidden");
    document.getElementById("preview-section").classList.add("hidden");
    document.getElementById("progress-section").classList.add("hidden");
    document.getElementById("results-section").classList.add("hidden");
    document.getElementById("errors-container").classList.add("hidden");
    document.getElementById("success-container").classList.add("hidden");

    document.getElementById("progress-bar").style.width = "0%";
    document.getElementById("progress-pct").textContent = "0%";

    document.getElementById("file-input").value = "";
}

// ------------------------------------------------------------------ //
//  Helpers
// ------------------------------------------------------------------ //

function esc(str) {
    if (str === null || str === undefined) return "";
    const div = document.createElement("div");
    div.textContent = String(str);
    return div.innerHTML;
}
