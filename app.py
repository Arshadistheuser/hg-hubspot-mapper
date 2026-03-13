"""
HG Insights → HubSpot Tech Stack Mapper
FastAPI application.
"""

from typing import Dict, List, Set
import os
import uuid
import json
import asyncio
import tempfile
import traceback
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

from hubspot_client import HubSpotClient
from excel_parser import parse_file, ParseError, HGRecord
from domain_utils import normalize_domain

load_dotenv()

app = FastAPI(title="HG Insights → HubSpot Mapper")

BASE_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

UPLOAD_DIR = Path(tempfile.gettempdir()) / "hg-hubspot-uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

# In-memory job store (sufficient for single-user / on-demand usage)
jobs: Dict[str, dict] = {}


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Ensure all errors return JSON, not HTML."""
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc), "traceback": traceback.format_exc()},
    )


def get_hubspot_client() -> HubSpotClient:
    token = os.getenv("HUBSPOT_ACCESS_TOKEN", "")
    if not token:
        raise HTTPException(status_code=500, detail="HUBSPOT_ACCESS_TOKEN not configured in .env")
    return HubSpotClient(token)


# ------------------------------------------------------------------ #
#  Pages
# ------------------------------------------------------------------ #

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ------------------------------------------------------------------ #
#  API: Test HubSpot connection
# ------------------------------------------------------------------ #

@app.get("/api/test-connection")
async def test_connection():
    hs = get_hubspot_client()
    ok = await hs.test_connection()
    if ok:
        return {"status": "ok", "message": "Connected to HubSpot successfully"}
    return JSONResponse(status_code=400, content={"status": "error", "message": "Failed to connect. Check your token."})


# ------------------------------------------------------------------ #
#  API: Get tech_stack property info
# ------------------------------------------------------------------ #

@app.get("/api/tech-stack-info")
async def tech_stack_info():
    hs = get_hubspot_client()
    try:
        prop = await hs.get_tech_stack_property()
        return {
            "fieldType": prop.get("fieldType"),
            "type": prop.get("type"),
            "label": prop.get("label"),
            "options": prop.get("options", []),
        }
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})


# ------------------------------------------------------------------ #
#  API: Upload & process
# ------------------------------------------------------------------ #

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """Save uploaded file and return a job_id for processing."""
    ext = Path(file.filename).suffix.lower()
    if ext not in (".xlsx", ".xls", ".csv"):
        raise HTTPException(status_code=400, detail="Only .xlsx, .xls, or .csv files are accepted.")

    job_id = str(uuid.uuid4())
    file_path = UPLOAD_DIR / f"{job_id}{ext}"

    content = await file.read()
    with open(file_path, "wb") as f:
        f.write(content)

    # Parse immediately to give row count feedback
    try:
        records = parse_file(str(file_path))
    except ParseError as e:
        file_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        file_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {str(e)}")

    # Build preview of first 50 records
    preview = []
    for rec in records[:50]:
        preview.append({
            "row": rec.row_number,
            "company": rec.company_name,
            "domain": rec.domain,
            "technology": rec.technology,
            "source": rec.source,
        })

    jobs[job_id] = {
        "id": job_id,
        "filename": file.filename,
        "file_path": str(file_path),
        "total_records": len(records),
        "status": "uploaded",
        "progress": 0,
        "results": None,
        "created_at": datetime.now().isoformat(),
    }

    return {
        "job_id": job_id,
        "filename": file.filename,
        "total_records": len(records),
        "preview": preview,
    }


@app.post("/api/process/{job_id}")
async def process_job(job_id: str):
    """Start processing a previously uploaded file."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]
    if job["status"] == "processing":
        raise HTTPException(status_code=409, detail="Job is already processing")

    job["status"] = "processing"
    job["progress"] = 0

    # Run processing in background
    asyncio.create_task(_process_records(job_id))
    return {"status": "processing", "job_id": job_id}


@app.get("/api/status/{job_id}")
async def job_status(job_id: str):
    """Poll job progress."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    job = jobs[job_id]
    return {
        "id": job["id"],
        "status": job["status"],
        "progress": job["progress"],
        "total_records": job["total_records"],
        "results": job["results"],
    }


@app.get("/api/download-errors/{job_id}")
async def download_errors(job_id: str):
    """Download the error report as CSV."""
    if job_id not in jobs or not jobs[job_id].get("results"):
        raise HTTPException(status_code=404, detail="No results available")

    results = jobs[job_id]["results"]
    errors = results.get("errors", [])
    if not errors:
        raise HTTPException(status_code=404, detail="No errors to download")

    # Build CSV
    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Row", "Company Name", "Domain", "Technology", "Error Reason"])
    for err in errors:
        writer.writerow([
            err.get("row", ""),
            err.get("company", ""),
            err.get("domain", ""),
            err.get("technology", ""),
            err.get("reason", ""),
        ])

    error_file = UPLOAD_DIR / f"{job_id}_errors.csv"
    with open(error_file, "w", newline="", encoding="utf-8") as f:
        f.write(output.getvalue())

    return FileResponse(
        error_file,
        media_type="text/csv",
        filename=f"error_report_{job_id[:8]}.csv",
    )


# ------------------------------------------------------------------ #
#  Background processing
# ------------------------------------------------------------------ #

async def _process_records(job_id: str):
    """Process all records for a job — runs as a background task."""
    job = jobs[job_id]
    hs = HubSpotClient(os.getenv("HUBSPOT_ACCESS_TOKEN", ""))

    records = parse_file(job["file_path"])
    total = len(records)

    # Fetch tech stack property info once
    try:
        field_type = await hs.get_tech_stack_field_type()
        valid_values = await hs.get_valid_tech_stack_values()
    except Exception as e:
        job["status"] = "error"
        job["results"] = {"error": f"Failed to fetch HubSpot property info: {e}"}
        return

    matched = 0
    updated = 0
    failed = 0
    errors: List[dict] = []
    successes: List[dict] = []

    for i, rec in enumerate(records):
        try:
            result = await _process_single_record(hs, rec, field_type, valid_values)
            if result["status"] == "updated":
                matched += 1
                updated += 1
                successes.append(result)
            elif result["status"] == "matched_not_updated":
                matched += 1
                errors.append(result)
            else:
                failed += 1
                errors.append(result)
        except Exception as e:
            failed += 1
            errors.append({
                "row": rec.row_number,
                "company": rec.company_name,
                "domain": rec.domain,
                "technology": rec.technology,
                "reason": f"Unexpected error: {str(e)}",
                "status": "error",
            })

        job["progress"] = int(((i + 1) / total) * 100)

        # Small delay to avoid HubSpot rate limits (100 requests / 10 sec)
        if (i + 1) % 5 == 0:
            await asyncio.sleep(0.2)

    job["status"] = "completed"
    job["results"] = {
        "total_processed": total,
        "accounts_matched": matched,
        "accounts_updated": updated,
        "failed_matches": failed,
        "errors": errors,
        "successes": successes,
    }


async def _process_single_record(
    hs: HubSpotClient,
    rec: HGRecord,
    field_type: str,
    valid_values: Set[str],
) -> dict:
    """Process one HG Insights record against HubSpot."""
    base = {
        "row": rec.row_number,
        "company": rec.company_name,
        "domain": rec.domain,
        "technology": rec.technology,
    }

    # 1. Validate domain
    normalized = normalize_domain(rec.domain)
    if not normalized:
        return {**base, "status": "error", "reason": f"Invalid domain format: '{rec.domain}'"}

    # 2. Validate tech stack value
    if rec.technology and rec.technology not in valid_values:
        # Try case-insensitive match
        match = None
        for v in valid_values:
            if v.lower() == rec.technology.lower():
                match = v
                break
        if match:
            rec.technology = match
        else:
            return {
                **base,
                "status": "matched_not_updated",
                "reason": f"Tech stack value '{rec.technology}' not in HubSpot dropdown. Valid values: {', '.join(sorted(valid_values))}",
            }

    if not rec.technology:
        return {**base, "status": "error", "reason": "Empty technology value"}

    # 3. Search HubSpot for the company
    try:
        companies = await hs.search_company_by_domain(rec.domain)
    except Exception as e:
        return {**base, "status": "error", "reason": f"HubSpot search failed: {str(e)}"}

    if not companies:
        return {**base, "status": "error", "reason": "Domain not found in HubSpot"}

    if len(companies) > 1:
        # Log duplicate warning but still update the first match
        base["warning"] = f"Duplicate accounts found ({len(companies)} matches) — updating first match"

    company = companies[0]
    company_id = company["id"]
    current_tech = company.get("properties", {}).get("tech_stack", "")

    # 4. Update the tech stack
    try:
        await hs.update_tech_stack(company_id, rec.technology, field_type, current_tech)
    except Exception as e:
        return {**base, "status": "error", "reason": f"Failed to update: {str(e)}"}

    return {
        **base,
        "status": "updated",
        "hubspot_company": company.get("properties", {}).get("name", "Unknown"),
        "hubspot_id": company_id,
    }


# ------------------------------------------------------------------ #
#  Run
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
