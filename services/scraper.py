"""
PDDikti Dosen Explorer — Scraper Service
Refactored from pddikti_dosen_ekosyariah.py for async backend integration.
"""

import asyncio
import json
import re
import time
from datetime import datetime, timezone
from typing import List, Optional

import requests
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal
from models import (
    Dosen, PerguruanTinggi, ProdiDetail, ProgramStudi, ScrapeJob, ScrapeLog
)

# ── Configuration ──
BASE_URL = "https://api-pddikti.kemdiktisaintek.go.id"
HEADERS = {
    "Origin": "https://pddikti.kemdiktisaintek.go.id",
    "Referer": "https://pddikti.kemdiktisaintek.go.id/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

TIMEOUT = 40
MAX_RETRIES = 3
RETRY_DELAY = 2
REQ_DELAY = 0.15
MAX_WORKERS = 5

DEFAULT_SEMESTERS = [
    "20252", "20251", "20242", "20241", "20232", "20231",
    "20222", "20221", "20212", "20211"
]

# ── 22 Official Prodi Categories ──
RUMPUN_PRODI_RESMI = [
    "AKUNTANSI SYARIAH",
    "ASURANSI SYARIAH",
    "BISNIS ISLAM",
    "EKONOMI DAN BISNIS ISLAM",
    "EKONOMI ISLAM",
    "EKONOMI SYARIAH",
    "EKONOMI SYARIAH (EKONOMI ISLAM)",
    "EKONOMI SYARIAH (MANAJEMEN SYARIAH)",
    "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "ILMU EKONOMI DAN KEUANGAN ISLAM",
    "ILMU EKONOMI ISLAM",
    "ILMU EKONOMI SYARIAH",
    "KEUANGAN ISLAM TERAPAN",
    "KEUANGAN SYARIAH",
    "MANAJEMEN BISNIS SYARIAH",
    "MANAJEMEN HAJI DAN UMROH",
    "MANAJEMEN KEUANGAN DAN PERBANKAN SYARIAH",
    "MANAJEMEN ZAKAT DAN WAKAF",
    "PARIWISATA SYARIAH",
    "PERBANKAN SYARIAH",
    "SAINS EKONOMI ISLAM",
    "ZAKAT DAN WAKAF",
]

PRODI_NORMALIZATION = {
    # AKUNTANSI SYARIAH
    "AKUNTANSI SYARIAH": "AKUNTANSI SYARIAH",
    "AKUNTANSI SYARI'AH": "AKUNTANSI SYARIAH",
    "AKUNTANSI SYARI`AH": "AKUNTANSI SYARIAH",
    "AKUNTANSI SYARI AH": "AKUNTANSI SYARIAH",
    # ASURANSI SYARIAH
    "ASURANSI SYARIAH": "ASURANSI SYARIAH",
    "ASURANSI SYARI'AH": "ASURANSI SYARIAH",
    "ASURANSI SYARI`AH": "ASURANSI SYARIAH",
    # BISNIS ISLAM
    "BISNIS ISLAM": "BISNIS ISLAM",
    # EKONOMI DAN BISNIS ISLAM
    "EKONOMI DAN BISNIS ISLAM": "EKONOMI DAN BISNIS ISLAM",
    # EKONOMI SYARIAH (EKONOMI ISLAM)
    "EKONOMI SYARIAH (EKONOMI ISLAM)": "EKONOMI SYARIAH (EKONOMI ISLAM)",
    "EKONOMI SYARIAH / EKONOMI ISLAM": "EKONOMI SYARIAH (EKONOMI ISLAM)",
    "EKONOMI SYARI'AH / EKONOMI ISLAM": "EKONOMI SYARIAH (EKONOMI ISLAM)",
    "EKONOMI SYARI`AH / EKONOMI ISLAM": "EKONOMI SYARIAH (EKONOMI ISLAM)",
    # EKONOMI SYARIAH (MANAJEMEN SYARIAH)
    "EKONOMI SYARIAH (MANAJEMEN SYARIAH)": "EKONOMI SYARIAH (MANAJEMEN SYARIAH)",
    "EKONOMI SYARI'AH (MANAJEMEN SYARI'AH)": "EKONOMI SYARIAH (MANAJEMEN SYARIAH)",
    # HUKUM EKONOMI SYARIAH (MUAMALAH)
    "HUKUM EKONOMI SYARIAH (MUAMALAH)": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "HUKUM EKONOMI SYARIAH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "HUKUM EKONOMI SYARI'AH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "HUKUM EKONOMI SYARI`AH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "MUAMALAH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "MUA'MALAH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "MU'AMALAH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "MU`AMALAH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "MUA`MALAH": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    "MUAMALAT": "HUKUM EKONOMI SYARIAH (MUAMALAH)",
    # ILMU EKONOMI DAN KEUANGAN ISLAM
    "ILMU EKONOMI DAN KEUANGAN ISLAM": "ILMU EKONOMI DAN KEUANGAN ISLAM",
    "EKONOMI DAN KEUANGAN ISLAM": "ILMU EKONOMI DAN KEUANGAN ISLAM",
    # ILMU EKONOMI ISLAM
    "ILMU EKONOMI ISLAM": "ILMU EKONOMI ISLAM",
    # ILMU EKONOMI SYARIAH
    "ILMU EKONOMI SYARIAH": "ILMU EKONOMI SYARIAH",
    "ILMU EKONOMI SYARI'AH": "ILMU EKONOMI SYARIAH",
    # KEUANGAN ISLAM TERAPAN
    "KEUANGAN ISLAM TERAPAN": "KEUANGAN ISLAM TERAPAN",
    # KEUANGAN SYARIAH
    "KEUANGAN SYARIAH": "KEUANGAN SYARIAH",
    "KEUANGAN SYARI'AH": "KEUANGAN SYARIAH",
    # MANAJEMEN BISNIS SYARIAH
    "MANAJEMEN BISNIS SYARIAH": "MANAJEMEN BISNIS SYARIAH",
    "MANAJEMEN BISNIS SYARI'AH": "MANAJEMEN BISNIS SYARIAH",
    "MANAJEMEN BISNIS SYARI`AH": "MANAJEMEN BISNIS SYARIAH",
    "MANAJEMEN DAN BISNIS SYARIAH": "MANAJEMEN BISNIS SYARIAH",
    "BISNIS DAN MANAJEMEN SYARIAH": "MANAJEMEN BISNIS SYARIAH",
    # MANAJEMEN HAJI DAN UMROH
    "MANAJEMEN HAJI DAN UMROH": "MANAJEMEN HAJI DAN UMROH",
    "MANAJEMEN HAJI DAN UMRAH": "MANAJEMEN HAJI DAN UMROH",
    "MANAJEMEN HAJI DAN UMRA": "MANAJEMEN HAJI DAN UMROH",
    # MANAJEMEN KEUANGAN DAN PERBANKAN SYARIAH
    "MANAJEMEN KEUANGAN DAN PERBANKAN SYARIAH": "MANAJEMEN KEUANGAN DAN PERBANKAN SYARIAH",
    "MANAJEMEN KEUANGAN DAN PERBANKAN SYARI'AH": "MANAJEMEN KEUANGAN DAN PERBANKAN SYARIAH",
    # MANAJEMEN ZAKAT DAN WAKAF
    "MANAJEMEN ZAKAT DAN WAKAF": "MANAJEMEN ZAKAT DAN WAKAF",
    # PARIWISATA SYARIAH
    "PARIWISATA SYARIAH": "PARIWISATA SYARIAH",
    "PARIWISATA SYARI'AH": "PARIWISATA SYARIAH",
    # PERBANKAN SYARIAH
    "PERBANKAN SYARIAH": "PERBANKAN SYARIAH",
    "PERBANKAN SYARI'AH": "PERBANKAN SYARIAH",
    "PERBANKAN SYARI`AH": "PERBANKAN SYARIAH",
    "PERBANKAN SYARI AH": "PERBANKAN SYARIAH",
    # SAINS EKONOMI ISLAM
    "SAINS EKONOMI ISLAM": "SAINS EKONOMI ISLAM",
    # ZAKAT DAN WAKAF
    "ZAKAT DAN WAKAF": "ZAKAT DAN WAKAF",
    "ZAKAT WAKAF": "ZAKAT DAN WAKAF",
    "ZAKAT & WAKAF": "ZAKAT DAN WAKAF",
    "ZAKAT WAKAF": "ZAKAT DAN WAKAF",
    # EKONOMI ISLAM (standalone)
    "EKONOMI ISLAM": "EKONOMI ISLAM",
    # EKONOMI SYARIAH (standalone — placed last so longer keys match first)
    "EKONOMI SYARIAH": "EKONOMI SYARIAH",
    "EKONOMI SYARI'AH": "EKONOMI SYARIAH",
    "EKONOMI SYARI`AH": "EKONOMI SYARIAH",
}

PRODI_SEARCH_VARIANTS = {
    "EKONOMI SYARIAH / EKONOMI ISLAM": ["EKONOMI SYARIAH", "EKONOMI ISLAM"],
    "EKONOMI SYARIAH (EKONOMI ISLAM)": ["EKONOMI SYARIAH", "EKONOMI ISLAM"],
    "EKONOMI SYARIAH (MANAJEMEN SYARIAH)": ["EKONOMI SYARIAH", "MANAJEMEN SYARIAH"],
    "HUKUM EKONOMI SYARIAH (MUAMALAH)": ["HUKUM EKONOMI SYARIAH", "MUAMALAH"],
}


def get_rumpun_search_terms(rumpun: str) -> list[str]:
    """Build search keywords for nationwide prodi discovery per rumpun."""
    terms = {rumpun.strip().upper()}

    for alias, normalized in PRODI_NORMALIZATION.items():
        if normalized == rumpun:
            terms.add(alias.strip().upper())

    if rumpun in PRODI_SEARCH_VARIANTS:
        for alias in PRODI_SEARCH_VARIANTS[rumpun]:
            terms.add(alias.strip().upper())

    # Search broad terms first so we can discover all campuses for each rumpun.
    return sorted(terms, key=len, reverse=True)


def normalize_prodi_name(api_name: str) -> Optional[str]:
    upper = api_name.strip().replace('\xa0', ' ').upper()
    sorted_keys = sorted(PRODI_NORMALIZATION.keys(), key=len, reverse=True)
    for key in sorted_keys:
        if key in upper:
            return PRODI_NORMALIZATION[key]
    return None


def normalize_for_search(name: str) -> str:
    name = name.replace('\xa0', ' ')
    if "/" in name:
        name = name.split("/")[0].strip()
    name = re.sub(r'\(.*?\)', '', name).strip()
    return name


def generate_search_queries(prodi_name: str, pt_name: str) -> list:
    queries = []
    prodi_name = prodi_name.replace('\xa0', ' ')
    pt_name = pt_name.replace('\xa0', ' ')
    prodi_up = prodi_name.strip().upper()

    if prodi_up in PRODI_SEARCH_VARIANTS:
        for v in PRODI_SEARCH_VARIANTS[prodi_up]:
            queries.append(f"{v} {pt_name}")

    clean = normalize_for_search(prodi_name)
    queries.append(f"{clean} {pt_name}")
    queries.append(f"{prodi_name} {pt_name}")
    queries.append(clean)

    seen = set()
    result = []
    for q in queries:
        q = q.strip()
        if q and q not in seen:
            seen.add(q)
            result.append(q)
    return result


def match_prodi(results, prodi_name, jenjang, pt_name):
    if not results or not isinstance(results, list):
        return None

    prodi_up = prodi_name.strip().upper()
    pt_up = pt_name.strip().upper()
    jenj_up = jenjang.strip().upper() if jenjang else ""
    clean_up = normalize_for_search(prodi_name).upper()

    best = None
    best_score = -1

    for r in results:
        r_nama = (r.get("nama", "") or "").strip().upper()
        r_pt = (r.get("pt", "") or "").strip().upper()
        r_jenj = (r.get("jenjang", "") or "").strip().upper()

        score = 0

        if pt_up == r_pt:
            score += 100
        elif pt_up in r_pt or r_pt in pt_up:
            score += 80
        else:
            pt_words = set(pt_up.split())
            rpt_words = set(r_pt.split())
            if len(pt_words & rpt_words) >= 2:
                score += 40
            else:
                continue

        target_rumpun = normalize_prodi_name(prodi_name)
        result_rumpun = normalize_prodi_name(r_nama)

        if target_rumpun and result_rumpun and target_rumpun == result_rumpun:
            score += 50
        elif clean_up == r_nama or prodi_up == r_nama:
            score += 45
        elif clean_up in r_nama or r_nama in clean_up:
            score += 40
        else:
            continue

        if jenj_up and jenj_up == r_jenj:
            score += 10

        if score > best_score:
            best_score = score
            best = r

    return best


def fetch_api(endpoint, retries=MAX_RETRIES):
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code >= 400:
                return None
            data = r.json()
            if isinstance(data, dict) and data.get("message") == "Not Found":
                return None
            return data
        except (requests.exceptions.RequestException, json.JSONDecodeError):
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
    return None


async def send_log(job_id: int, level: str, message: str, db: AsyncSession):
    """Save log to DB and broadcast via WebSocket."""
    log = ScrapeLog(job_id=job_id, level=level, message=message)
    db.add(log)
    await db.commit()

    # Broadcast to WS
    from routers.scrape_router import broadcast_to_job
    await broadcast_to_job(job_id, {
        "type": "log",
        "level": level,
        "message": message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


async def send_progress(job_id: int, data: dict):
    """Broadcast progress update via WebSocket."""
    from routers.scrape_router import broadcast_to_job
    data["type"] = "progress"
    await broadcast_to_job(job_id, data)


async def run_scraping_job(
    job_id: int,
    prodi_filter: List[str],
    semesters: Optional[List[str]] = None,
    pt_filter: Optional[str] = None,
):
    """Main scraping job — runs in background."""
    semesters = semesters or DEFAULT_SEMESTERS

    async with AsyncSessionLocal() as db:
        try:
            await send_log(job_id, "info", f"🚀 Memulai scraping untuk {len(prodi_filter)} program studi...", db)

            # STEP 1: Nationwide discovery prodi IDs per selected rumpun
            await send_log(job_id, "info", "🌏 STEP 1: Discovery prodi nasional dari API PDDikti...", db)
            resolved = []
            seen_ids = set()

            for rumpun_index, rumpun in enumerate(prodi_filter, 1):
                queries = get_rumpun_search_terms(rumpun)
                before_count = len(resolved)

                await send_log(
                    job_id,
                    "info",
                    f"🔎 [{rumpun_index}/{len(prodi_filter)}] Discovery program studi '{rumpun}' "
                    f"dengan {len(queries)} kata kunci",
                    db,
                )

                for query_index, query in enumerate(queries, 1):
                    await asyncio.sleep(REQ_DELAY)
                    results = await asyncio.to_thread(fetch_api, f"pencarian/prodi/{query}")
                    if not isinstance(results, list):
                        continue

                    for item in results:
                        if not isinstance(item, dict):
                            continue

                        pid = (item.get("id", "") or "").strip()
                        nama = (item.get("nama", "") or "").strip()
                        pt_name = (item.get("pt", "") or "").strip()
                        jenjang = (item.get("jenjang", "") or "").strip()

                        if not pid or not nama or not pt_name:
                            continue
                        if pid in seen_ids:
                            continue

                        normalized = normalize_prodi_name(nama)
                        if normalized != rumpun:
                            continue

                        if pt_filter and pt_filter.strip().upper() not in pt_name.upper():
                            continue

                        seen_ids.add(pid)
                        resolved.append({
                            "id": pid,
                            "nama": nama,
                            "jenjang": jenjang,
                            "pt": pt_name,
                            "pt_singkat": (item.get("pt_singkat", "") or "").strip(),
                            "target_prodi": nama,
                            "target_pt": pt_name,
                            "rumpun": rumpun,
                        })

                    if query_index % 5 == 0 or query_index == len(queries):
                        await send_progress(job_id, {
                            "phase": "resolve",
                            "current": query_index,
                            "total": len(queries),
                            "resolved": len(resolved),
                        })

                discovered = len(resolved) - before_count
                await send_log(
                    job_id,
                    "info",
                    f"✅ Program Studi '{rumpun}': {discovered} prodi berhasil ditemukan",
                    db,
                )

                await db.execute(
                    update(ScrapeJob).where(ScrapeJob.id == job_id).values(
                        total_prodi=len(resolved),
                        resolved_prodi=len(resolved),
                    )
                )
                await db.commit()

            await send_log(
                job_id, "success",
                f"✅ STEP 1 selesai: {len(resolved)} prodi nasional berhasil resolved",
                db
            )

            if not resolved:
                await send_log(
                    job_id,
                    "warning",
                    "⚠️ Tidak ada prodi yang berhasil ditemukan dari filter saat ini",
                    db,
                )
                await db.execute(
                    update(ScrapeJob).where(ScrapeJob.id == job_id).values(
                        status="completed",
                        completed_at=datetime.now(timezone.utc),
                        total_prodi=0,
                        resolved_prodi=0,
                        total_dosen=0,
                        new_dosen=0,
                    )
                )
                await db.commit()
                return

            # ── STEP 2: Scrape & save prodi detail + PT info ──
            await send_log(
                job_id, "info",
                f"📋 STEP 2: Mengambil detail {len(resolved)} prodi & info PT...",
                db,
            )

            # Cache PT detail fetches to avoid duplicates
            pt_detail_cache: dict[str, dict] = {}   # pt_name → detail dict
            prodi_saved_count = 0

            for i, prodi in enumerate(resolved, 1):
                prodi_id = prodi["id"]
                prodi_nama = prodi["nama"]
                pt_name = prodi["pt"]
                rumpun = prodi["rumpun"]

                # Check if already in DB
                existing_pd = await db.execute(
                    select(ProdiDetail).where(ProdiDetail.pddikti_id == prodi_id)
                )
                if existing_pd.scalar_one_or_none():
                    continue

                # Fetch prodi detail from API
                await asyncio.sleep(REQ_DELAY)
                prodi_detail = await asyncio.to_thread(
                    fetch_api, f"prodi/detail/{prodi_id}"
                )
                if not prodi_detail or not isinstance(prodi_detail, dict):
                    prodi_detail = {}

                # Parse prodi detail fields
                kode_prodi = (prodi_detail.get("kode_prodi", "") or "").strip()
                keterangan = (prodi_detail.get("status", "") or "").strip() or "Aktif"
                akreditasi_val = (prodi_detail.get("akreditasi", "") or "").strip()
                status_akreditasi_val = (prodi_detail.get("status_akreditasi", "") or "").strip()
                provinsi_prodi = (prodi_detail.get("provinsi", "") or "").strip()

                # Fetch PT detail (cached)
                pt_info = pt_detail_cache.get(pt_name.upper())
                if pt_info is None:
                    pt_sp_id = (prodi_detail.get("id_sp", "") or "").strip()
                    if pt_sp_id:
                        await asyncio.sleep(REQ_DELAY)
                        pt_raw = await asyncio.to_thread(
                            fetch_api, f"pt/detail/{pt_sp_id}"
                        )
                        if pt_raw and isinstance(pt_raw, dict):
                            pt_info = {
                                "status_pt": (pt_raw.get("status_pt", "") or "").strip(),
                                "kelompok": (pt_raw.get("kelompok", "") or "").strip(),
                                "pembina": (pt_raw.get("pembina", "") or "").strip(),
                                "provinsi": (pt_raw.get("provinsi_pt", "") or "").strip(),
                                "akreditasi_pt": (pt_raw.get("akreditasi_pt", "") or "").strip(),
                            }
                        else:
                            pt_info = {}
                    else:
                        pt_info = {}
                    pt_detail_cache[pt_name.upper()] = pt_info

                # Derive classification
                status_pt = pt_info.get("status_pt", "").upper()
                ptn_pts = "PTN" if "NEGERI" in status_pt or status_pt == "PTN" else "PTS"
                kelompok = pt_info.get("kelompok", "").upper()
                pembina = pt_info.get("pembina", "").upper()
                ptkin = "PTKIN" if "PTKIN" in kelompok or "AGAMA" in pembina or "KEMENAG" in pembina else "NON PTKIN"
                dikti = "DIKTIS" if ptkin == "PTKIN" else "DIKTI"
                provinsi_final = pt_info.get("provinsi", "") or provinsi_prodi

                # Count dosen for this prodi (from first semester with data)
                jumlah_dosen = 0
                for sem in semesters:
                    await asyncio.sleep(REQ_DELAY)
                    homebase = await asyncio.to_thread(
                        fetch_api, f"dosen/homebase/{prodi_id}?semester={sem}"
                    )
                    if homebase and isinstance(homebase, list) and len(homebase) > 0:
                        jumlah_dosen = len(homebase)
                        prodi["semester_terakhir"] = sem
                        break

                # Get or create PT in DB
                pt_result = await db.execute(
                    select(PerguruanTinggi).where(PerguruanTinggi.nama == pt_name)
                )
                pt_obj = pt_result.scalar_one_or_none()
                if not pt_obj:
                    pt_obj = PerguruanTinggi(
                        nama=pt_name,
                        provinsi=provinsi_final,
                        status_pt=ptn_pts,
                        kelompok=ptkin,
                        pembina=dikti,
                    )
                    db.add(pt_obj)
                    await db.flush()
                else:
                    # Update PT classification if not yet set
                    if not pt_obj.status_pt:
                        pt_obj.status_pt = ptn_pts
                    if not pt_obj.kelompok:
                        pt_obj.kelompok = ptkin
                    if not pt_obj.pembina:
                        pt_obj.pembina = dikti
                    if not pt_obj.provinsi:
                        pt_obj.provinsi = provinsi_final

                # Save ProdiDetail
                pd_obj = ProdiDetail(
                    pddikti_id=prodi_id,
                    pt_id=pt_obj.id,
                    nama_prodi=prodi_nama,
                    rumpun=rumpun,
                    jenjang=prodi.get("jenjang", ""),
                    kode_prodi=kode_prodi,
                    jumlah_dosen=jumlah_dosen,
                    keterangan=keterangan,
                    akreditasi=akreditasi_val,
                    status_akreditasi=status_akreditasi_val or (
                        "Belum Terakreditasi" if not akreditasi_val else "Terakreditasi"
                    ),
                    ptn_pts=ptn_pts,
                    ptkin_non_ptkin=ptkin,
                    dikti_diktis=dikti,
                    provinsi=provinsi_final,
                    semester_terakhir=prodi.get("semester_terakhir", ""),
                )
                db.add(pd_obj)
                prodi_saved_count += 1

                if prodi_saved_count % 25 == 0:
                    await db.commit()
                    await send_log(
                        job_id, "info",
                        f"📋 Detail prodi tersimpan: {prodi_saved_count}/{len(resolved)}",
                        db,
                    )
                    await send_progress(job_id, {
                        "phase": "prodi_detail",
                        "current": i,
                        "total": len(resolved),
                        "saved": prodi_saved_count,
                    })

                if i % 10 == 0 or i <= 3 or i == len(resolved):
                    await send_progress(job_id, {
                        "phase": "prodi_detail",
                        "current": i,
                        "total": len(resolved),
                        "saved": prodi_saved_count,
                    })

                    await db.execute(
                        update(ScrapeJob).where(ScrapeJob.id == job_id).values(
                            total_prodi_detail=prodi_saved_count,
                            new_prodi_detail=prodi_saved_count,
                        )
                    )
                    await db.commit()

            await db.commit()
            await send_log(
                job_id, "success",
                f"✅ STEP 2 selesai: {prodi_saved_count} detail prodi tersimpan",
                db,
            )

            # STEP 3: Fetch dosen homebase
            await send_log(
                job_id, "info",
                f"📚 STEP 3: Mengambil dosen dari {len(resolved)} prodi × {len(semesters)} semester...",
                db
            )

            all_dosen = []
            seen_global = set()
            new_count = 0
            skip_count = 0

            for i, prodi in enumerate(resolved, 1):
                prodi_id = prodi["id"]
                rumpun = prodi["rumpun"]
                prodi_label = f"{prodi['nama']} ({prodi['jenjang']}) - {prodi['pt']}"

                prodi_new = 0

                for sem in semesters:
                    await asyncio.sleep(REQ_DELAY)
                    data = await asyncio.to_thread(
                        fetch_api, f"dosen/homebase/{prodi_id}?semester={sem}"
                    )
                    if not data or not isinstance(data, list):
                        continue

                    for d in data:
                        nidn = (d.get("nidn", "") or "").strip()
                        nama = (d.get("nama_dosen", "") or "").strip()
                        pt = prodi["pt"]

                        if nidn:
                            key = f"nidn:{nidn}"
                        elif nama:
                            key = f"nama:{nama.upper()}|pt:{pt.upper()}"
                        else:
                            continue

                        if key in seen_global:
                            skip_count += 1
                            continue
                        seen_global.add(key)
                        prodi_new += 1
                        new_count += 1

                        all_dosen.append({
                            "nidn": nidn,
                            "nama_dosen": nama,
                            "nuptk": (d.get("nuptk", "") or "").strip(),
                            "pendidikan": (d.get("pendidikan", "") or "").strip(),
                            "status_aktif": (d.get("status_aktif", "") or "").strip(),
                            "status_pegawai": (d.get("status_pegawai", "") or "").strip(),
                            "ikatan_kerja": (d.get("ikatan_kerja", "") or "").strip(),
                            "rumpun_prodi": rumpun,
                            "prodi_api": prodi["nama"],
                            "jenjang_prodi": prodi["jenjang"],
                            "pt_asal": prodi["pt"],
                        })

                if i % 10 == 0 or i <= 3 or i == len(resolved):
                    await send_log(
                        job_id, "info",
                        f"📊 [{i}/{len(resolved)}] {prodi_label} → "
                        f"{prodi_new} baru | Total: {len(all_dosen)}",
                        db
                    )
                    await send_progress(job_id, {
                        "phase": "fetch",
                        "current": i,
                        "total": len(resolved),
                        "total_dosen": len(all_dosen),
                        "new_dosen": new_count,
                        "skipped_dosen": skip_count,
                    })

                    await db.execute(
                        update(ScrapeJob).where(ScrapeJob.id == job_id).values(
                            total_dosen=len(all_dosen),
                            new_dosen=new_count,
                            skipped_dosen=skip_count,
                        )
                    )
                    await db.commit()

            await send_log(
                job_id, "success",
                f"✅ STEP 3 selesai: {len(all_dosen)} dosen unik ditemukan",
                db
            )

            # STEP 4: Fetch profiles & save to DB
            await send_log(job_id, "info", f"💾 STEP 4: Fetching profil & menyimpan ke database...", db)

            saved_count = 0
            for i, dosen_data in enumerate(all_dosen, 1):
                nidn = dosen_data["nidn"]
                nama = dosen_data["nama_dosen"]
                pt_name = dosen_data["pt_asal"]
                rumpun = dosen_data["rumpun_prodi"]

                # Check if already in DB
                if nidn:
                    existing = await db.execute(
                        select(Dosen).where(Dosen.nidn == nidn)
                    )
                    if existing.scalar_one_or_none():
                        skip_count += 1
                        continue

                # Get or create PT
                pt_result = await db.execute(
                    select(PerguruanTinggi).where(PerguruanTinggi.nama == pt_name)
                )
                pt_obj = pt_result.scalar_one_or_none()
                if not pt_obj:
                    pt_obj = PerguruanTinggi(nama=pt_name)
                    db.add(pt_obj)
                    await db.flush()

                # Get or create Prodi
                prodi_api_name = dosen_data["prodi_api"]
                prodi_result = await db.execute(
                    select(ProgramStudi).where(
                        ProgramStudi.nama == prodi_api_name,
                        ProgramStudi.pt_id == pt_obj.id,
                    )
                )
                prodi_obj = prodi_result.scalar_one_or_none()
                if not prodi_obj:
                    prodi_obj = ProgramStudi(
                        nama=prodi_api_name,
                        rumpun=rumpun,
                        jenjang=dosen_data["jenjang_prodi"],
                        pt_id=pt_obj.id,
                    )
                    db.add(prodi_obj)
                    await db.flush()

                # Fetch profile detail
                profile_data = {}
                search_key = nidn if nidn else nama
                if search_key:
                    await asyncio.sleep(REQ_DELAY)
                    search_results = await asyncio.to_thread(
                        fetch_api, f"pencarian/dosen/{search_key}"
                    )
                    if search_results and isinstance(search_results, list):
                        search_id = None
                        for sr in search_results:
                            if nidn and sr.get("nidn", "") == nidn:
                                search_id = sr.get("id", "")
                                break
                            if not search_id and sr.get("nama", "").upper() == nama.upper():
                                search_id = sr.get("id", "")
                        if not search_id and search_results:
                            search_id = search_results[0].get("id", "")

                        if search_id:
                            await asyncio.sleep(REQ_DELAY)
                            profile = await asyncio.to_thread(fetch_api, f"dosen/profile/{search_id}")
                            if profile and isinstance(profile, dict):
                                profile_data = profile

                # Create dosen record
                dosen_obj = Dosen(
                    pddikti_id=profile_data.get("id"),
                    nidn=nidn or None,
                    nuptk=dosen_data["nuptk"] or None,
                    nama=profile_data.get("nama_dosen", nama),
                    jenis_kelamin=profile_data.get("jenis_kelamin", ""),
                    jabatan_fungsional=profile_data.get("jabatan_akademik", ""),
                    pendidikan_terakhir=profile_data.get(
                        "pendidikan_tertinggi",
                        dosen_data["pendidikan"]
                    ),
                    status_ikatan_kerja=profile_data.get(
                        "status_ikatan_kerja",
                        dosen_data["ikatan_kerja"]
                    ),
                    status_aktivitas=profile_data.get(
                        "status_aktivitas",
                        dosen_data["status_aktif"]
                    ),
                    pt_id=pt_obj.id,
                    prodi_id=prodi_obj.id,
                    rumpun_prodi=rumpun,
                )
                db.add(dosen_obj)
                saved_count += 1

                if saved_count % 50 == 0:
                    await db.commit()
                    await send_log(
                        job_id, "info",
                        f"💾 Tersimpan: {saved_count}/{len(all_dosen)} dosen",
                        db
                    )
                    await send_progress(job_id, {
                        "phase": "save",
                        "current": i,
                        "total": len(all_dosen),
                        "saved": saved_count,
                    })

            await db.commit()

            # Complete job
            elapsed = ""
            job_result = await db.execute(select(ScrapeJob).where(ScrapeJob.id == job_id))
            job_obj = job_result.scalar_one_or_none()
            if job_obj and job_obj.started_at:
                diff = datetime.now(timezone.utc) - job_obj.started_at.replace(tzinfo=timezone.utc)
                minutes = int(diff.total_seconds() // 60)
                seconds = int(diff.total_seconds() % 60)
                elapsed = f"{minutes}m{seconds}s"

            await db.execute(
                update(ScrapeJob).where(ScrapeJob.id == job_id).values(
                    status="completed",
                    completed_at=datetime.now(timezone.utc),
                    total_dosen=saved_count,
                    new_dosen=saved_count,
                    skipped_dosen=skip_count,
                    total_prodi_detail=prodi_saved_count,
                    new_prodi_detail=prodi_saved_count,
                )
            )
            await db.commit()

            await send_log(
                job_id, "success",
                f"🎉 Scraping selesai! {saved_count} dosen + {prodi_saved_count} prodi tersimpan ({elapsed})",
                db
            )

            from routers.scrape_router import broadcast_to_job
            await broadcast_to_job(job_id, {
                "type": "done",
                "total_dosen": saved_count,
                "total_prodi_detail": prodi_saved_count,
                "new": saved_count,
                "skipped": skip_count,
                "elapsed": elapsed,
            })

        except asyncio.CancelledError:
            await send_log(job_id, "warning", "⚠️ Scraping dibatalkan", db)
            await db.execute(
                update(ScrapeJob).where(ScrapeJob.id == job_id).values(
                    status="cancelled",
                    completed_at=datetime.now(timezone.utc),
                    error_message="Dibatalkan",
                )
            )
            await db.commit()

        except Exception as e:
            error_msg = str(e)
            await send_log(job_id, "error", f"❌ Error: {error_msg}", db)
            await _fail_job(db, job_id, error_msg)


async def _fail_job(db: AsyncSession, job_id: int, error_msg: str):
    await db.execute(
        update(ScrapeJob).where(ScrapeJob.id == job_id).values(
            status="failed",
            completed_at=datetime.now(timezone.utc),
            error_message=error_msg,
        )
    )
    await db.commit()

    from routers.scrape_router import broadcast_to_job
    await broadcast_to_job(job_id, {
        "type": "error",
        "message": error_msg,
    })
