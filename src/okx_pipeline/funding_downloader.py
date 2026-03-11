"""
Download historical funding rates for OKX perpetual swap instruments.

Data sources & coverage:
  - CDN bulk download: March 2022 → ~Sept 2025 (daily ZIP files, all instruments)
  - REST API:          ~Dec 2025 → present     (per-instrument, paginated)

The pipeline combines both sources to produce a single CSV per instrument
covering the maximum available date range.

Strategy:
  1. Download CDN daily files (each contains ALL instruments for one day).
     Extract all target coins in a single pass — very efficient.
  2. Fill in recent data from the REST API (paginate backward from present).
  3. Merge, deduplicate, and save one CSV per instrument.
"""

import pandas as pd
from datetime import datetime
from pathlib import Path
from tqdm import tqdm

from .okx_client import OKXClient
from .chunking import StateManager, generate_monthly_periods
from . import config
from .utils import setup_logger, safe_filename

logger = setup_logger("funding_downloader")

# The target instrument IDs we want to extract from CDN files
TARGET_SWAP_IDS = {f"{c}-USDT-SWAP" for c in config.COINS}


# ═════════════════════════════════════════════════════════════════════════════
#  PHASE 1: CDN Bulk Download (historical data, March 2022 → ~Sept 2025)
# ═════════════════════════════════════════════════════════════════════════════

def download_funding_rates_cdn_bulk(
    client: OKXClient,
    output_dir: Path | None = None,
    state: StateManager | None = None,
    start_date: str = config.FUNDING_RATE_START,
) -> dict[str, pd.DataFrame]:
    """
    Download ALL funding rate data from CDN in one pass.
    Each daily ZIP file contains every instrument. We extract only our target
    coins, saving one CSV per instrument.

    Returns:
        Dict mapping instId -> DataFrame
    """
    output_dir = output_dir or config.FUNDING_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    state = state or StateManager()

    if state.is_complete("funding_cdn", "ALL"):
        logger.info("CDN funding bulk download already complete. Loading from disk.")
        return _load_existing_funding_csvs(output_dir, suffix="_funding_cdn.csv")

    logger.info("Starting CDN bulk funding rate download...")
    end_date = datetime.utcnow().strftime("%Y-%m-%d")
    months = generate_monthly_periods(start_date, end_date)
    completed_months = set(state.get_completed_chunks("funding_cdn", "ALL"))

    # Accumulate records per instrument
    records_by_inst: dict[str, list[dict]] = {iid: [] for iid in TARGET_SWAP_IDS}
    total_files = 0
    skipped_months = 0

    for month in tqdm(months, desc="CDN funding months", unit=" months"):
        if month in completed_months:
            skipped_months += 1
            continue

        cdn_path = f"{config.CDN_SWAPRATE_PATH}/{month}"
        try:
            files = client.cdn_list_all_files(cdn_path)
        except Exception as e:
            logger.debug(f"CDN list for {month}: {e}")
            # Month might not exist yet — mark as done (empty) to skip next time
            state.mark_chunk_done("funding_cdn", "ALL", month)
            continue

        if not files:
            state.mark_chunk_done("funding_cdn", "ALL", month)
            continue

        for file_info in files:
            fname = file_info["fileName"]
            try:
                csv_text = client.cdn_download_and_extract_csv(cdn_path, fname)
                _parse_cdn_csv(csv_text, records_by_inst)
                total_files += 1
            except Exception as e:
                logger.warning(f"CDN file {month}/{fname} failed: {e}")

        state.mark_chunk_done("funding_cdn", "ALL", month)

    logger.info(
        f"CDN download: {total_files} files processed, "
        f"{skipped_months} months skipped (already done)"
    )

    # Save per-instrument CSVs
    results = {}
    for inst_id, records in records_by_inst.items():
        if not records:
            continue
        df = pd.DataFrame(records)
        df["timestamp"] = df["timestamp"].astype(int)
        df = df.sort_values("timestamp").drop_duplicates(
            subset=["timestamp", "instId"]
        )
        csv_path = output_dir / f"{safe_filename(inst_id)}_funding_cdn.csv"
        df.to_csv(csv_path, index=False)
        results[inst_id] = df
        logger.info(f"  {inst_id}: {len(df)} CDN records saved")

    state.mark_complete("funding_cdn", "ALL")
    return results


def _parse_cdn_csv(csv_text: str, records_by_inst: dict[str, list[dict]]):
    """
    Parse a CDN swaprate CSV and extract records for target instruments.

    CDN CSV format (header has Chinese + English labels):
      instrument_name, contract_type, funding_rate, real_funding_rate, funding_time
      BTC-USDT-SWAP,  SWAP,          0.0001,       0.0001,            1646121600000
    """
    lines = csv_text.strip().split("\n")
    if len(lines) < 2:
        return

    for line in lines[1:]:  # skip header
        parts = line.strip().split(",")
        if len(parts) < 5:
            continue
        inst_name = parts[0].strip()
        if inst_name in records_by_inst:
            records_by_inst[inst_name].append({
                "instId": inst_name,
                "funding_rate": parts[2].strip(),
                "realized_rate": parts[3].strip(),
                "timestamp": parts[4].strip(),
            })


# ═════════════════════════════════════════════════════════════════════════════
#  PHASE 2: REST API (recent data, ~Dec 2025 → present)
# ═════════════════════════════════════════════════════════════════════════════

def download_funding_rate_rest(
    client: OKXClient,
    inst_id: str,
    output_dir: Path | None = None,
    state: StateManager | None = None,
) -> pd.DataFrame:
    """
    Download funding rate history for one instrument via REST API.
    Paginates backward from the most recent record.

    Note: The REST API only retains ~3 months of history.
    """
    output_dir = output_dir or config.FUNDING_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    state = state or StateManager()

    rest_key = f"funding_rest:{inst_id}"
    if state.is_complete("funding_rest", inst_id):
        logger.info(f"[{inst_id}] REST funding already complete. Skipping.")
        csv_path = output_dir / f"{safe_filename(inst_id)}_funding_rest.csv"
        if csv_path.exists():
            return pd.read_csv(csv_path)
        return pd.DataFrame()

    logger.info(f"[{inst_id}] Downloading funding rates via REST API...")

    all_records = []
    after = None
    page = 0

    pbar = tqdm(desc=f"{inst_id} REST", unit=" records")

    while True:
        try:
            batch = client.get_funding_rate_history(
                inst_id, after=after, limit=config.FUNDING_PAGE_SIZE
            )
        except Exception as e:
            logger.error(f"[{inst_id}] REST API error on page {page}: {e}")
            break

        if not batch:
            break

        for rec in batch:
            all_records.append({
                "timestamp": rec["fundingTime"],
                "funding_rate": rec["fundingRate"],
                "realized_rate": rec.get("realizedRate", ""),
                "instId": rec["instId"],
            })

        after = batch[-1]["fundingTime"]
        page += 1
        pbar.update(len(batch))

        if len(batch) < config.FUNDING_PAGE_SIZE:
            break

    pbar.close()

    if not all_records:
        logger.warning(f"[{inst_id}] No REST API data retrieved.")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df["timestamp"] = df["timestamp"].astype(int)
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp", "instId"])

    csv_path = output_dir / f"{safe_filename(inst_id)}_funding_rest.csv"
    df.to_csv(csv_path, index=False)
    state.mark_complete("funding_rest", inst_id)

    logger.info(
        f"[{inst_id}] REST complete: {len(df)} records "
        f"({pd.to_datetime(df['timestamp'].min(), unit='ms')} to "
        f"{pd.to_datetime(df['timestamp'].max(), unit='ms')})"
    )
    return df


# ═════════════════════════════════════════════════════════════════════════════
#  PHASE 3: Merge CDN + REST into final per-instrument files
# ═════════════════════════════════════════════════════════════════════════════

def merge_funding_sources(
    output_dir: Path | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Merge CDN and REST data for each instrument into a single
    deduplicated, sorted CSV.

    Output files: {COIN}_USDT_SWAP_funding.csv
    """
    output_dir = output_dir or config.FUNDING_DIR
    results = {}

    for coin in config.COINS:
        inst_id = f"{coin}-USDT-SWAP"
        safe_id = safe_filename(inst_id)

        cdn_path = output_dir / f"{safe_id}_funding_cdn.csv"
        rest_path = output_dir / f"{safe_id}_funding_rest.csv"
        final_path = output_dir / f"{safe_id}_funding.csv"

        frames = []
        if cdn_path.exists():
            frames.append(pd.read_csv(cdn_path))
        if rest_path.exists():
            frames.append(pd.read_csv(rest_path))

        if not frames:
            logger.warning(f"[{inst_id}] No funding data from any source.")
            results[coin] = pd.DataFrame()
            continue

        df = pd.concat(frames, ignore_index=True)
        df["timestamp"] = df["timestamp"].astype(int)
        df = df.sort_values("timestamp").drop_duplicates(
            subset=["timestamp", "instId"]
        )
        df.to_csv(final_path, index=False)
        results[coin] = df

        # Report coverage
        ts_min = pd.to_datetime(df["timestamp"].min(), unit="ms")
        ts_max = pd.to_datetime(df["timestamp"].max(), unit="ms")

        # Detect gaps > 24 hours
        diffs = df["timestamp"].diff().dropna()
        max_gap_h = diffs.max() / 3_600_000
        gap_warning = f" ⚠ max gap: {max_gap_h:.0f}h" if max_gap_h > 24 else ""

        logger.info(
            f"  {coin}: {len(df):,} records, {ts_min} → {ts_max}{gap_warning}"
        )

    return results


# ═════════════════════════════════════════════════════════════════════════════
#  Top-level orchestrator
# ═════════════════════════════════════════════════════════════════════════════

def download_all_funding_rates(
    client: OKXClient,
    instrument_map: dict,
    state: StateManager | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Full pipeline: CDN bulk → REST supplement → merge.

    Args:
        client: OKXClient instance
        instrument_map: Output from instrument_mapper.map_instruments()
        state: StateManager for resume

    Returns:
        Dict mapping coin -> merged DataFrame
    """
    state = state or StateManager()

    # Phase 1: CDN bulk download (all coins at once)
    logger.info("=" * 60)
    logger.info("PHASE 1: CDN Bulk Download (historical)")
    logger.info("=" * 60)
    cdn_results = download_funding_rates_cdn_bulk(client, state=state)

    # Phase 2: REST API (each coin individually)
    logger.info("=" * 60)
    logger.info("PHASE 2: REST API Download (recent)")
    logger.info("=" * 60)
    for coin in config.COINS:
        inst_id = f"{coin}-USDT-SWAP"
        try:
            download_funding_rate_rest(client, inst_id, state=state)
        except Exception as e:
            logger.error(f"[{coin}] REST download failed: {e}")

    # Phase 3: Merge
    logger.info("=" * 60)
    logger.info("PHASE 3: Merging CDN + REST data")
    logger.info("=" * 60)
    merged = merge_funding_sources()

    return merged


# ── Helpers ──────────────────────────────────────────────────────────────────

def _load_existing_funding_csvs(
    output_dir: Path, suffix: str
) -> dict[str, pd.DataFrame]:
    """Load existing CDN funding CSVs from disk."""
    results = {}
    for inst_id in TARGET_SWAP_IDS:
        csv_path = output_dir / f"{safe_filename(inst_id)}{suffix}"
        if csv_path.exists():
            results[inst_id] = pd.read_csv(csv_path)
    return results
