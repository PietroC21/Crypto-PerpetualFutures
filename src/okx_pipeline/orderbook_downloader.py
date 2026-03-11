"""
Download historical L2 order book snapshots for OKX spot and perpetual instruments.

The ONLY source for historical order book data is the OKX historical data
export system (https://www.okx.com/en-us/historical-data). This system uses
a job-based API:

    POST /priapi/v5/broker/public/trade-data/download-link

This endpoint requires browser session cookies for authentication.
Without auth, it returns successfully (code 0) but with empty download details.

Approach:
1. If session cookies are provided, use the export API to request download links.
2. Download the ZIP files from the returned URLs.
3. Extract and organize into per-instrument, per-date CSV files.
4. As a fallback, provide a current-snapshot collector for going forward.

Note: Historical order book data is available from March 2023 onward.
"""

import io
import zipfile
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm

from .okx_client import OKXClient
from .chunking import StateManager, generate_date_chunks
from . import config
from .utils import setup_logger, safe_filename, datetime_to_ts_ms

logger = setup_logger("orderbook_downloader")


def download_orderbook_export(
    client: OKXClient,
    inst_family: str,
    inst_type: str,
    output_dir: Path,
    state: StateManager,
    start_date: str = config.ORDERBOOK_START,
    end_date: str | None = None,
    chunk_days: int = config.DEFAULT_CHUNK_DAYS,
    depth: int = config.ORDERBOOK_DEPTH,
) -> int:
    """
    Download historical order book data using the export API.

    Args:
        client: OKXClient (should have session cookies set)
        inst_family: Instrument family, e.g. "BTC-USDT"
        inst_type: "SPOT" or "SWAP"
        output_dir: Directory to save extracted data
        state: StateManager for resume
        start_date: Start date string "YYYY-MM-DD"
        end_date: End date string (defaults to yesterday)
        chunk_days: Days per chunk (default 14)
        depth: Order book depth (400 or 5000)

    Returns:
        Number of files downloaded
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    data_type = f"orderbook_{inst_type.lower()}"

    if state.is_complete(data_type, inst_family):
        logger.info(f"[{inst_family}/{inst_type}] Order book already complete. Skipping.")
        return 0

    end_date = end_date or (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")

    # Choose module based on depth
    module = config.MODULE_ORDER_BOOK_400 if depth <= 400 else config.MODULE_ORDER_BOOK_5000

    chunks = generate_date_chunks(start_date, end_date, chunk_days)
    completed = set(state.get_completed_chunks(data_type, inst_family))
    total_files = 0
    consecutive_empty = 0

    logger.info(
        f"[{inst_family}/{inst_type}] Starting order book download: "
        f"{len(chunks)} chunks, depth={depth}"
    )

    for chunk_start, chunk_end in tqdm(
        chunks, desc=f"{inst_family} {inst_type} OB", unit=" chunks"
    ):
        chunk_id = f"{chunk_start.strftime('%Y%m%d')}_{chunk_end.strftime('%Y%m%d')}"
        if chunk_id in completed:
            continue

        begin_ms = datetime_to_ts_ms(chunk_start)
        end_ms = datetime_to_ts_ms(chunk_end)

        try:
            result = client.export_request_download(
                module=module,
                inst_type=inst_type,
                inst_families=[inst_family],
                begin_ms=begin_ms,
                end_ms=end_ms,
            )
        except Exception as e:
            logger.error(
                f"[{inst_family}/{inst_type}] Export request failed for "
                f"{chunk_id}: {e}"
            )
            # Reduce chunk size on failure
            if chunk_days > config.MIN_CHUNK_DAYS:
                logger.info(f"Reducing chunk size and retrying...")
                sub_downloaded = _retry_with_smaller_chunks(
                    client, inst_family, inst_type, output_dir, state,
                    chunk_start, chunk_end, chunk_days // 2, module
                )
                total_files += sub_downloaded
            continue

        details = result.get("details", [])
        if not details:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                logger.warning(
                    f"[{inst_family}/{inst_type}] {consecutive_empty} consecutive "
                    f"empty responses. This likely means authentication is required. "
                    f"Provide OKX session cookies to the OKXClient."
                )
            # Still mark the chunk to avoid re-requesting
            state.mark_chunk_done(data_type, inst_family, chunk_id)
            continue

        consecutive_empty = 0

        # Download each file from the details
        for detail in details:
            for group_detail in detail.get("groupDetails", []):
                url = group_detail.get("url")
                if not url:
                    continue
                try:
                    content = client.download_file(url)
                    n = _extract_and_save_orderbook(
                        content, inst_family, inst_type, output_dir
                    )
                    total_files += n
                except Exception as e:
                    logger.error(f"[{inst_family}] Download failed: {url}: {e}")

        state.mark_chunk_done(data_type, inst_family, chunk_id)

    if total_files > 0:
        state.mark_complete(data_type, inst_family)

    logger.info(
        f"[{inst_family}/{inst_type}] Order book download done: {total_files} files"
    )
    return total_files


def _retry_with_smaller_chunks(
    client, inst_family, inst_type, output_dir, state,
    start, end, chunk_days, module
):
    """Retry a failed chunk with smaller sub-chunks."""
    data_type = f"orderbook_{inst_type.lower()}"
    sub_chunks = generate_date_chunks(start, end, chunk_days)
    downloaded = 0

    for sub_start, sub_end in sub_chunks:
        chunk_id = f"{sub_start.strftime('%Y%m%d')}_{sub_end.strftime('%Y%m%d')}"
        if chunk_id in state.get_completed_chunks(data_type, inst_family):
            continue

        try:
            result = client.export_request_download(
                module=module,
                inst_type=inst_type,
                inst_families=[inst_family],
                begin_ms=datetime_to_ts_ms(sub_start),
                end_ms=datetime_to_ts_ms(sub_end),
            )
            for detail in result.get("details", []):
                for gd in detail.get("groupDetails", []):
                    url = gd.get("url")
                    if url:
                        content = client.download_file(url)
                        downloaded += _extract_and_save_orderbook(
                            content, inst_family, inst_type, output_dir
                        )
            state.mark_chunk_done(data_type, inst_family, chunk_id)
        except Exception as e:
            logger.error(f"Sub-chunk {chunk_id} failed: {e}")
            if chunk_days > config.MIN_CHUNK_DAYS:
                downloaded += _retry_with_smaller_chunks(
                    client, inst_family, inst_type, output_dir, state,
                    sub_start, sub_end, max(chunk_days // 2, 1), module
                )

    return downloaded


def _extract_and_save_orderbook(
    content: bytes, inst_family: str, inst_type: str, output_dir: Path
) -> int:
    """
    Extract order book data from downloaded content (ZIP or CSV).
    Save as per-date CSV files.
    Returns number of files saved.
    """
    saved = 0
    try:
        zf = zipfile.ZipFile(io.BytesIO(content))
        for name in zf.namelist():
            if name.endswith(".csv"):
                csv_data = zf.read(name).decode("utf-8", errors="replace")
                # Try to extract date from filename
                date_str = _extract_date_from_filename(name)
                market = "spot" if inst_type == "SPOT" else "swap"
                out_name = (
                    f"{safe_filename(inst_family)}_{market}_orderbook"
                    f"{'_' + date_str if date_str else ''}.csv"
                )
                out_path = output_dir / out_name
                with open(out_path, "w") as f:
                    f.write(csv_data)
                saved += 1
    except zipfile.BadZipFile:
        # Might be a raw CSV, not a ZIP
        try:
            text = content.decode("utf-8", errors="replace")
            if text.strip():
                market = "spot" if inst_type == "SPOT" else "swap"
                out_name = f"{safe_filename(inst_family)}_{market}_orderbook.csv"
                out_path = output_dir / out_name
                with open(out_path, "w") as f:
                    f.write(text)
                saved += 1
        except Exception as e:
            logger.error(f"Could not parse content as CSV: {e}")

    return saved


def _extract_date_from_filename(filename: str) -> str:
    """Try to extract a date string from a filename."""
    import re
    match = re.search(r"(\d{4}[-_]?\d{2}[-_]?\d{2})", filename)
    if match:
        return match.group(1).replace("-", "_")
    return ""


# ── Current Snapshot Collector ───────────────────────────────────────────────

def collect_current_orderbook(
    client: OKXClient,
    inst_id: str,
    inst_type: str,
    output_dir: Path | None = None,
    depth: int = config.ORDERBOOK_DEPTH,
) -> pd.DataFrame:
    """
    Grab the CURRENT order book snapshot from the REST API.
    Useful for real-time data collection going forward.

    The REST API only returns the live order book, not historical data.
    For historical data, use download_orderbook_export() with session cookies.
    """
    if inst_type == "SPOT":
        output_dir = output_dir or config.SPOT_OB_DIR
    else:
        output_dir = output_dir or config.PERP_OB_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"[{inst_id}] Collecting current order book snapshot...")
    book = client.get_orderbook(inst_id, depth=depth)

    if not book:
        logger.warning(f"[{inst_id}] Empty order book response")
        return pd.DataFrame()

    ts = book.get("ts", "")
    asks = book.get("asks", [])
    bids = book.get("bids", [])

    rows = []
    for level in asks:
        rows.append({
            "timestamp": ts,
            "side": "ask",
            "price": float(level[0]),
            "quantity": float(level[1]),
            "num_orders": int(level[3]) if len(level) > 3 else 0,
            "instId": inst_id,
        })
    for level in bids:
        rows.append({
            "timestamp": ts,
            "side": "bid",
            "price": float(level[0]),
            "quantity": float(level[1]),
            "num_orders": int(level[3]) if len(level) > 3 else 0,
            "instId": inst_id,
        })

    df = pd.DataFrame(rows)

    # Save snapshot
    now = datetime.utcnow()
    date_str = now.strftime("%Y_%m_%d")
    market = "spot" if inst_type == "SPOT" else "swap"
    fname = f"{safe_filename(inst_id)}_{market}_orderbook_{date_str}.csv"
    df.to_csv(output_dir / fname, index=False)

    logger.info(
        f"[{inst_id}] Snapshot saved: {len(asks)} asks, {len(bids)} bids"
    )
    return df


def download_all_orderbooks(
    client: OKXClient,
    instrument_map: dict,
    state: StateManager | None = None,
    start_date: str = config.ORDERBOOK_START,
    method: str = "export",
) -> dict:
    """
    Download order book data for all coins, both spot and perp.

    Args:
        client: OKXClient instance
        instrument_map: Output from instrument_mapper.map_instruments()
        state: StateManager for resume
        start_date: Start date for historical data
        method: "export" for historical export API, "snapshot" for current snapshot

    Returns:
        Dict of results keyed by "{coin}_{type}"
    """
    state = state or StateManager()
    results = {}

    for coin in config.COINS:
        info = instrument_map.get(coin, {})

        # -- Spot order book --
        spot_id = info.get("spot", f"{coin}-USDT")
        spot_family = f"{coin}-USDT"
        try:
            if method == "export":
                n = download_orderbook_export(
                    client, spot_family, "SPOT", config.SPOT_OB_DIR, state,
                    start_date=start_date,
                )
                results[f"{coin}_spot"] = n
            else:
                df = collect_current_orderbook(client, spot_id, "SPOT")
                results[f"{coin}_spot"] = df
        except Exception as e:
            logger.error(f"[{coin}] Spot order book failed: {e}")
            results[f"{coin}_spot"] = None

        # -- Perp order book --
        perp_id = info.get("perp", f"{coin}-USDT-SWAP")
        perp_family = info.get("perp_family", f"{coin}-USDT")
        try:
            if method == "export":
                n = download_orderbook_export(
                    client, perp_family, "SWAP", config.PERP_OB_DIR, state,
                    start_date=start_date,
                )
                results[f"{coin}_perp"] = n
            else:
                df = collect_current_orderbook(client, perp_id, "SWAP")
                results[f"{coin}_perp"] = df
        except Exception as e:
            logger.error(f"[{coin}] Perp order book failed: {e}")
            results[f"{coin}_perp"] = None

    return results
