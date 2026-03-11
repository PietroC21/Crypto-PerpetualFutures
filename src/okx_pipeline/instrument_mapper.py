"""
Maps each coin to its OKX instrument IDs for spot and perpetual swap markets.
Verifies instrument existence via the OKX public instruments API.
"""

from .okx_client import OKXClient
from . import config
from .utils import setup_logger

logger = setup_logger("instrument_mapper")


def map_instruments(client: OKXClient, coins: list[str] | None = None) -> dict:
    """
    Query the OKX instruments API and map each coin to its correct instId.

    Returns:
        dict of the form:
        {
            "BTC": {
                "spot": "BTC-USDT",
                "perp": "BTC-USDT-SWAP",
                "spot_live": True,
                "perp_live": True,
            },
            ...
        }
    """
    coins = coins or config.COINS
    coins_upper = {c.upper() for c in coins}

    # Fetch all live SPOT instruments
    logger.info("Fetching SPOT instruments from OKX...")
    spot_instruments = client.get_instruments("SPOT")
    spot_by_coin = {}
    for inst in spot_instruments:
        inst_id = inst["instId"]  # e.g. "BTC-USDT"
        parts = inst_id.split("-")
        if len(parts) == 2 and parts[1] == "USDT" and parts[0] in coins_upper:
            spot_by_coin[parts[0]] = {
                "instId": inst_id,
                "state": inst.get("state", "unknown"),
            }

    # Fetch all live SWAP instruments
    logger.info("Fetching SWAP instruments from OKX...")
    swap_instruments = client.get_instruments("SWAP")
    perp_by_coin = {}
    for inst in swap_instruments:
        inst_id = inst["instId"]  # e.g. "BTC-USDT-SWAP"
        parts = inst_id.split("-")
        if (
            len(parts) == 3
            and parts[1] == "USDT"
            and parts[2] == "SWAP"
            and parts[0] in coins_upper
        ):
            perp_by_coin[parts[0]] = {
                "instId": inst_id,
                "instFamily": inst.get("instFamily", f"{parts[0]}-USDT"),
                "state": inst.get("state", "unknown"),
                "listTime": inst.get("listTime", ""),
            }

    # Build the mapping
    result = {}
    for coin in coins:
        c = coin.upper()
        spot_info = spot_by_coin.get(c)
        perp_info = perp_by_coin.get(c)

        result[c] = {
            "spot": spot_info["instId"] if spot_info else f"{c}-USDT",
            "perp": perp_info["instId"] if perp_info else f"{c}-USDT-SWAP",
            "perp_family": perp_info["instFamily"] if perp_info else f"{c}-USDT",
            "spot_live": spot_info["state"] == "live" if spot_info else False,
            "perp_live": perp_info["state"] == "live" if perp_info else False,
            "perp_list_time": perp_info.get("listTime", "") if perp_info else "",
        }

        status = "OK" if result[c]["spot_live"] and result[c]["perp_live"] else "WARN"
        logger.info(
            f"  {c}: spot={result[c]['spot']} "
            f"(live={result[c]['spot_live']}), "
            f"perp={result[c]['perp']} "
            f"(live={result[c]['perp_live']}) [{status}]"
        )

    missing_spot = [c for c in coins if not result[c.upper()]["spot_live"]]
    missing_perp = [c for c in coins if not result[c.upper()]["perp_live"]]
    if missing_spot:
        logger.warning(f"Missing/inactive SPOT instruments: {missing_spot}")
    if missing_perp:
        logger.warning(f"Missing/inactive SWAP instruments: {missing_perp}")

    return result
