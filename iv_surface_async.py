"""
Production async IV surface support for the Options Simulator.

Provides:
- /api/iv-surface
- /api/iv-surface/status/<job_id>
- Redis cache for completed IV surface payloads
- Redis in-flight job lookup to avoid duplicate jobs
- Kafka producer integration from Flask route
- Kafka worker helpers

This file is intentionally dependency-injected from app.py so it does not need
to import your whole app during route registration.
"""

from __future__ import annotations

import json
import os
import time
import uuid
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from flask import jsonify, request


logger = logging.getLogger(__name__)

IV_SURFACE_TOPIC = os.getenv("IV_SURFACE_TOPIC", "iv_surface_requests")
IV_SURFACE_CACHE_TTL_SECONDS = int(os.getenv("IV_SURFACE_CACHE_TTL_SECONDS", "3600"))
IV_SURFACE_JOB_TTL_SECONDS = int(os.getenv("IV_SURFACE_JOB_TTL_SECONDS", "600"))
IV_SURFACE_MAX_EXPIRIES = int(os.getenv("IV_SURFACE_MAX_EXPIRIES", "5"))
IV_SURFACE_MAX_WORKERS = int(os.getenv("IV_SURFACE_MAX_WORKERS", "4"))


# =========================================================
# REDIS / KEY HELPERS
# =========================================================

def redis_get_json(redis_client: Any, key: str) -> Optional[Dict[str, Any]]:
    value = redis_client.get(key)

    if not value:
        return None

    if isinstance(value, bytes):
        value = value.decode("utf-8")

    return json.loads(value)


def redis_set_json(redis_client: Any, key: str, value: Dict[str, Any], ex: int) -> None:
    redis_client.setex(
        key,
        ex,
        json.dumps(value, separators=(",", ":"), default=str),
    )


def iv_surface_cache_key(
    dataset: str,
    query_date: str,
    query_time: str,
    interval: int,
    strike_count: int,
    max_months: int,
) -> str:
    return (
        f"iv_surface:{dataset}:{query_date}:{query_time}:"
        f"{int(interval)}:{int(strike_count)}:{int(max_months)}"
    )


def job_lookup_key(cache_key: str) -> str:
    return f"job_lookup:{cache_key}"


def job_key(job_id: str) -> str:
    return f"job:iv_surface:{job_id}"


# =========================================================
# IV SURFACE BUILDER
# =========================================================

def build_iv_surface_payload(
    *,
    dataset: str,
    query_date: str,
    query_time: str,
    interval: int,
    strike_count: int,
    max_months: int,
    build_option_chain_snapshot: Callable[..., Dict[str, Any]],
    get_available_expiries_for_date: Callable[..., Iterable[Dict[str, Any]]],
    safe_float: Callable[[Any], Optional[float]],
) -> Dict[str, Any]:
    all_expiries = list(
        get_available_expiries_for_date(
            query_date=query_date,
            dataset=dataset,
            max_months=max_months,
        )
    )

    expiries = all_expiries[:IV_SURFACE_MAX_EXPIRIES]

    if not expiries:
        return {
            "ok": True,
            "status": "ready",
            "dataset": dataset,
            "query_date": query_date,
            "query_time": query_time,
            "interval": interval,
            "expiries": [],
            "strikes": [],
            "chain_source": "none",
            "rows": [],
        }

    def process_expiry(exp: Dict[str, Any]) -> Tuple[list[Dict[str, Any]], set[int], str]:
        expiry_value = exp["value"]
        expiry_label = exp["label"]

        expiry_started = time.monotonic()
        snap = build_option_chain_snapshot(
            query_date=query_date,
            query_time=query_time,
            dataset=dataset,
            expiry_rule="current expiry",
            strike_count_each_side=strike_count,
            candle_interval_minutes=interval,
            selected_expiry=expiry_value,
            # The surface only needs smile IV per strike; skip the Greek passes.
            compute_greeks=False,
        )
        chain_source = snap.get("chain_source", "unknown")
        logger.info(
            "IV surface expiry %s built in %.2fs via %s",
            expiry_value,
            time.monotonic() - expiry_started,
            chain_source,
        )

        atm = safe_float(snap.get("atm")) or 0.0
        local_rows: list[Dict[str, Any]] = []
        local_strikes: set[int] = set()

        for row in snap.get("rows", []):
            strike = row.get("strike")

            if strike is None:
                continue

            strike = int(strike)
            local_strikes.add(strike)

            ce = safe_float(row.get("ce_iv"))
            pe = safe_float(row.get("pe_iv"))

            if ce is not None and ce <= 1:
                ce *= 100

            if pe is not None and pe <= 1:
                pe *= 100

            if atm and strike < atm:
                smile_iv = pe
            elif atm and strike > atm:
                smile_iv = ce
            else:
                vals = [v for v in (ce, pe) if v is not None]
                smile_iv = sum(vals) / len(vals) if vals else None

            if smile_iv is None:
                continue

            local_rows.append({
                "expiry": expiry_value,
                "expiry_label": expiry_label,
                "strike": strike,
                "iv": round(float(smile_iv), 4),
                "atm": strike == int(atm) if atm else False,
            })

        return local_rows, local_strikes, chain_source

    surface_rows: list[Dict[str, Any]] = []
    strikes_set: set[int] = set()
    sources: set[str] = set()

    worker_count = min(
        len(expiries),
        max(1, IV_SURFACE_MAX_WORKERS),
        max(1, os.cpu_count() or 1),
    )

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {executor.submit(process_expiry, exp): exp for exp in expiries}

        for future in as_completed(futures):
            exp = futures[future]

            try:
                rows, strikes, source = future.result()
                surface_rows.extend(rows)
                strikes_set.update(strikes)
                sources.add(source)

            except Exception as exc:
                logger.exception("IV surface expiry failed: %s", exp)
                surface_rows.append({
                    "expiry": exp.get("value"),
                    "expiry_label": exp.get("label"),
                    "error": str(exc),
                })

    # Single source if every expiry agreed, else "mixed" (some consolidated,
    # some fell back to per-contract reads).
    if not sources:
        chain_source = "none"
    elif len(sources) == 1:
        chain_source = next(iter(sources))
    else:
        chain_source = "mixed"

    surface_rows.sort(
        key=lambda r: (
            str(r.get("expiry", "")),
            int(r.get("strike", 0)) if r.get("strike") is not None else 0,
        )
    )

    return {
        "ok": True,
        "status": "ready",
        "dataset": dataset,
        "query_date": query_date,
        "query_time": query_time,
        "interval": interval,
        "expiries": expiries,
        "strikes": sorted(strikes_set),
        "chain_source": chain_source,
        "rows": surface_rows,
    }


# =========================================================
# FLASK ROUTE REGISTRATION
# =========================================================

def register_iv_surface_routes(
    *,
    app: Any,
    redis_client: Any,
    get_kafka_producer: Callable[[], Any],
    build_option_chain_snapshot: Callable[..., Dict[str, Any]],
    get_available_expiries_for_date: Callable[..., Iterable[Dict[str, Any]]],
    normalize_dataset: Callable[[Optional[str]], str],
    normalize_date: Callable[[Optional[str]], str],
    normalize_time: Callable[[Optional[str]], str],
    resolve_default_week_date: Callable[[str], Tuple[int, str, str, str]],
    safe_float: Callable[[Any], Optional[float]],
    json_error: Callable[..., Any],
    default_interval: int,
) -> None:
    @app.route("/api/iv-surface")
    def api_iv_surface():
        try:
            dataset = normalize_dataset(
                request.args.get("dataset") or request.args.get("underlying")
            )

            query_date = request.args.get("date") or request.args.get("query_date")
            query_date = normalize_date(query_date) if query_date else None

            query_time = normalize_time(
                request.args.get("time") or request.args.get("query_time")
            )

            interval = int(request.args.get("interval", default_interval))
            strike_count = int(request.args.get("strike_count", 10))
            max_months = int(request.args.get("max_months", 2))

            if interval <= 0:
                raise ValueError("interval must be greater than zero.")

            if strike_count <= 0:
                raise ValueError("strike_count must be greater than zero.")

            if max_months <= 0:
                raise ValueError("max_months must be greater than zero.")

            if not query_date:
                _, _, _, query_date = resolve_default_week_date(dataset)

            cache_key = iv_surface_cache_key(
                dataset=dataset,
                query_date=query_date,
                query_time=query_time,
                interval=interval,
                strike_count=strike_count,
                max_months=max_months,
            )

            cached_result = redis_get_json(redis_client, cache_key)

            if cached_result:
                return jsonify(cached_result)

            lookup_key = job_lookup_key(cache_key)
            existing_job_id = redis_client.get(lookup_key)

            if existing_job_id:
                if isinstance(existing_job_id, bytes):
                    existing_job_id = existing_job_id.decode("utf-8")

                return jsonify({
                    "ok": True,
                    "status": "processing",
                    "job_id": existing_job_id,
                    "cache_key": cache_key,
                })

            new_job_id = str(uuid.uuid4())
            new_job_key = job_key(new_job_id)

            job_payload = {
                "ok": True,
                "status": "processing",
                "job_id": new_job_id,
                "job_key": new_job_key,
                "cache_key": cache_key,
                "dataset": dataset,
                "query_date": query_date,
                "query_time": query_time,
                "interval": interval,
                "strike_count": strike_count,
                "max_months": max_months,
            }

            redis_set_json(
                redis_client,
                new_job_key,
                job_payload,
                ex=IV_SURFACE_JOB_TTL_SECONDS,
            )

            redis_client.setex(
                lookup_key,
                IV_SURFACE_JOB_TTL_SECONDS,
                new_job_id,
            )

            producer = get_kafka_producer()
            producer.send(IV_SURFACE_TOPIC, job_payload)
            producer.flush()

            return jsonify({
                "ok": True,
                "status": "processing",
                "job_id": new_job_id,
                "cache_key": cache_key,
            })

        except Exception as exc:
            return json_error(str(exc), 400)

    @app.route("/api/iv-surface/status/<job_id_value>")
    def api_iv_surface_status(job_id_value: str):
        try:
            current_job_key = job_key(job_id_value)
            job_data = redis_get_json(redis_client, current_job_key)

            if not job_data:
                return json_error("IV surface job not found or expired.", 404)

            cache_key = job_data.get("cache_key")

            if cache_key:
                cached_result = redis_get_json(redis_client, cache_key)

                if cached_result:
                    return jsonify(cached_result)

            return jsonify(job_data)

        except Exception as exc:
            return json_error(str(exc), 400)


# =========================================================
# KAFKA WORKER HELPERS
# =========================================================

def process_iv_surface_message(
    *,
    message: Dict[str, Any],
    redis_client: Any,
    build_option_chain_snapshot: Callable[..., Dict[str, Any]],
    get_available_expiries_for_date: Callable[..., Iterable[Dict[str, Any]]],
    safe_float: Callable[[Any], Optional[float]],
) -> Dict[str, Any]:
    required = [
        "job_id",
        "job_key",
        "cache_key",
        "dataset",
        "query_date",
        "query_time",
        "interval",
        "strike_count",
        "max_months",
    ]

    missing = [key for key in required if key not in message]

    if missing:
        raise ValueError(f"Invalid IV surface job. Missing keys: {missing}")

    result = build_iv_surface_payload(
        dataset=message["dataset"],
        query_date=message["query_date"],
        query_time=message["query_time"],
        interval=int(message["interval"]),
        strike_count=int(message["strike_count"]),
        max_months=int(message["max_months"]),
        build_option_chain_snapshot=build_option_chain_snapshot,
        get_available_expiries_for_date=get_available_expiries_for_date,
        safe_float=safe_float,
    )

    redis_set_json(
        redis_client,
        message["cache_key"],
        result,
        ex=IV_SURFACE_CACHE_TTL_SECONDS,
    )

    redis_set_json(
        redis_client,
        message["job_key"],
        {
            "ok": True,
            "status": "done",
            "job_id": message["job_id"],
            "cache_key": message["cache_key"],
        },
        ex=IV_SURFACE_JOB_TTL_SECONDS,
    )

    return result


def mark_iv_surface_job_failed(
    *,
    redis_client: Any,
    message: Dict[str, Any],
    error: Exception,
) -> None:
    current_job_key = message.get("job_key") or job_key(str(message.get("job_id", "")))

    redis_set_json(
        redis_client,
        current_job_key,
        {
            "ok": False,
            "status": "failed",
            "job_id": message.get("job_id"),
            "cache_key": message.get("cache_key"),
            "error": str(error),
        },
        ex=IV_SURFACE_JOB_TTL_SECONDS,
    )


def run_iv_surface_worker(
    *,
    redis_client: Any,
    build_option_chain_snapshot: Callable[..., Dict[str, Any]],
    get_available_expiries_for_date: Callable[..., Iterable[Dict[str, Any]]],
    safe_float: Callable[[Any], Optional[float]],
) -> None:
    try:
        from kafka import KafkaConsumer
    except Exception as exc:
        raise RuntimeError("kafka-python is not installed or KafkaConsumer is unavailable.") from exc

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = os.getenv("KAFKA_IV_SURFACE_GROUP_ID", "iv-surface-workers")

    consumer = KafkaConsumer(
        IV_SURFACE_TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    logger.info(
        "IV surface worker started. topic=%s group_id=%s",
        IV_SURFACE_TOPIC,
        group_id,
    )

    for record in consumer:
        message = record.value

        try:
            process_iv_surface_message(
                message=message,
                redis_client=redis_client,
                build_option_chain_snapshot=build_option_chain_snapshot,
                get_available_expiries_for_date=get_available_expiries_for_date,
                safe_float=safe_float,
            )
            consumer.commit()

        except Exception as exc:
            logger.exception("IV surface job failed")
            mark_iv_surface_job_failed(
                redis_client=redis_client,
                message=message,
                error=exc,
            )
            consumer.commit()
