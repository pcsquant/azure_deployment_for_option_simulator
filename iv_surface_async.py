"""
Production asynchronous IV-surface support for the Options Simulator.

Features
--------
* Flask endpoints:
    - GET /api/iv-surface
    - GET /api/iv-surface/status/<job_id>
* Redis result cache and distributed in-flight job deduplication.
* ThreadPoolExecutor-backed background processing.
* Per-expiry parallelism with bounded workers.
* Input validation, deterministic cache keys, structured job states, timing
  metadata, safe cleanup, and resilient Redis/background-worker handling.

The module is dependency-injected from the Flask application, so it does not
import the simulator application itself.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence, Tuple

from flask import jsonify, request

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def _env_int(name: str, default: int, minimum: int = 1, maximum: int | None = None) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, received {raw!r}") from exc
    if value < minimum:
        raise RuntimeError(f"{name} must be >= {minimum}, received {value}")
    if maximum is not None and value > maximum:
        raise RuntimeError(f"{name} must be <= {maximum}, received {value}")
    return value


IV_SURFACE_CACHE_VERSION = os.getenv("IV_SURFACE_CACHE_VERSION", "v3-threadpool").strip()
IV_SURFACE_CACHE_TTL_SECONDS = _env_int("IV_SURFACE_CACHE_TTL_SECONDS", 3600, 30)
IV_SURFACE_JOB_TTL_SECONDS = _env_int("IV_SURFACE_JOB_TTL_SECONDS", 900, 60)
IV_SURFACE_LOCK_TTL_SECONDS = _env_int("IV_SURFACE_LOCK_TTL_SECONDS", 900, 60)
IV_SURFACE_MAX_EXPIRIES = _env_int("IV_SURFACE_MAX_EXPIRIES", 5, 1, 24)
IV_SURFACE_MAX_WORKERS = _env_int("IV_SURFACE_MAX_WORKERS", 4, 1, 32)
IV_SURFACE_MAX_STRIKES_EACH_SIDE = _env_int("IV_SURFACE_MAX_STRIKES_EACH_SIDE", 100, 1, 500)
IV_SURFACE_MAX_MONTHS_ALLOWED = _env_int("IV_SURFACE_MAX_MONTHS_ALLOWED", 12, 1, 60)
IV_SURFACE_BACKGROUND_WORKERS = _env_int("IV_SURFACE_BACKGROUND_WORKERS", 2, 1, 16)

_BACKGROUND_EXECUTOR = ThreadPoolExecutor(
    max_workers=IV_SURFACE_BACKGROUND_WORKERS,
    thread_name_prefix="iv-surface-job",
)


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, default=str)


def _decode_redis_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def redis_get_json(redis_client: Any, key: str) -> Optional[Dict[str, Any]]:
    """Read a JSON object from Redis.

    Corrupt entries are removed because retaining them would make every request
    fail until their TTL expires.
    """
    try:
        raw = redis_client.get(key)
    except Exception as exc:
        logger.exception("Redis GET failed for key=%s", key)
        raise RuntimeError("Redis is unavailable") from exc

    text = _decode_redis_value(raw)
    if not text:
        return None

    try:
        value = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        logger.warning("Removing corrupt Redis JSON entry key=%s", key)
        try:
            redis_client.delete(key)
        except Exception:
            logger.debug("Unable to delete corrupt Redis key=%s", key, exc_info=True)
        return None

    return value if isinstance(value, dict) else None


def redis_set_json(redis_client: Any, key: str, value: Mapping[str, Any], ex: int) -> None:
    try:
        redis_client.setex(key, int(ex), _json_dumps(dict(value)))
    except Exception as exc:
        logger.exception("Redis SETEX failed for key=%s", key)
        raise RuntimeError("Redis is unavailable") from exc


def redis_set_if_absent(redis_client: Any, key: str, value: str, ex: int) -> bool:
    """Atomically acquire a Redis key with TTL."""
    try:
        result = redis_client.set(key, value, nx=True, ex=int(ex))
    except Exception as exc:
        logger.exception("Redis SET NX failed for key=%s", key)
        raise RuntimeError("Redis is unavailable") from exc
    return bool(result)


def redis_delete(redis_client: Any, *keys: str) -> None:
    filtered = [key for key in keys if key]
    if not filtered:
        return
    try:
        redis_client.delete(*filtered)
    except Exception:
        logger.warning("Unable to delete Redis keys=%s", filtered, exc_info=True)


def _normalise_cache_component(value: Any) -> str:
    return str(value).strip().lower().replace(" ", "_")


def iv_surface_cache_key(
    dataset: str,
    query_date: str,
    query_time: str,
    interval: int,
    strike_count: int,
    max_months: int,
) -> str:
    """Build a short deterministic cache key.

    The readable prefix helps operations; the SHA-256 suffix prevents unsafe or
    unexpectedly long user-controlled Redis keys.
    """
    canonical = {
        "version": IV_SURFACE_CACHE_VERSION,
        "dataset": _normalise_cache_component(dataset),
        "query_date": str(query_date),
        "query_time": str(query_time),
        "interval": int(interval),
        "strike_count": int(strike_count),
        "max_months": int(max_months),
    }
    digest = hashlib.sha256(_json_dumps(canonical).encode("utf-8")).hexdigest()[:24]
    return f"iv_surface:{IV_SURFACE_CACHE_VERSION}:{canonical['dataset']}:{digest}"


def job_lookup_key(cache_key: str) -> str:
    return f"iv_surface:lookup:{cache_key}"


def job_key(job_id: str) -> str:
    return f"iv_surface:job:{job_id}"


def _validate_job_id(value: str) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError("Invalid IV surface job id") from exc


def _coerce_int(name: str, value: Any, minimum: int, maximum: int) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return result


@dataclass(frozen=True)
class IVSurfaceRequest:
    dataset: str
    query_date: str
    query_time: str
    interval: int
    strike_count: int
    max_months: int

    @property
    def cache_key(self) -> str:
        return iv_surface_cache_key(
            self.dataset,
            self.query_date,
            self.query_time,
            self.interval,
            self.strike_count,
            self.max_months,
        )


# ---------------------------------------------------------------------------
# Surface builder
# ---------------------------------------------------------------------------


def _normalise_expiry(expiry: Mapping[str, Any]) -> Dict[str, Any]:
    value = str(expiry.get("value", "")).strip()
    if not value:
        raise ValueError("Expiry entry is missing 'value'")
    label = str(expiry.get("label") or value).strip()
    return {**dict(expiry), "value": value, "label": label}


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
    """Build a complete IV-surface payload for a single timestamp."""
    started = time.monotonic()

    raw_expiries = get_available_expiries_for_date(
        query_date=query_date,
        dataset=dataset,
        max_months=max_months,
    )
    all_expiries: list[Dict[str, Any]] = []
    seen_values: set[str] = set()

    for raw in raw_expiries or []:
        try:
            exp = _normalise_expiry(raw)
        except Exception:
            logger.warning("Skipping malformed expiry entry: %r", raw, exc_info=True)
            continue
        if exp["value"] in seen_values:
            continue
        seen_values.add(exp["value"])
        all_expiries.append(exp)

    expiries = all_expiries[: min(max_months, IV_SURFACE_MAX_EXPIRIES)]

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
            "errors": [],
            "generated_at": utc_now_iso(),
            "duration_ms": round((time.monotonic() - started) * 1000, 2),
        }

    def process_expiry(exp: Dict[str, Any]) -> Tuple[list[Dict[str, Any]], set[int], str, Dict[str, Any]]:
        expiry_started = time.monotonic()
        expiry_value = exp["value"]
        expiry_label = exp["label"]

        snap = build_option_chain_snapshot(
            query_date=query_date,
            query_time=query_time,
            dataset=dataset,
            expiry_rule="current expiry",
            strike_count_each_side=strike_count,
            candle_interval_minutes=interval,
            selected_expiry=expiry_value,
            compute_greeks=False,
        )
        if not isinstance(snap, dict):
            raise TypeError("build_option_chain_snapshot must return a dictionary")

        chain_source = str(snap.get("chain_source") or "unknown")
        atm_value = safe_float(snap.get("atm"))
        atm = int(round(atm_value)) if atm_value is not None else None
        rows: list[Dict[str, Any]] = []
        strikes: set[int] = set()

        for raw_row in snap.get("rows") or []:
            if not isinstance(raw_row, Mapping):
                continue
            strike_value = safe_float(raw_row.get("strike"))
            if strike_value is None:
                continue
            strike = int(round(strike_value))

            ce = safe_float(raw_row.get("ce_iv"))
            pe = safe_float(raw_row.get("pe_iv"))
            if ce is not None and 0 < ce <= 1:
                ce *= 100.0
            if pe is not None and 0 < pe <= 1:
                pe *= 100.0

            if atm is not None and strike < atm:
                smile_iv = pe
            elif atm is not None and strike > atm:
                smile_iv = ce
            else:
                valid_values = [value for value in (ce, pe) if value is not None and value > 0]
                smile_iv = sum(valid_values) / len(valid_values) if valid_values else None

            if smile_iv is None or smile_iv <= 0:
                continue

            strikes.add(strike)
            rows.append({
                "expiry": expiry_value,
                "expiry_label": expiry_label,
                "strike": strike,
                "iv": round(float(smile_iv), 4),
                "ce_iv": round(float(ce), 4) if ce is not None else None,
                "pe_iv": round(float(pe), 4) if pe is not None else None,
                "atm": atm is not None and strike == atm,
            })

        meta = {
            "expiry": expiry_value,
            "chain_source": chain_source,
            "row_count": len(rows),
            "duration_ms": round((time.monotonic() - expiry_started) * 1000, 2),
        }
        logger.info("IV surface expiry complete %s", meta)
        return rows, strikes, chain_source, meta

    rows: list[Dict[str, Any]] = []
    strikes: set[int] = set()
    sources: set[str] = set()
    expiry_meta: list[Dict[str, Any]] = []
    errors: list[Dict[str, Any]] = []

    worker_count = min(len(expiries), IV_SURFACE_MAX_WORKERS, max(1, os.cpu_count() or 1))
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="iv-expiry") as executor:
        futures = {executor.submit(process_expiry, exp): exp for exp in expiries}
        for future in as_completed(futures):
            exp = futures[future]
            try:
                exp_rows, exp_strikes, source, meta = future.result()
                rows.extend(exp_rows)
                strikes.update(exp_strikes)
                sources.add(source)
                expiry_meta.append(meta)
            except Exception as exc:
                logger.exception("IV surface expiry failed expiry=%s", exp.get("value"))
                errors.append({
                    "expiry": exp.get("value"),
                    "expiry_label": exp.get("label"),
                    "error": str(exc),
                })

    if not sources:
        chain_source = "none"
    elif len(sources) == 1:
        chain_source = next(iter(sources))
    else:
        chain_source = "mixed"

    rows.sort(key=lambda row: (str(row.get("expiry", "")), int(row.get("strike", 0))))
    expiry_meta.sort(key=lambda item: str(item.get("expiry", "")))

    return {
        "ok": not errors or bool(rows),
        "status": "ready" if not errors else "ready_with_errors",
        "dataset": dataset,
        "query_date": query_date,
        "query_time": query_time,
        "interval": interval,
        "strike_count": strike_count,
        "max_months": max_months,
        "expiries": expiries,
        "strikes": sorted(strikes),
        "chain_source": chain_source,
        "rows": rows,
        "errors": errors,
        "expiry_meta": expiry_meta,
        "generated_at": utc_now_iso(),
        "duration_ms": round((time.monotonic() - started) * 1000, 2),
    }


def _run_iv_surface_job(
    *,
    job_payload: Dict[str, Any],
    redis_client: Any,
    build_option_chain_snapshot: Callable[..., Dict[str, Any]],
    get_available_expiries_for_date: Callable[..., Iterable[Dict[str, Any]]],
    safe_float: Callable[[Any], Optional[float]],
) -> None:
    """Execute one IV-surface job in a shared background thread."""
    try:
        process_iv_surface_message(
            message=job_payload,
            redis_client=redis_client,
            build_option_chain_snapshot=build_option_chain_snapshot,
            get_available_expiries_for_date=get_available_expiries_for_date,
            safe_float=safe_float,
        )
    except Exception as exc:
        logger.exception(
            "Background IV-surface job failed job_id=%s",
            job_payload.get("job_id"),
        )
        mark_iv_surface_job_failed(
            redis_client=redis_client,
            message=job_payload,
            error=exc,
        )


# ---------------------------------------------------------------------------
# Flask integration
# ---------------------------------------------------------------------------


def register_iv_surface_routes(
    *,
    app: Any,
    redis_client: Any,
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
    """Register Redis-backed, ThreadPoolExecutor IV-surface routes."""

    def parse_request() -> IVSurfaceRequest:
        dataset = normalize_dataset(request.args.get("dataset") or request.args.get("underlying"))
        raw_date = request.args.get("date") or request.args.get("query_date")
        query_date = normalize_date(raw_date) if raw_date else None
        query_time = normalize_time(request.args.get("time") or request.args.get("query_time"))
        interval = _coerce_int("interval", request.args.get("interval", default_interval), 1, 60)
        strike_count = _coerce_int(
            "strike_count", request.args.get("strike_count", 10), 1, IV_SURFACE_MAX_STRIKES_EACH_SIDE
        )
        max_months = _coerce_int(
            "max_months", request.args.get("max_months", 2), 1, IV_SURFACE_MAX_MONTHS_ALLOWED
        )
        if not query_date:
            _, _, _, query_date = resolve_default_week_date(dataset)
        return IVSurfaceRequest(dataset, query_date, query_time, interval, strike_count, max_months)

    @app.get("/api/iv-surface")
    def api_iv_surface():
        try:
            params = parse_request()
            cache_key = params.cache_key

            cached = redis_get_json(redis_client, cache_key)
            if cached is not None:
                cached["cache_hit"] = True
                return jsonify(cached), 200

            lookup_key = job_lookup_key(cache_key)
            existing_job_id = _decode_redis_value(redis_client.get(lookup_key))
            if existing_job_id:
                existing_job = redis_get_json(redis_client, job_key(existing_job_id))
                if existing_job:
                    return jsonify(existing_job), 202
                redis_delete(redis_client, lookup_key)

            job_id = str(uuid.uuid4())
            if not redis_set_if_absent(redis_client, lookup_key, job_id, IV_SURFACE_LOCK_TTL_SECONDS):
                winner = _decode_redis_value(redis_client.get(lookup_key))
                if winner:
                    winner_job = redis_get_json(redis_client, job_key(winner))
                    if winner_job:
                        return jsonify(winner_job), 202
                raise RuntimeError("Unable to acquire IV surface job lock")

            current_job_key = job_key(job_id)
            job_payload: Dict[str, Any] = {
                "ok": True,
                "status": "queued",
                "job_id": job_id,
                "job_key": current_job_key,
                "cache_key": cache_key,
                "lookup_key": lookup_key,
                "dataset": params.dataset,
                "query_date": params.query_date,
                "query_time": params.query_time,
                "interval": params.interval,
                "strike_count": params.strike_count,
                "max_months": params.max_months,
                "created_at": utc_now_iso(),
            }
            redis_set_json(redis_client, current_job_key, job_payload, IV_SURFACE_JOB_TTL_SECONDS)

            try:
                future = _BACKGROUND_EXECUTOR.submit(
                    _run_iv_surface_job,
                    job_payload=job_payload,
                    redis_client=redis_client,
                    build_option_chain_snapshot=build_option_chain_snapshot,
                    get_available_expiries_for_date=get_available_expiries_for_date,
                    safe_float=safe_float,
                )
                future.add_done_callback(
                    lambda done: logger.error(
                        "IV-surface executor task failed: %s",
                        done.exception(),
                    )
                    if done.exception() is not None
                    else None
                )
            except Exception as exc:
                failed = {
                    **job_payload,
                    "ok": False,
                    "status": "failed",
                    "error": "Unable to submit background job",
                    "failed_at": utc_now_iso(),
                }
                redis_set_json(
                    redis_client,
                    current_job_key,
                    failed,
                    IV_SURFACE_JOB_TTL_SECONDS,
                )
                redis_delete(redis_client, lookup_key)
                logger.exception("Unable to submit IV surface job id=%s", job_id)
                raise RuntimeError("Unable to queue IV surface calculation") from exc

            return jsonify(job_payload), 202
        except ValueError as exc:
            return json_error(str(exc), 400)
        except Exception as exc:
            logger.exception("IV surface request failed")
            return json_error(str(exc), 503)

    @app.get("/api/iv-surface/status/<job_id_value>")
    def api_iv_surface_status(job_id_value: str):
        try:
            validated_job_id = _validate_job_id(job_id_value)
            data = redis_get_json(redis_client, job_key(validated_job_id))
            if not data:
                return json_error("IV surface job not found or expired.", 404)

            cache_key = data.get("cache_key")
            if cache_key:
                result = redis_get_json(redis_client, cache_key)
                if result is not None:
                    result["cache_hit"] = True
                    result["job_id"] = validated_job_id
                    return jsonify(result), 200

            status = data.get("status")
            return jsonify(data), (500 if status == "failed" else 202)
        except ValueError as exc:
            return json_error(str(exc), 400)
        except Exception as exc:
            logger.exception("IV surface status request failed")
            return json_error(str(exc), 503)


# ---------------------------------------------------------------------------
# Background executor lifecycle
# ---------------------------------------------------------------------------


_REQUIRED_JOB_FIELDS: Sequence[str] = (
    "job_id", "job_key", "cache_key", "lookup_key", "dataset", "query_date",
    "query_time", "interval", "strike_count", "max_months",
)


def validate_job_message(message: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(message, Mapping):
        raise ValueError("IV surface message must be a JSON object")
    missing = [key for key in _REQUIRED_JOB_FIELDS if not message.get(key)]
    if missing:
        raise ValueError(f"Invalid IV surface job. Missing keys: {missing}")
    out = dict(message)
    out["job_id"] = _validate_job_id(str(out["job_id"]))
    out["interval"] = _coerce_int("interval", out["interval"], 1, 60)
    out["strike_count"] = _coerce_int(
        "strike_count", out["strike_count"], 1, IV_SURFACE_MAX_STRIKES_EACH_SIDE
    )
    out["max_months"] = _coerce_int(
        "max_months", out["max_months"], 1, IV_SURFACE_MAX_MONTHS_ALLOWED
    )
    return out


def process_iv_surface_message(
    *,
    message: Dict[str, Any],
    redis_client: Any,
    build_option_chain_snapshot: Callable[..., Dict[str, Any]],
    get_available_expiries_for_date: Callable[..., Iterable[Dict[str, Any]]],
    safe_float: Callable[[Any], Optional[float]],
) -> Dict[str, Any]:
    message = validate_job_message(message)
    started = time.monotonic()
    processing_state = {
        **message,
        "ok": True,
        "status": "processing",
        "started_at": utc_now_iso(),
    }
    redis_set_json(redis_client, message["job_key"], processing_state, IV_SURFACE_JOB_TTL_SECONDS)
    existing = redis_get_json(redis_client, message["cache_key"])
    if existing is not None:
        redis_delete(redis_client, message["lookup_key"])
        return existing
    result = build_iv_surface_payload(
        dataset=str(message["dataset"]),
        query_date=str(message["query_date"]),
        query_time=str(message["query_time"]),
        interval=message["interval"],
        strike_count=message["strike_count"],
        max_months=message["max_months"],
        build_option_chain_snapshot=build_option_chain_snapshot,
        get_available_expiries_for_date=get_available_expiries_for_date,
        safe_float=safe_float,
    )
    result["job_id"] = message["job_id"]
    result["cache_hit"] = False
    redis_set_json(redis_client, message["cache_key"], result, IV_SURFACE_CACHE_TTL_SECONDS)
    done_state = {
        "ok": True,
        "status": "done",
        "job_id": message["job_id"],
        "cache_key": message["cache_key"],
        "finished_at": utc_now_iso(),
        "duration_ms": round((time.monotonic() - started) * 1000, 2),
    }
    redis_set_json(redis_client, message["job_key"], done_state, IV_SURFACE_JOB_TTL_SECONDS)
    redis_delete(redis_client, message["lookup_key"])
    return result


def mark_iv_surface_job_failed(
    *, redis_client: Any, message: Mapping[str, Any], error: Exception
) -> None:
    current_job_key = str(message.get("job_key") or job_key(str(message.get("job_id", ""))))
    failed = {
        "ok": False,
        "status": "failed",
        "job_id": message.get("job_id"),
        "cache_key": message.get("cache_key"),
        "error": str(error),
        "failed_at": utc_now_iso(),
    }
    try:
        redis_set_json(redis_client, current_job_key, failed, IV_SURFACE_JOB_TTL_SECONDS)
    finally:
        redis_delete(redis_client, str(message.get("lookup_key") or ""))


def shutdown_iv_surface_executor(wait: bool = True) -> None:
    _BACKGROUND_EXECUTOR.shutdown(wait=bool(wait), cancel_futures=not bool(wait))


def run_iv_surface_worker(**_: Any) -> None:
    raise RuntimeError(
        "Kafka IV-surface workers are disabled. "
        "IV-surface jobs now run inside the Flask process using ThreadPoolExecutor."
    )
