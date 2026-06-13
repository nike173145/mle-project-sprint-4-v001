import json
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any

import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query


logger = logging.getLogger("uvicorn.error")

PERSONAL_COLUMNS = ["user_id", "item_id", "rank"]
DEFAULT_COLUMNS = ["item_id", "rank"]
SIMILAR_COLUMNS = ["item_id", "similar_item_id", "score"]
LEGACY_SIMILAR_COLUMNS = ["item_id_1", "item_id_2", "score"]


@dataclass(frozen=True)
class Settings:
    personal_recs_path: str
    top_popular_path: str
    similar_items_path: str
    max_recommendations: int
    max_events_per_user: int
    online_events_limit: int

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        return cls(
            personal_recs_path=os.getenv(
                "PERSONAL_RECS_PATH", "data/personal_als.parquet"
            ),
            top_popular_path=os.getenv(
                "TOP_POPULAR_PATH", "data/top_popular.parquet"
            ),
            similar_items_path=os.getenv(
                "SIMILAR_ITEMS_PATH", "data/similar_items.parquet"
            ),
            max_recommendations=_positive_int("MAX_RECOMMENDATIONS", 100),
            max_events_per_user=_positive_int("MAX_EVENTS_PER_USER", 10),
            online_events_limit=_positive_int("ONLINE_EVENTS_LIMIT", 3),
        )


def _positive_int(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer") from error
    if value < 1:
        raise RuntimeError(f"{name} must be greater than zero")
    return value


def get_storage_options(path: str) -> dict[str, Any] | None:
    raw_options = os.getenv("PARQUET_STORAGE_OPTIONS_JSON")
    if raw_options:
        try:
            options = json.loads(raw_options)
        except json.JSONDecodeError as error:
            raise RuntimeError(
                "PARQUET_STORAGE_OPTIONS_JSON must contain valid JSON"
            ) from error
        if not isinstance(options, dict):
            raise RuntimeError(
                "PARQUET_STORAGE_OPTIONS_JSON must contain a JSON object"
            )
        return options

    if not path.startswith("s3://"):
        return None

    options: dict[str, Any] = {}
    if os.getenv("S3_ACCESS_KEY"):
        options["key"] = os.environ["S3_ACCESS_KEY"]
    if os.getenv("S3_SECRET_KEY"):
        options["secret"] = os.environ["S3_SECRET_KEY"]
    if os.getenv("S3_ENDPOINT_URL"):
        options["client_kwargs"] = {
            "endpoint_url": os.environ["S3_ENDPOINT_URL"]
        }
    return options or None


def read_parquet(
    path: str,
    columns: list[str],
    filters: list[tuple[str, str, Any]] | None = None,
) -> pd.DataFrame:
    read_kwargs: dict[str, Any] = {
        "path": path,
        "columns": columns,
        "engine": "pyarrow",
    }
    if filters is not None:
        read_kwargs["filters"] = filters
    storage_options = get_storage_options(path)
    if storage_options is not None:
        read_kwargs["storage_options"] = storage_options

    try:
        return pd.read_parquet(**read_kwargs)
    except Exception as error:
        raise RuntimeError(f"Could not load parquet file {path!r}: {error}") from error


def validate_columns(
    frame: pd.DataFrame, required: list[str], name: str
) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"{name} data is missing columns: {missing}")


def parquet_rows(path: str) -> int | None:
    if path.startswith("s3://") or not Path(path).is_file():
        return None
    return pq.ParquetFile(path).metadata.num_rows


def deduplicate(ids: list[int]) -> list[int]:
    return list(dict.fromkeys(ids))


def blend_recommendations(
    online: list[int], offline: list[int], k: int
) -> list[int]:
    """Alternate online and offline recommendations, online first."""
    blended: list[int] = []
    max_length = max(len(online), len(offline))
    for index in range(max_length):
        if index < len(online):
            blended.append(online[index])
        if index < len(offline):
            blended.append(offline[index])
    return deduplicate(blended)[:k]


class RecommendationStore:
    def __init__(self) -> None:
        self._personal: pd.DataFrame | None = None
        self._personal_path: str | None = None
        self._personal_rows: int | None = None
        self._default: list[int] = []
        self._stats = {"personal_requests": 0, "default_requests": 0}
        self._lock = Lock()

    def load(self, personal_path: str, default_path: str) -> None:
        # Validate the schema without loading the 100+ million personal rows.
        personal = read_parquet(
            personal_path,
            PERSONAL_COLUMNS,
            filters=[("user_id", "==", -1)],
        )
        default = read_parquet(default_path, DEFAULT_COLUMNS)
        validate_columns(personal, PERSONAL_COLUMNS, "personal recommendations")
        validate_columns(default, DEFAULT_COLUMNS, "top popular recommendations")
        self._personal = None
        self._personal_path = personal_path
        self._personal_rows = parquet_rows(personal_path)
        self._default = (
            default[DEFAULT_COLUMNS]
            .sort_values("rank", kind="stable")
            .drop_duplicates("item_id", keep="first")["item_id"]
            .tolist()
        )
        with self._lock:
            self._stats = {"personal_requests": 0, "default_requests": 0}
        logger.info(
            "Offline recommendations ready: lazy personal lookup, %s popular rows",
            len(default),
        )

    def load_frames(
        self, personal: pd.DataFrame, default: pd.DataFrame
    ) -> None:
        validate_columns(personal, PERSONAL_COLUMNS, "personal recommendations")
        validate_columns(default, DEFAULT_COLUMNS, "top popular recommendations")
        self._personal_path = None
        self._personal_rows = len(personal)
        self._personal = (
            personal[PERSONAL_COLUMNS]
            .sort_values(["user_id", "rank"], kind="stable")
            .drop_duplicates(["user_id", "item_id"], keep="first")
            .set_index("user_id")
        )
        self._default = (
            default[DEFAULT_COLUMNS]
            .sort_values("rank", kind="stable")
            .drop_duplicates("item_id", keep="first")["item_id"]
            .tolist()
        )
        with self._lock:
            self._stats = {"personal_requests": 0, "default_requests": 0}

    def get(self, user_id: int, k: int) -> tuple[list[int], str]:
        if self._personal is None and self._personal_path is None:
            raise RuntimeError("Recommendation store is not loaded")

        if self._personal_path is not None:
            personal = read_parquet(
                self._personal_path,
                PERSONAL_COLUMNS,
                filters=[("user_id", "==", user_id)],
            )
            recs = (
                personal.sort_values("rank", kind="stable")["item_id"].tolist()
            )
        else:
            try:
                recs = self._personal.loc[[user_id], "item_id"].tolist()
            except KeyError:
                recs = []

        if recs:
            source = "personal"
            counter = "personal_requests"
        else:
            recs = self._default
            source = "top_popular"
            counter = "default_requests"
        with self._lock:
            self._stats[counter] += 1
        return deduplicate(recs)[:k], source

    def stats(self) -> dict[str, Any]:
        personal_users = (
            None
            if self._personal is None
            else int(self._personal.index.nunique())
        )
        with self._lock:
            return {
                **self._stats,
                "personal_users": personal_users,
                "personal_rows": self._personal_rows,
                "top_popular_rows": len(self._default),
            }


class SimilarItemsStore:
    def __init__(self) -> None:
        self._similar: pd.DataFrame | None = None
        self._similar_path: str | None = None
        self._rows: int | None = None
        self._item_column = "item_id"
        self._similar_item_column = "similar_item_id"

    def load(self, path: str) -> None:
        try:
            similar = read_parquet(
                path,
                SIMILAR_COLUMNS,
                filters=[("item_id", "==", -1)],
            )
            validate_columns(similar, SIMILAR_COLUMNS, "similar items")
            self._item_column = "item_id"
            self._similar_item_column = "similar_item_id"
        except RuntimeError:
            similar = read_parquet(
                path,
                LEGACY_SIMILAR_COLUMNS,
                filters=[("item_id_1", "==", -1)],
            )
            validate_columns(similar, LEGACY_SIMILAR_COLUMNS, "similar items")
            self._item_column = "item_id_1"
            self._similar_item_column = "item_id_2"

        self._similar = None
        self._similar_path = path
        self._rows = parquet_rows(path)
        logger.info("Similar items ready for lazy lookup")

    def load_frame(self, similar: pd.DataFrame) -> None:
        if set(SIMILAR_COLUMNS).issubset(similar.columns):
            self._item_column = "item_id"
            self._similar_item_column = "similar_item_id"
        else:
            validate_columns(similar, LEGACY_SIMILAR_COLUMNS, "similar items")
            self._item_column = "item_id_1"
            self._similar_item_column = "item_id_2"
        self._similar_path = None
        self._rows = len(similar)
        self._similar = (
            similar[[self._item_column, self._similar_item_column, "score"]]
            .sort_values([self._item_column, "score"], ascending=[True, False])
            .drop_duplicates(
                [self._item_column, self._similar_item_column], keep="first"
            )
            .set_index(self._item_column)
        )

    def get_for_history(
        self, history: list[int], k: int
    ) -> list[int]:
        if self._similar is None and self._similar_path is None:
            raise RuntimeError("Similar items store is not loaded")

        candidates: list[tuple[int, float]] = []
        for item_id in history:
            if self._similar_path is not None:
                item_candidates = read_parquet(
                    self._similar_path,
                    [self._item_column, self._similar_item_column, "score"],
                    filters=[(self._item_column, "==", item_id)],
                )
            else:
                try:
                    item_candidates = self._similar.loc[[item_id]]
                except KeyError:
                    continue
            candidates.extend(
                zip(
                    item_candidates[self._similar_item_column].tolist(),
                    item_candidates["score"].tolist(),
                )
            )

        history_ids = set(history)
        candidates.sort(key=lambda candidate: candidate[1], reverse=True)
        recs = [item_id for item_id, _ in candidates if item_id not in history_ids]
        return deduplicate(recs)[:k]

    def size(self) -> int | None:
        return self._rows


class EventStore:
    def __init__(self, max_events_per_user: int = 10) -> None:
        self._events: dict[int, list[int]] = {}
        self._max_events_per_user = max_events_per_user
        self._lock = Lock()

    def configure(self, max_events_per_user: int) -> None:
        with self._lock:
            self._max_events_per_user = max_events_per_user
            self._events.clear()

    def put(self, user_id: int, item_id: int) -> None:
        with self._lock:
            current = self._events.get(user_id, [])
            self._events[user_id] = (
                [item_id] + current
            )[: self._max_events_per_user]

    def get(self, user_id: int, k: int) -> list[int]:
        with self._lock:
            return self._events.get(user_id, [])[:k]

    def users_count(self) -> int:
        with self._lock:
            return len(self._events)


rec_store = RecommendationStore()
similar_store = SimilarItemsStore()
event_store = EventStore()


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_env()
    rec_store.load(settings.personal_recs_path, settings.top_popular_path)
    similar_store.load(settings.similar_items_path)
    event_store.configure(settings.max_events_per_user)
    app.state.settings = settings
    yield
    logger.info("Recommendation service stopped")


app = FastAPI(
    title="Music recommendations",
    description="Blends offline ALS recommendations with online history signals.",
    lifespan=lifespan,
)


def validate_k(k: int) -> None:
    if k > app.state.settings.max_recommendations:
        raise HTTPException(
            status_code=422,
            detail=f"k must not exceed {app.state.settings.max_recommendations}",
        )


def make_recommendations(user_id: int, k: int) -> dict[str, Any]:
    settings = app.state.settings
    candidate_limit = settings.max_recommendations
    offline, offline_source = rec_store.get(user_id, candidate_limit)
    history = event_store.get(user_id, settings.online_events_limit)
    online = (
        similar_store.get_for_history(history, candidate_limit)
        if history
        else []
    )
    blended = blend_recommendations(online, offline, k)
    return {
        "recs": blended,
        "offline_source": offline_source,
        "history": history,
        "online_recs": online,
    }


@app.post("/events")
@app.post("/events/put")
async def put_event(user_id: int, item_id: int) -> dict[str, str]:
    event_store.put(user_id, item_id)
    return {"result": "ok"}


@app.get("/events")
async def get_events(
    user_id: int, k: int = Query(default=10, ge=1)
) -> dict[str, list[int]]:
    return {"events": event_store.get(user_id, k)}


@app.post("/recommendations_offline")
async def recommendations_offline(
    user_id: int, k: int = Query(default=100, ge=1)
) -> dict[str, Any]:
    validate_k(k)
    recs, source = rec_store.get(user_id, k)
    return {"recs": recs, "source": source}


@app.post("/recommendations_online")
async def recommendations_online(
    user_id: int, k: int = Query(default=100, ge=1)
) -> dict[str, Any]:
    validate_k(k)
    history = event_store.get(user_id, app.state.settings.online_events_limit)
    recs = similar_store.get_for_history(history, k) if history else []
    return {"recs": recs, "history": history}


@app.post("/recommendations")
async def recommendations(
    user_id: int, k: int = Query(default=100, ge=1)
) -> dict[str, Any]:
    validate_k(k)
    return make_recommendations(user_id, k)


@app.get("/health")
async def health() -> dict[str, Any]:
    settings = app.state.settings
    return {
        "status": "ok",
        "personal_recs_path": settings.personal_recs_path,
        "top_popular_path": settings.top_popular_path,
        "similar_items_path": settings.similar_items_path,
        "similar_items_rows": similar_store.size(),
        **rec_store.stats(),
    }
