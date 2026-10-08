"""Verified monthly Binance spot OHLCV archives for reproducible backfills."""

import csv
import hashlib
import io
import json
import re
import subprocess
import zipfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from trading.data.schema import frame_from_rows, from_milliseconds, require_utc
from trading.data.transform import interval_milliseconds

BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"


@dataclass(frozen=True, slots=True)
class ArchivePart:
    """Identity and integrity evidence for one downloaded month."""

    month: str
    url: str
    sha256: str
    path: str
    rows: int
    observed_at_utc: str


class BinanceMonthlyArchive:
    """Download checksum-verified, complete spot months without credentials."""

    def __init__(self, symbol: str, timeframe: str, root: Path) -> None:
        """Restrict identifiers before using them in official archive URLs."""
        if re.fullmatch(r"[A-Z0-9]{3,24}", symbol) is None or not symbol.endswith("USDT"):
            raise ValueError("Binance archive symbol must be an uppercase USDT pair")
        if timeframe not in {"1h", "4h", "1d"}:
            raise ValueError("supported archive intervals are 1h, 4h, and 1d")
        self.symbol = symbol
        self.timeframe = timeframe
        self.root = root

    @staticmethod
    def _download(url: str) -> bytes:
        """Use the system's TLS-aware curl and fail on HTTP or transport errors."""
        result = subprocess.run(
            ["curl", "--fail", "--location", "--silent", "--show-error", "--retry", "4",
             "--retry-all-errors", "--retry-delay", "1", "--max-time", "45", url],
            capture_output=True,
            check=False,
            timeout=240,
        )
        if result.returncode:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"archive download failed for {url}: {detail}")
        return result.stdout

    def _url(self, month: str) -> str:
        """Build an official monthly spot kline archive URL."""
        filename = f"{self.symbol}-{self.timeframe}-{month}.zip"
        return f"{BASE_URL}/{self.symbol}/{self.timeframe}/{filename}"

    def fetch_month(self, month: str) -> tuple[pl.DataFrame, ArchivePart]:
        """Verify SHA-256 before parsing one immutable raw archive copy."""
        datetime.strptime(month, "%Y-%m")
        url = self._url(month)
        month_dir = self.root / self.symbol / self.timeframe
        cached = sorted(month_dir.glob(f"{month}-*.zip"))
        if len(cached) > 1:
            raise ValueError(f"ambiguous cached archive for {month}")
        if cached:
            checksum = [cached[0].stem.removeprefix(f"{month}-"), url.rsplit("/", 1)[-1]]
        else:
            checksum = self._download(f"{url}.CHECKSUM").decode("ascii").split()
        if len(checksum) != 2 or checksum[1] != url.rsplit("/", 1)[-1]:
            raise ValueError("archive checksum response has an unexpected filename")
        digest = checksum[0].lower()
        if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValueError("archive checksum is not a SHA-256 digest")
        path = self.root / self.symbol / self.timeframe / f"{month}-{digest}.zip"
        metadata_path = path.with_suffix(".json")
        if path.exists():
            payload = path.read_bytes()
        else:
            payload = self._download(url)
        if hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError(f"archive SHA-256 mismatch: {month}")
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(".tmp")
            temporary.write_bytes(payload)
            temporary.replace(path)
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            files = [name for name in archive.namelist() if name.endswith(".csv")]
            if len(files) != 1:
                raise ValueError("monthly archive must contain exactly one CSV")
            rows = list(csv.reader(io.TextIOWrapper(archive.open(files[0]), encoding="utf-8")))
        records: list[dict[str, object]] = []
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            recorded = metadata.get("observed_at_utc")
            observed_at = (
                datetime.fromisoformat(recorded)
                if isinstance(recorded, str)
                else datetime.fromtimestamp(metadata_path.stat().st_mtime, tz=UTC)
            )
            require_utc(observed_at, "archive observed_at")
        else:
            observed_at = datetime.now(UTC)
        step_ms = interval_milliseconds(self.timeframe)
        for row in rows:
            if len(row) != 12:
                raise ValueError(f"monthly archive has an invalid kline row: {month}")
            raw_open = int(row[0])
            if raw_open >= 10**15:
                if raw_open % 1000:
                    raise ValueError("microsecond kline timestamp is not millisecond aligned")
                open_ms = raw_open // 1000
            elif 10**12 <= raw_open < 10**15:
                open_ms = raw_open
            else:
                raise ValueError("unexpected kline timestamp unit")
            timestamp = from_milliseconds(open_ms)
            if timestamp.strftime("%Y-%m") != month or open_ms % step_ms:
                raise ValueError(f"off-month or off-grid kline in {month}")
            records.append(
                {
                    "exchange": "binance",
                    "symbol": f"{self.symbol[:-4]}/USDT",
                    "timeframe": self.timeframe,
                    "timestamp": timestamp,
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "observed_at": observed_at,
                }
            )
        if not records:
            raise ValueError(f"monthly archive is empty: {month}")
        part = ArchivePart(month, url, digest, str(path), len(records), observed_at.isoformat())
        metadata_path.write_text(
            json.dumps(asdict(part), indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return frame_from_rows("ohlcv", records), part

    def collect(
        self, start: datetime, end: datetime
    ) -> tuple[pl.DataFrame, tuple[ArchivePart, ...]]:
        """Collect complete months over a UTC half-open interval."""
        require_utc(start, "start")
        require_utc(end, "end")
        if start.day != 1 or end.day != 1 or any(
            (start.hour, start.minute, start.second, start.microsecond,
             end.hour, end.minute, end.second, end.microsecond)
        ) or end <= start:
            raise ValueError("archive window must contain complete UTC months")
        now = datetime.now(UTC)
        current_month = datetime(now.year, now.month, 1, tzinfo=UTC)
        if end > current_month:
            raise ValueError("current incomplete month cannot enter research data")
        frames: list[pl.DataFrame] = []
        parts: list[ArchivePart] = []
        year, month = start.year, start.month
        while (year, month) < (end.year, end.month):
            frame, part = self.fetch_month(f"{year:04d}-{month:02d}")
            frames.append(frame)
            parts.append(part)
            month += 1
            if month == 13:
                year, month = year + 1, 1
        return pl.concat(frames).sort("timestamp"), tuple(parts)
