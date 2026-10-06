

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from sklearn.preprocessing import MinMaxScaler, StandardScaler

VARIABLES = ("temperature", "humidity", "distance_cm")
LED = "led"

# Độ lệch tối thiểu: tránh IQR hoặc độ lệch chuẩn = 0 khi dữ liệu gần như không đổi
# (IQR dùng max(IQR, giá trị này); Z-score dùng max(std, giá trị này)).
MIN_SPREAD = {"temperature": 1.0, "humidity": 3.0, "distance_cm": 10.0}
IQR_K = 1.5
Z_THRESHOLD = 3.0
ROLL_WINDOW = 5
MIN_SAMPLES_FOR_OUTLIERS = 8  # ít hơn thì không đủ mẫu để ước lượng ngưỡng

INT_FIELDS = ("n_samples", "n_outliers", "filled")
CLEAN_COLUMNS = [
    "device_id",
    *VARIABLES,
    LED,
    "n_samples",
    "n_outliers",
    "filled",
    *[f"{v}_roll{ROLL_WINDOW}" for v in VARIABLES],
    *[f"{v}_delta" for v in VARIABLES],
    *[f"{v}_norm" for v in VARIABLES],
]
OUTLIER_COLUMNS = ["time", "device_id", "variable", "value"]


# ======================================================================
# Phần tính toán thuần (không đụng tới DB)
# ======================================================================
@dataclass
class PreprocessResult:
    clean: pd.DataFrame  # index = thời điểm bắt đầu cửa sổ (UTC); cột theo CLEAN_COLUMNS
    outliers: pd.DataFrame  # cột theo OUTLIER_COLUMNS: các mẫu thô bị coi là outlier
    bounds: pd.DataFrame  # mỗi (thiết bị, biến) một dòng: ngưỡng outlier, thống kê trước/sau
    windows: pd.DataFrame  # mỗi thiết bị một dòng: số cửa sổ có mẫu / nội suy / bị loại / ghi


def _parse_window(window) -> pd.Timedelta:
    try:
        td = pd.Timedelta(window)
    except (ValueError, TypeError):
        raise ValueError(f"Cửa sổ không hợp lệ: {window!r} (ví dụ: 30s, 1min, 5min)") from None
    if pd.isna(td) or td < pd.Timedelta(seconds=1):
        raise ValueError(f"Cửa sổ phải >= 1 giây: {window!r}")
    return td


def _window_label(td: pd.Timedelta) -> str:
    seconds = int(td.total_seconds())
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def _flag_outliers(values: pd.Series, method: str, min_spread: float):
    """Trả về (cờ outlier, ngưỡng dưới, ngưỡng trên). Giá trị NaN không bao giờ bị gắn cờ."""
    flags = pd.Series(False, index=values.index)
    valid = values.dropna()
    if len(valid) < MIN_SAMPLES_FOR_OUTLIERS:
        return flags, np.nan, np.nan

    if method == "iqr":
        q1, q3 = valid.quantile(0.25), valid.quantile(0.75)
        spread = max(q3 - q1, min_spread)
        lower, upper = q1 - IQR_K * spread, q3 + IQR_K * spread
    elif method == "zscore":
        mean = valid.mean()
        std = max(valid.std(ddof=0), min_spread)
        lower, upper = mean - Z_THRESHOLD * std, mean + Z_THRESHOLD * std
    else:
        raise ValueError(f"Phương pháp outlier không hợp lệ: {method!r}")

    flags = (values < lower) | (values > upper)
    return flags, float(lower), float(upper)


def _fill_short_gaps(series: pd.Series, max_gap: int):
    """Nội suy theo thời gian các khoảng trống NaN dài <= max_gap cửa sổ.

    Khoảng trống dài hơn, và NaN ở hai đầu chuỗi (không có hai phía để nội suy), được giữ nguyên.
    Trả về (chuỗi sau khi lấp, mặt nạ các điểm đã được nội suy).
    """
    isna = series.isna()
    run_id = isna.ne(isna.shift(fill_value=False)).cumsum()
    run_len = isna.groupby(run_id).transform("sum")
    long_gap = isna & (run_len > max_gap)

    interpolated = series.interpolate(method="time", limit_area="inside")
    filled = isna & ~long_gap & interpolated.notna()

    out = series.copy()
    out[filled] = interpolated[filled]
    return out, filled


def _check_columns(raw: pd.DataFrame) -> None:
    needed = ["_time", "device_id", *VARIABLES, LED]
    missing = [c for c in needed if c not in raw.columns]
    if missing:
        raise ValueError(
            f"Dữ liệu thô thiếu cột: {', '.join(missing)}. "
            "Kiểm tra bucket có field tương ứng (dữ liệu cũ có thể còn trường `light`)."
        )


def _empty_result_parts():
    return pd.DataFrame(columns=CLEAN_COLUMNS), pd.DataFrame(columns=OUTLIER_COLUMNS)


def _process_device(device_id, group, window_td, method, scaler_name, max_gap):
    df = group.copy()
    df["_time"] = pd.to_datetime(df["_time"], utc=True)
    df = df.sort_values("_time").set_index("_time")
    for col in (*VARIABLES, LED):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- 2. Outlier trên mẫu thô -------------------------------------------
    flags = pd.DataFrame(index=df.index)
    masked = df[list(VARIABLES)].copy()
    thresholds = {}
    outlier_parts = []
    for var in VARIABLES:
        flag, lower, upper = _flag_outliers(df[var], method, MIN_SPREAD[var])
        flags[var] = flag
        thresholds[var] = (lower, upper)
        masked.loc[flag, var] = np.nan
        if flag.any():
            outlier_parts.append(
                pd.DataFrame(
                    {
                        "time": df.index[flag.to_numpy()],
                        "device_id": device_id,
                        "variable": var,
                        "value": df.loc[flag, var].to_numpy(),
                    }
                )
            )

    # --- 3. Gom theo cửa sổ -------------------------------------------------
    # origin="epoch": ranh giới cửa sổ cố định, chạy lại với --start khác vẫn trùng timestamp
    resample = dict(rule=window_td, origin="epoch")
    win = masked.resample(**resample).mean()
    win[LED] = df[LED].resample(**resample).mean()
    win["n_samples"] = pd.Series(1, index=df.index).resample(**resample).sum().astype(int)
    win["n_outliers"] = flags.sum(axis=1).resample(**resample).sum().astype(int)

    # --- 4. Nội suy khoảng trống ngắn --------------------------------------
    filled_count = pd.Series(0, index=win.index)
    for var in VARIABLES:
        win[var], filled_mask = _fill_short_gaps(win[var], max_gap)
        filled_count += filled_mask.astype(int)
    win["filled"] = filled_count

    # Cửa sổ vẫn còn NaN (khoảng trống dài, hoặc nằm ở đầu/cuối chuỗi) bị loại
    dropped = win[list(VARIABLES)].isna().any(axis=1)
    kept = win[~dropped].copy()

    # --- 5. Đặc trưng và chuẩn hóa -----------------------------------------
    if kept.empty:
        clean = pd.DataFrame(columns=CLEAN_COLUMNS)
    else:
        # Mỗi đoạn liên tục (không bị cắt bởi cửa sổ bị loại) là một segment riêng
        segment = (kept.index.to_series().diff() != window_td).cumsum().to_numpy()
        scaler_cls = StandardScaler if scaler_name == "standard" else MinMaxScaler
        for var in VARIABLES:
            grouped = kept[var].groupby(segment)
            kept[f"{var}_roll{ROLL_WINDOW}"] = grouped.transform(
                lambda s: s.rolling(ROLL_WINDOW, min_periods=1).mean()
            )
            kept[f"{var}_delta"] = grouped.diff()
            kept[f"{var}_norm"] = scaler_cls().fit_transform(kept[[var]])[:, 0]
        kept.insert(0, "device_id", device_id)
        clean = kept[CLEAN_COLUMNS]
    clean.index.name = "time"

    # --- Thống kê cho báo cáo ----------------------------------------------
    bound_rows = []
    for var in VARIABLES:
        n_raw = int(df[var].notna().sum())
        n_out = int(flags[var].sum())
        lower, upper = thresholds[var]
        cleaned = clean[var].astype(float) if not clean.empty else pd.Series(dtype=float)
        bound_rows.append(
            {
                "device_id": device_id,
                "variable": var,
                "n_raw": n_raw,
                "n_outliers": n_out,
                "pct": round(100.0 * n_out / n_raw, 2) if n_raw else 0.0,
                "lower": lower,
                "upper": upper,
                "raw_min": df[var].min(),
                "raw_max": df[var].max(),
                "raw_std": df[var].std(),
                "clean_min": cleaned.min(),
                "clean_max": cleaned.max(),
                "clean_std": cleaned.std(),
            }
        )

    window_row = {
        "device_id": device_id,
        "n_raw": len(df),
        "first_sample": df.index.min(),
        "last_sample": df.index.max(),
        "windows": len(win),
        "with_data": int((win["n_samples"] > 0).sum()),
        "interpolated": int(((win["filled"] > 0) & ~dropped).sum()),
        "dropped": int(dropped.sum()),
        "written": len(clean),
    }
    outliers = (
        pd.concat(outlier_parts, ignore_index=True)
        if outlier_parts
        else pd.DataFrame(columns=OUTLIER_COLUMNS)
    )
    return clean, outliers, bound_rows, window_row


def preprocess(
    raw: pd.DataFrame,
    window="1min",
    method: str = "iqr",
    scaler: str = "standard",
    max_gap: int = 5,
) -> PreprocessResult:
    """Tiền xử lý dữ liệu thô.

    raw: DataFrame có các cột `_time`, `device_id`, temperature, humidity, distance_cm, led
         (mỗi dòng là một mẫu thô). Mỗi thiết bị được xử lý riêng.
    """
    if method not in ("iqr", "zscore"):
        raise ValueError(f"Phương pháp outlier không hợp lệ: {method!r}")
    if scaler not in ("standard", "minmax"):
        raise ValueError(f"Bộ chuẩn hóa không hợp lệ: {scaler!r}")
    if max_gap < 0:
        raise ValueError("max_gap phải >= 0")
    _check_columns(raw)
    window_td = _parse_window(window)

    cleans, outliers, bounds, windows = [], [], [], []
    for device_id, group in raw.groupby("device_id", sort=True):
        clean, out, bound_rows, window_row = _process_device(
            device_id, group, window_td, method, scaler, max_gap
        )
        cleans.append(clean)
        outliers.append(out)
        bounds.extend(bound_rows)
        windows.append(window_row)

    if not cleans:
        clean_all, out_all = _empty_result_parts()
        return PreprocessResult(clean_all, out_all, pd.DataFrame(bounds), pd.DataFrame(windows))
    return PreprocessResult(
        clean=pd.concat(cleans),
        outliers=pd.concat(outliers, ignore_index=True),
        bounds=pd.DataFrame(bounds),
        windows=pd.DataFrame(windows),
    )


# ======================================================================
# Đọc / ghi InfluxDB
# ======================================================================
@dataclass(frozen=True)
class Config:
    url: str
    org: str
    token: str
    bucket_raw: str
    bucket_clean: str


def load_config() -> Config:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    required = ["INFLUX_URL", "INFLUX_ORG", "INFLUX_TOKEN"]
    missing = [k for k in required if not os.getenv(k, "").strip()]
    if missing:
        raise SystemExit(f"Thiếu biến trong .env: {', '.join(missing)}")
    return Config(
        url=os.environ["INFLUX_URL"].strip(),
        org=os.environ["INFLUX_ORG"].strip(),
        token=os.environ["INFLUX_TOKEN"].strip(),
        bucket_raw=os.getenv("INFLUX_BUCKET_RAW", "iot_raw").strip(),
        bucket_clean=os.getenv("INFLUX_BUCKET_CLEAN", "iot_clean").strip(),
    )


_RELATIVE = re.compile(r"-?\d+(ns|us|ms|s|m|h|d|w|mo|y)")
_ABSOLUTE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})")


def _flux_time(value: str, name: str) -> str:
    """Chuẩn hóa tham số thời gian để nhúng vào Flux (đã kiểm tra định dạng)."""
    text = value.strip()
    if text == "now()":
        return text
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):  # chỉ có ngày -> 00:00 UTC
        return text + "T00:00:00Z"
    if re.fullmatch(r"\d+(ns|us|ms|s|m|h|d|w|mo|y)", text):  # "30m" nghĩa là 30 phút trước
        return "-" + text
    if _RELATIVE.fullmatch(text) or _ABSOLUTE.fullmatch(text):
        return text
    raise SystemExit(
        f"{name} không hợp lệ: {value!r}. Dùng dạng 30m, 2h, 2026-10-02 "
        "hoặc 2026-10-02T10:30:00+07:00 (không có múi giờ thì phải thêm Z)."
    )


def _escape(text: str) -> str:
    return str(text).replace("\\", "\\\\").replace('"', '\\"')


def fetch_raw(client: InfluxDBClient, cfg: Config, start: str, stop: str | None, device: str | None):
    range_args = f"start: {start}" + (f", stop: {stop}" if stop else "")
    fields = " or ".join(f'r._field == "{f}"' for f in (*VARIABLES, LED))
    device_filter = (
        f'  |> filter(fn: (r) => r.device_id == "{_escape(device)}")\n' if device else ""
    )
    columns = ", ".join(f'"{c}"' for c in ("_time", "device_id", *VARIABLES, LED))
    flux = (
        f'from(bucket: "{_escape(cfg.bucket_raw)}")\n'
        f"  |> range({range_args})\n"
        '  |> filter(fn: (r) => r._measurement == "environment")\n'
        f"  |> filter(fn: (r) => {fields})\n"
        f"{device_filter}"
        "  |> group()\n"
        '  |> pivot(rowKey: ["_time", "device_id"], columnKey: ["_field"], valueColumn: "_value")\n'
        f"  |> keep(columns: [{columns}])\n"
        '  |> sort(columns: ["_time"])\n'
    )
    result = client.query_api().query_data_frame(flux, org=cfg.org)
    if isinstance(result, list):
        result = pd.concat(result, ignore_index=True) if result else pd.DataFrame()
    if result.empty:
        return result
    return result.drop(columns=[c for c in ("result", "table") if c in result.columns])


def build_points(result: PreprocessResult, measurement: str):
    """Tạo các Point cần ghi: (điểm environment_<cửa sổ>, điểm outliers)."""
    fields = [c for c in result.clean.columns if c != "device_id"]
    clean_points = []
    for ts, rec in zip(result.clean.index, result.clean.to_dict("records")):
        point = Point(measurement).tag("device_id", rec["device_id"])
        for name in fields:
            value = rec[name]
            if pd.isna(value):  # ví dụ delta của cửa sổ đầu đoạn: bỏ qua field
                continue
            point = point.field(name, int(value) if name in INT_FIELDS else float(value))
        clean_points.append(point.time(ts.value // 1_000_000, WritePrecision.MS))

    outlier_points = []
    for row in result.outliers.itertuples(index=False):
        outlier_points.append(
            Point("outliers")
            .tag("device_id", row.device_id)
            .tag("variable", row.variable)
            .field("value", float(row.value))
            .time(row.time.value // 1_000_000, WritePrecision.MS)
        )
    return clean_points, outlier_points


def write_results(client, cfg: Config, result: PreprocessResult, measurement: str, window_td):
    clean_points, outlier_points = build_points(result, measurement)

    # Xóa kết quả cũ trong đúng khoảng dữ liệu vừa xử lý để chạy lại không để lại điểm thừa
    # (ví dụ outlier của lần chạy trước với phương pháp khác).
    delete_api = client.delete_api()
    for row in result.windows.itertuples(index=False):
        start = row.first_sample.floor(window_td).to_pydatetime()
        stop = (row.last_sample + window_td + pd.Timedelta(milliseconds=1)).to_pydatetime()
        device = _escape(row.device_id)
        for name in (measurement, "outliers"):
            delete_api.delete(
                start,
                stop,
                f'_measurement="{name}" AND device_id="{device}"',
                bucket=cfg.bucket_clean,
                org=cfg.org,
            )

    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(
        bucket=cfg.bucket_clean,
        org=cfg.org,
        record=clean_points + outlier_points,
        write_precision=WritePrecision.MS,
    )
    return len(clean_points), len(outlier_points)


# ======================================================================
# Giao diện dòng lệnh
# ======================================================================
def print_summary(result: PreprocessResult, args, measurement: str) -> None:
    line = "=" * 72
    print(line)
    print(
        f"Cửa sổ: {args.window} | outlier: {args.method} | chuẩn hóa: {args.scaler} "
        f"| max-gap: {args.max_gap} cửa sổ | measurement: {measurement}"
    )
    print(line)
    for win in result.windows.itertuples(index=False):
        print(f"\nThiết bị {win.device_id}")
        print(
            f"  Mẫu thô: {win.n_raw}  (từ {win.first_sample:%Y-%m-%d %H:%M:%S} "
            f"đến {win.last_sample:%Y-%m-%d %H:%M:%S} UTC)"
        )
        print(
            f"  Cửa sổ: tổng {win.windows} | có mẫu {win.with_data} | "
            f"nội suy {win.interpolated} | bị loại {win.dropped} | ghi {win.written}"
        )
        table = result.bounds[result.bounds["device_id"] == win.device_id].drop(
            columns="device_id"
        )
        print("  Outlier trên mẫu thô và thống kê trước/sau xử lý:")
        text = table.round(2).to_string(index=False)
        print("\n".join("    " + row for row in text.splitlines()))
    print(f"\nTổng: {len(result.clean)} cửa sổ sạch, {len(result.outliers)} outlier\n")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Tiền xử lý dữ liệu IoT từ iot_raw sang iot_clean.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--start", default="1h", help="Bắt đầu: 30m (30 phút trước), 2026-10-02, hoặc RFC3339")
    p.add_argument("--stop", default=None, help="Kết thúc (mặc định: bây giờ)")
    p.add_argument("--device", default=None, help="Chỉ xử lý thiết bị này (mặc định: tất cả)")
    p.add_argument("--window", default="1min", help="Độ rộng cửa sổ, ví dụ 30s, 1min, 5min")
    p.add_argument("--method", choices=["iqr", "zscore"], default="iqr", help="Phương pháp outlier")
    p.add_argument("--scaler", choices=["standard", "minmax"], default="standard", help="Cách chuẩn hóa")
    p.add_argument("--max-gap", type=int, default=5, help="Khoảng trống dài nhất (số cửa sổ) được nội suy")
    p.add_argument("--measurement", default=None, help="Tên measurement đầu ra (mặc định: environment_<cửa sổ>)")
    p.add_argument("--csv", default=None, help="Xuất thêm CSV (outlier ghi vào <tên>_outliers.csv)")
    p.add_argument("--dry-run", action="store_true", help="Chỉ tính và in kết quả, không ghi InfluxDB")
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    cfg = load_config()
    start = _flux_time(args.start, "--start")
    stop = _flux_time(args.stop, "--stop") if args.stop else None
    try:
        window_td = _parse_window(args.window)
    except ValueError as exc:
        raise SystemExit(str(exc))
    measurement = args.measurement or f"environment_{_window_label(window_td)}"

    client = InfluxDBClient(url=cfg.url, token=cfg.token, org=cfg.org, timeout=60_000)
    try:
        try:
            raw = fetch_raw(client, cfg, start, stop, args.device)
        except Exception as exc:
            raise SystemExit(f"Không đọc được InfluxDB ({cfg.url}): {exc}")
        if raw.empty:
            raise SystemExit(
                f"Không có dữ liệu `environment` trong {cfg.bucket_raw} (start={start}, "
                f"stop={stop or 'now'}, device={args.device or 'tất cả'})."
            )
        try:
            result = preprocess(raw, args.window, args.method, args.scaler, args.max_gap)
        except ValueError as exc:
            raise SystemExit(str(exc))

        print_summary(result, args, measurement)

        if args.csv:
            path = Path(args.csv)
            path.parent.mkdir(parents=True, exist_ok=True)
            result.clean.to_csv(path, index_label="time")
            outlier_path = path.with_name(f"{path.stem}_outliers{path.suffix}")
            result.outliers.to_csv(outlier_path, index=False)
            print(f"Đã xuất CSV: {path} và {outlier_path}")

        if args.dry_run:
            print("Chế độ dry-run: không ghi vào InfluxDB.")
        elif result.clean.empty:
            print("Không có cửa sổ sạch nào để ghi.")
        else:
            n_clean, n_out = write_results(client, cfg, result, measurement, window_td)
            print(
                f"Đã ghi vào {cfg.bucket_clean}: {n_clean} điểm `{measurement}`, "
                f"{n_out} điểm `outliers` (kết quả cũ trong cùng khoảng đã được thay thế)."
            )
    finally:
        client.close()


if __name__ == "__main__":
    main()
