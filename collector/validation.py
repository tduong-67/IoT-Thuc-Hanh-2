
import json
import math

# Khoảng giá trị hợp lệ của từng cảm biến (đơn vị: °C, %, cm)
RANGES = {
    "temperature": (-40.0, 80.0),
    "humidity": (0.0, 100.0),
    "distance_cm": (2.0, 400.0),
}

REQUIRED_FIELDS = (
    "device_id",
    "seq",
    "ts_publish",
    "temperature",
    "humidity",
    "distance_cm",
    "led",
)

# ts_publish là epoch tính bằng mili giây; nhỏ hơn mốc này (~tháng 9/2020) coi là sai
MIN_TS_PUBLISH_MS = 1_600_000_000_000
MAX_DEVICE_ID_LEN = 64


class ValidationError(ValueError):
    """Bản tin không hợp lệ; nội dung lỗi là lý do bị loại."""


def _is_int(value) -> bool:
    # bool là lớp con của int trong Python nên phải loại riêng
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def parse_payload(raw) -> dict:
    """Giải mã và kiểm tra payload. Trả về dict đã chuẩn hóa kiểu dữ liệu."""
    try:
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
        obj = json.loads(text)
    except ValueError as exc:  # gồm JSONDecodeError và UnicodeDecodeError
        raise ValidationError(f"JSON không hợp lệ: {exc}") from exc

    if not isinstance(obj, dict):
        raise ValidationError("Payload không phải đối tượng JSON")

    missing = [k for k in REQUIRED_FIELDS if k not in obj]
    if missing:
        raise ValidationError(f"Thiếu trường: {', '.join(missing)}")

    device_id = obj["device_id"]
    if (
        not isinstance(device_id, str)
        or not device_id.strip()
        or len(device_id) > MAX_DEVICE_ID_LEN
    ):
        raise ValidationError("device_id phải là chuỗi không rỗng")

    seq = obj["seq"]
    if not _is_int(seq) or seq < 0:
        raise ValidationError(f"seq phải là số nguyên không âm: {seq!r}")

    ts_publish = obj["ts_publish"]
    if not _is_int(ts_publish) or ts_publish < MIN_TS_PUBLISH_MS:
        raise ValidationError(f"ts_publish không hợp lệ: {ts_publish!r}")

    led = obj["led"]
    if not _is_int(led) or led not in (0, 1):
        raise ValidationError(f"led phải là 0 hoặc 1: {led!r}")

    values = {}
    for name, (low, high) in RANGES.items():
        value = obj[name]
        if not _is_number(value):
            raise ValidationError(f"{name} phải là số: {value!r}")
        value = float(value)
        if math.isnan(value) or math.isinf(value):
            raise ValidationError(f"{name} không hữu hạn: {value}")
        if not low <= value <= high:
            raise ValidationError(f"{name}={value} ngoài khoảng [{low}, {high}]")
        values[name] = value

    return {
        "device_id": device_id.strip(),
        "seq": seq,
        "ts_publish": ts_publish,
        "led": led,
        **values,
    }
