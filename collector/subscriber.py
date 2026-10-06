

from __future__ import annotations

import logging
import os
import signal
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from quality import Deduper, GapTracker
from validation import ValidationError, parse_payload

log = logging.getLogger("collector")

COLLECTOR_NAME = "collector-1"  # giá trị tag `collector` của pipeline_stats
MQTT_CLIENT_ID = "ptit-iot-collector"  # cố định: chạy 2 collector cùng lúc sẽ đá nhau
STATS_INTERVAL_S = 30
MAX_PENDING = 1000  # số điểm tối đa chờ ghi khi DB lỗi; đầy thì bỏ điểm cũ nhất
RETRY_COOLDOWN_S = 5.0  # sau khi ghi lỗi, đợi chừng này giây mới thử lại
INFLUX_TIMEOUT_MS = 5000


# ----------------------------------------------------------------- cấu hình
@dataclass(frozen=True)
class Config:
    mqtt_host: str
    mqtt_port: int
    mqtt_tls: bool
    mqtt_user: str
    mqtt_pass: str
    mqtt_topic: str
    influx_url: str
    influx_org: str
    influx_token: str
    bucket_raw: str
    timestamp_source: str  # "receive" (giờ máy thu) hoặc "device" (ts_publish)


def load_config() -> Config:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    required = ["MQTT_HOST", "MQTT_TOPIC", "INFLUX_URL", "INFLUX_ORG", "INFLUX_TOKEN"]
    missing = [k for k in required if not os.getenv(k, "").strip()]
    if missing:
        raise SystemExit(f"Thiếu biến trong .env: {', '.join(missing)}")

    try:
        mqtt_port = int(os.getenv("MQTT_PORT", "8883"))
    except ValueError:
        raise SystemExit("MQTT_PORT phải là số nguyên")

    source = os.getenv("TIMESTAMP_SOURCE", "receive").strip().lower()
    if source not in ("receive", "device"):
        raise SystemExit("TIMESTAMP_SOURCE phải là 'receive' hoặc 'device'")

    return Config(
        mqtt_host=os.environ["MQTT_HOST"].strip(),
        mqtt_port=mqtt_port,
        mqtt_tls=os.getenv("MQTT_TLS", "true").strip().lower() in ("1", "true", "yes"),
        mqtt_user=os.getenv("MQTT_USER", "").strip(),
        mqtt_pass=os.getenv("MQTT_PASS", ""),
        mqtt_topic=os.environ["MQTT_TOPIC"].strip(),
        influx_url=os.environ["INFLUX_URL"].strip(),
        influx_org=os.environ["INFLUX_ORG"].strip(),
        influx_token=os.environ["INFLUX_TOKEN"].strip(),
        bucket_raw=os.getenv("INFLUX_BUCKET_RAW", "iot_raw").strip(),
        timestamp_source=source,
    )


# ------------------------------------------------------------ bộ đếm pipeline
@dataclass
class Stats:
    """Bộ đếm tích lũy từ lúc collector khởi động (reset khi chạy lại).

    received: bản tin hợp lệ, không trùng, được nhận để ghi (nên written == received
              khi DB ổn định); invalid/duplicate: bản tin bị loại;
    lost/restarts: suy ra từ seq; write_errors: số lần ghi DB thất bại.
    """

    received: int = 0
    invalid: int = 0
    duplicate: int = 0
    lost: int = 0
    restarts: int = 0
    written: int = 0
    write_errors: int = 0


def check_influx(client: InfluxDBClient, cfg: Config) -> None:
    """Kiểm tra kết nối, token và sự tồn tại của bucket trước khi chạy."""
    try:
        bucket = client.buckets_api().find_bucket_by_name(cfg.bucket_raw)
    except Exception as exc:
        raise SystemExit(
            f"Không kết nối được InfluxDB ({cfg.influx_url}): {exc}\n"
            "Kiểm tra Docker đang chạy và INFLUX_TOKEN / INFLUX_ORG trong .env."
        )
    if bucket is None:
        raise SystemExit(f"Không tìm thấy bucket '{cfg.bucket_raw}' trong InfluxDB.")


# ------------------------------------------------------------------ collector
class Collector:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        # on_message chạy ở luồng của paho, còn ghi stats chạy ở luồng chính
        self.lock = threading.Lock()
        self.stats = Stats()
        self.deduper = Deduper()
        self.gaps = GapTracker()
        # Hàng chờ ghi: (là_điểm_environment, Point)
        self.pending: deque[tuple[bool, Point]] = deque(maxlen=MAX_PENDING)
        self.retry_at = 0.0

        self.influx = InfluxDBClient(
            url=cfg.influx_url,
            token=cfg.influx_token,
            org=cfg.influx_org,
            timeout=INFLUX_TIMEOUT_MS,
        )
        check_influx(self.influx, cfg)
        self.write_api = self.influx.write_api(write_options=SYNCHRONOUS)
        self.mqtt = self._build_mqtt()

    # ---- MQTT
    def _build_mqtt(self) -> mqtt.Client:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
        if self.cfg.mqtt_user:
            client.username_pw_set(self.cfg.mqtt_user, self.cfg.mqtt_pass)
        if self.cfg.mqtt_tls:
            client.tls_set()  # xác thực chứng chỉ broker bằng kho CA của hệ thống
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        return client

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("MQTT kết nối thất bại: %s (kiểm tra MQTT_USER / MQTT_PASS)", reason_code)
            return
        log.info("MQTT đã kết nối %s:%s", self.cfg.mqtt_host, self.cfg.mqtt_port)
        # Đăng ký trong on_connect để tự đăng ký lại sau mỗi lần kết nối lại
        client.subscribe(self.cfg.mqtt_topic, qos=1)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        log.warning("MQTT mất kết nối (%s), đang tự kết nối lại...", reason_code)

    def _on_message(self, client, userdata, msg):
        t_recv_ms = time.time() * 1000.0
        try:
            with self.lock:
                self._process(msg.payload, t_recv_ms)
        except Exception:  # không để lỗi làm chết luồng MQTT
            log.exception("Lỗi không mong đợi khi xử lý bản tin")

    # ---- xử lý một bản tin
    def _process(self, payload: bytes, t_recv_ms: float) -> None:
        try:
            data = parse_payload(payload)
        except ValidationError as exc:
            self.stats.invalid += 1
            log.warning("INVALID: %s", exc)
            return

        device_id = data["device_id"]
        if self.deduper.is_duplicate((device_id, data["ts_publish"])):
            self.stats.duplicate += 1
            log.info("DUPLICATE seq=%d (bỏ qua)", data["seq"])
            return

        gap = self.gaps.update(device_id, data["seq"])
        self.stats.received += 1
        self.stats.lost += gap.lost
        if gap.restarted:
            self.stats.restarts += 1
            log.warning("Thiết bị %s khởi động lại (seq=%d)", device_id, data["seq"])
        if gap.lost:
            log.warning("Mất %d bản tin trước seq=%d", gap.lost, data["seq"])

        if self.cfg.timestamp_source == "device":
            ts_ms = data["ts_publish"]
        else:
            ts_ms = int(t_recv_ms)

        env = (
            Point("environment")
            .tag("device_id", device_id)
            .field("temperature", data["temperature"])
            .field("humidity", data["humidity"])
            .field("distance_cm", data["distance_cm"])
            .field("led", data["led"])
            .field("seq", data["seq"])
            .field("ts_publish", data["ts_publish"])
            .time(ts_ms, WritePrecision.MS)
        )
        self._enqueue(env, is_env=True)
        write_ms = self._flush()

        if write_ms is None:
            log.warning(
                "QUEUED seq=%d (chưa ghi được DB, đang chờ: %d điểm)",
                data["seq"],
                len(self.pending),
            )
            return

        # network_ms phụ thuộc đồng hồ thiết bị (xem ghi chú về Wokwi trong báo cáo)
        network_ms = t_recv_ms - data["ts_publish"]
        e2e_ms = network_ms + write_ms
        latency = (
            Point("latency")
            .tag("device_id", device_id)
            .field("network_ms", round(network_ms, 1))
            .field("write_ms", round(write_ms, 1))
            .field("e2e_ms", round(e2e_ms, 1))
            .time(ts_ms, WritePrecision.MS)
        )
        # write_ms chỉ biết sau khi ghi xong, nên điểm latency được ghi kèm lần ghi kế tiếp
        self._enqueue(latency, is_env=False)

        log.info(
            "OK seq=%d T=%.1f H=%.1f D=%.1fcm LED=%d | network=%.0fms e2e=%.0fms",
            data["seq"],
            data["temperature"],
            data["humidity"],
            data["distance_cm"],
            data["led"],
            network_ms,
            e2e_ms,
        )

    # ---- ghi InfluxDB
    def _enqueue(self, point: Point, is_env: bool) -> None:
        if len(self.pending) == self.pending.maxlen:
            log.warning("Hàng chờ ghi đầy (%d điểm), bỏ điểm cũ nhất", MAX_PENDING)
        self.pending.append((is_env, point))

    def _flush(self) -> float | None:
        """Ghi toàn bộ hàng chờ. Trả về thời gian ghi (ms), hoặc None nếu chưa ghi được."""
        if not self.pending:
            return None
        if time.monotonic() < self.retry_at:
            return None

        batch = list(self.pending)
        started = time.perf_counter()
        try:
            self.write_api.write(
                bucket=self.cfg.bucket_raw,
                org=self.cfg.influx_org,
                record=[point for _, point in batch],
                write_precision=WritePrecision.MS,
            )
        except Exception as exc:
            self.stats.write_errors += 1
            self.retry_at = time.monotonic() + RETRY_COOLDOWN_S
            log.error("Ghi InfluxDB lỗi: %s", exc)
            return None

        write_ms = (time.perf_counter() - started) * 1000.0
        self.pending.clear()
        self.stats.written += sum(1 for is_env, _ in batch if is_env)
        return write_ms

    def write_stats(self) -> None:
        with self.lock:
            point = Point("pipeline_stats").tag("collector", COLLECTOR_NAME)
            for name, value in asdict(self.stats).items():
                point = point.field(name, int(value))
            point = point.time(int(time.time() * 1000), WritePrecision.MS)
            self._enqueue(point, is_env=False)
            self._flush()
            s = self.stats
            log.info(
                "STATS received=%d written=%d invalid=%d duplicate=%d lost=%d "
                "restarts=%d write_errors=%d",
                s.received, s.written, s.invalid, s.duplicate, s.lost,
                s.restarts, s.write_errors,
            )

    # ---- vòng đời
    def start(self) -> None:
        # connect_async + loop_start: mất mạng lúc khởi động vẫn tự thử lại
        self.mqtt.connect_async(self.cfg.mqtt_host, self.cfg.mqtt_port, keepalive=60)
        self.mqtt.loop_start()

    def stop(self) -> None:
        self.mqtt.disconnect()
        self.mqtt.loop_stop()
        self.write_stats()
        self.influx.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    cfg = load_config()
    collector = Collector(cfg)

    stop = threading.Event()

    def _request_stop(signum, frame):
        stop.set()

    signal.signal(signal.SIGINT, _request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _request_stop)

    collector.start()
    log.info("Đang chạy, topic=%s. Nhấn Ctrl+C để dừng.", cfg.mqtt_topic)

    last_stats = time.monotonic()
    while not stop.wait(1.0):
        if time.monotonic() - last_stats >= STATS_INTERVAL_S:
            collector.write_stats()
            last_stats = time.monotonic()

    log.info("Đang dừng...")
    collector.stop()


if __name__ == "__main__":
    main()
