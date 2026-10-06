
from collections import OrderedDict
from dataclasses import dataclass


class Deduper:
    """Nhận biết bản tin trùng theo khóa (device_id, ts_publish).

    Chỉ nhớ tối đa `max_size` khóa gần nhất nên bộ nhớ không phình ra.
    """

    def __init__(self, max_size: int = 5000):
        if max_size < 1:
            raise ValueError("max_size phải >= 1")
        self._max_size = max_size
        self._seen: OrderedDict = OrderedDict()

    def is_duplicate(self, key) -> bool:
        """True nếu khóa đã thấy trước đó; nếu chưa thì ghi nhớ và trả về False."""
        if key in self._seen:
            return True
        self._seen[key] = None
        if len(self._seen) > self._max_size:
            self._seen.popitem(last=False)  # bỏ khóa cũ nhất
        return False


@dataclass(frozen=True)
class GapResult:
    lost: int  # số bản tin bị mất giữa lần trước và lần này
    restarted: bool  # True nếu thiết bị khởi động lại (seq giảm)


class GapTracker:
    """Đếm bản tin mất theo seq của từng thiết bị."""

    def __init__(self):
        self._last_seq: dict = {}

    def update(self, device_id: str, seq: int) -> GapResult:
        last = self._last_seq.get(device_id)
        self._last_seq[device_id] = seq
        if last is None:  # bản tin đầu tiên của thiết bị: chưa có mốc so sánh
            return GapResult(lost=0, restarted=False)
        if seq < last:  # seq quay về nhỏ hơn: thiết bị khởi động lại
            return GapResult(lost=0, restarted=True)
        return GapResult(lost=max(0, seq - last - 1), restarted=False)
