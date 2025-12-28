import re
from pathlib import Path


def test_monitor_default_max_wait_time_is_11_hours():
    # Some environments have a broken requests/pyOpenSSL import chain.
    # To keep this unit test hermetic, we avoid importing meeting_monitor and
    # instead assert on the function signature in the source.
    src = Path(__file__).with_name("meeting_monitor.py").read_text(encoding="utf-8")

    # Look for: max_wait_time: int = 39600
    m = re.search(r"max_wait_time\s*:\s*int\s*=\s*(\d+)\s*,", src)
    assert m is not None
    assert int(m.group(1)) == 39600
