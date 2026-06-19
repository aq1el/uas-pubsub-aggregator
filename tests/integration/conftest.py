"""
conftest.py
===========
- `wait_for_service`: fixture session-scope yang menunggu aggregator siap
  sebelum test integration mulai jalan (mencegah false-fail karena container
  belum sempat boot).
- `wait_until`: helper polling, dipakai karena worker memproses event secara
  ASYNC (eventual consistency) -- assert tidak boleh langsung dilakukan
  sesaat setelah POST /publish, harus menunggu sampai kondisi terpenuhi atau
  timeout.

Jalankan dulu `docker compose up --build` di root project SEBELUM menjalankan
test integration (test_integration.py). Test unit (test_models.py) tidak butuh
Docker sama sekali.
"""
import time

import pytest
import requests

BASE_URL = "http://localhost:8080"


@pytest.fixture(scope="session", autouse=True)
def wait_for_service():
    for _ in range(40):
        try:
            r = requests.get(f"{BASE_URL}/health", timeout=2)
            if r.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(1)
    pytest.skip(
        "Aggregator tidak terjangkau di http://localhost:8080. "
        "Jalankan 'docker compose up --build' dahulu sebelum menjalankan test integration."
    )


def wait_until(predicate, timeout: float = 15, interval: float = 0.3) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        if predicate():
            return True
        time.sleep(interval)
    return False
