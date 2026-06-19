"""Menambahkan folder aggregator/ ke sys.path supaya test bisa `import app.models`
langsung tanpa perlu menjalankan Docker (ini test UNIT murni)."""
import pathlib
import sys

AGGREGATOR_DIR = pathlib.Path(__file__).resolve().parents[2] / "aggregator"
sys.path.insert(0, str(AGGREGATOR_DIR))
