from __future__ import annotations

import io
import sys
import unittest
import wave
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from samples.generate_synthetic_wav import (  # noqa: E402
    CHANNELS,
    SAMPLE_RATE,
    SAMPLE_WIDTH_BYTES,
    build_wav_bytes,
)
from tools.validate_repo import (  # noqa: E402
    validate_sample,
    validate_schema_privacy,
    validate_taxonomies,
    validate_workflows,
)


class RepositoryContractTests(unittest.TestCase):
    def test_workflow_contracts(self) -> None:
        self.assertEqual(validate_workflows(), [])

    def test_taxonomy_contracts(self) -> None:
        self.assertEqual(validate_taxonomies(), [])

    def test_gold_schema_privacy_contract(self) -> None:
        self.assertEqual(validate_schema_privacy(), [])

    def test_public_sample_contract(self) -> None:
        self.assertEqual(validate_sample(), [])

    def test_synthetic_wav_is_deterministic_and_well_formed(self) -> None:
        first = build_wav_bytes()
        second = build_wav_bytes()
        self.assertEqual(first, second)
        with wave.open(io.BytesIO(first), "rb") as wav_file:
            self.assertEqual(wav_file.getnchannels(), CHANNELS)
            self.assertEqual(wav_file.getsampwidth(), SAMPLE_WIDTH_BYTES)
            self.assertEqual(wav_file.getframerate(), SAMPLE_RATE)
            self.assertGreater(wav_file.getnframes(), SAMPLE_RATE)


if __name__ == "__main__":
    unittest.main()
