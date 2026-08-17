"""Generate a deterministic, non-speech WAV fixture without external services."""

from __future__ import annotations

import argparse
import io
import struct
import wave
from pathlib import Path

SAMPLE_RATE = 16_000
SAMPLE_WIDTH_BYTES = 2
CHANNELS = 1
AMPLITUDE = 5_000


def _square_wave(frequency_hz: int, duration_ms: int) -> bytes:
    frame_count = SAMPLE_RATE * duration_ms // 1_000
    frames = bytearray()
    for index in range(frame_count):
        phase = (index * frequency_hz * 2) // SAMPLE_RATE
        sample = AMPLITUDE if phase % 2 == 0 else -AMPLITUDE
        frames.extend(struct.pack("<h", sample))
    return bytes(frames)


def _silence(duration_ms: int) -> bytes:
    frame_count = SAMPLE_RATE * duration_ms // 1_000
    return b"\x00\x00" * frame_count


def build_wav_bytes() -> bytes:
    """Return a stable mono/16 kHz/16-bit waveform with call-like turns."""
    pcm = b"".join(
        [
            _silence(250),
            _square_wave(440, 750),
            _silence(300),
            _square_wave(660, 900),
            _silence(300),
            _square_wave(520, 700),
            _silence(250),
        ]
    )
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav_file:
        wav_file.setnchannels(CHANNELS)
        wav_file.setsampwidth(SAMPLE_WIDTH_BYTES)
        wav_file.setframerate(SAMPLE_RATE)
        wav_file.writeframes(pcm)
    return buffer.getvalue()


def write_fixture(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(build_wav_bytes())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("samples/generated/synthetic_support_call.wav"),
    )
    args = parser.parse_args()
    write_fixture(args.output)
    print(f"Wrote deterministic synthetic fixture: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
