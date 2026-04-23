import builtins
import math
import struct

try:
    import audioop as _audioop
except ImportError:  # pragma: no cover - exercised only on runtimes without audioop
    _audioop = None


def _clip_sample(value: int, sample_width: int) -> int:
    if sample_width == 1:
        return builtins.max(-128, builtins.min(127, int(value)))
    if sample_width == 2:
        return builtins.max(-32768, builtins.min(32767, int(value)))
    if sample_width == 4:
        return builtins.max(-(2 ** 31), builtins.min(2 ** 31 - 1, int(value)))
    raise ValueError(f"unsupported sample width: {sample_width}")


def _iter_samples(fragment: bytes, sample_width: int):
    if sample_width == 1:
        for byte in fragment:
            yield int(byte) - 128
        return
    if sample_width == 2:
        count = len(fragment) // 2
        for sample in struct.unpack("<" + "h" * count, fragment[: count * 2]):
            yield int(sample)
        return
    if sample_width == 4:
        count = len(fragment) // 4
        for sample in struct.unpack("<" + "i" * count, fragment[: count * 4]):
            yield int(sample)
        return
    raise ValueError(f"unsupported sample width: {sample_width}")


def _pack_samples(samples, sample_width: int) -> bytes:
    clipped = [_clip_sample(sample, sample_width) for sample in samples]
    if sample_width == 1:
        return bytes((sample + 128) & 0xFF for sample in clipped)
    if sample_width == 2:
        return struct.pack("<" + "h" * len(clipped), *clipped)
    if sample_width == 4:
        return struct.pack("<" + "i" * len(clipped), *clipped)
    raise ValueError(f"unsupported sample width: {sample_width}")


def tomono(fragment: bytes, sample_width: int, left_factor: float, right_factor: float) -> bytes:
    if _audioop is not None:
        return _audioop.tomono(fragment, sample_width, left_factor, right_factor)
    samples = list(_iter_samples(fragment, sample_width))
    if len(samples) < 2:
        return fragment
    if len(samples) % 2 == 1:
        samples = samples[:-1]
    mixed = [
        int(left * left_factor + right * right_factor)
        for left, right in zip(samples[0::2], samples[1::2])
    ]
    return _pack_samples(mixed, sample_width)


def rms(fragment: bytes, sample_width: int) -> int:
    if _audioop is not None:
        return _audioop.rms(fragment, sample_width)
    samples = list(_iter_samples(fragment, sample_width))
    if not samples:
        return 0
    return int(math.sqrt(sum(sample * sample for sample in samples) / len(samples)))


def max(fragment: bytes, sample_width: int) -> int:  # pylint: disable=redefined-builtin
    if _audioop is not None:
        return _audioop.max(fragment, sample_width)
    samples = list(_iter_samples(fragment, sample_width))
    if not samples:
        return 0
    return builtins.max(abs(sample) for sample in samples)


class _AudioOpCompat:
    tomono = staticmethod(tomono)
    rms = staticmethod(rms)
    max = staticmethod(max)


audioop = _AudioOpCompat()
