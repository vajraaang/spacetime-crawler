"""
Minimal CBOR encoder/decoder.

This project depends on a `cbor` module (typically from PyPI) for communication
with the spacetime services and the course cache server. Some environments used
for the class ship with a Python version where the external `cbor` package is
not readily available. To keep the crawler runnable without external installs,
we provide a small, pure-Python subset implementation here.

Supported types:
- None, bool
- int
- float (encoded as float64)
- bytes/bytearray/memoryview
- str (UTF-8)
- list/tuple/set/frozenset
- dict (with keys/values from supported types)

Decoding supports both definite and indefinite length strings/arrays/maps.
"""

from __future__ import annotations

import struct
from typing import Any, Tuple


class CBORError(Exception):
    pass


class CBORDecodeError(CBORError):
    pass


class CBOREncodeError(CBORError):
    pass


_BREAK = object()


def dumps(obj: Any) -> bytes:
    out = bytearray()
    _encode(obj, out)
    return bytes(out)


def dump(obj: Any, fp) -> None:
    fp.write(dumps(obj))


def loads(data: bytes | bytearray | memoryview) -> Any:
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("cbor.loads expects a bytes-like object")
    raw = bytes(data)
    value, idx = _decode(raw, 0)
    if idx != len(raw):
        raise CBORDecodeError(f"Trailing bytes after CBOR object: {len(raw) - idx}")
    return value


def load(fp) -> Any:
    return loads(fp.read())


def _encode(obj: Any, out: bytearray) -> None:
    if obj is None:
        out.append(0xF6)
        return
    if obj is False:
        out.append(0xF4)
        return
    if obj is True:
        out.append(0xF5)
        return

    if isinstance(obj, int) and not isinstance(obj, bool):
        if obj >= 0:
            out.extend(_encode_type_and_len(0, obj))
        else:
            out.extend(_encode_type_and_len(1, -1 - obj))
        return

    if isinstance(obj, float):
        # Encode as float64.
        out.append(0xFB)
        out.extend(struct.pack("!d", obj))
        return

    if isinstance(obj, (bytes, bytearray, memoryview)):
        b = bytes(obj)
        out.extend(_encode_type_and_len(2, len(b)))
        out.extend(b)
        return

    if isinstance(obj, str):
        b = obj.encode("utf-8")
        out.extend(_encode_type_and_len(3, len(b)))
        out.extend(b)
        return

    if isinstance(obj, (list, tuple, set, frozenset)):
        items = list(obj)
        out.extend(_encode_type_and_len(4, len(items)))
        for item in items:
            _encode(item, out)
        return

    if isinstance(obj, dict):
        out.extend(_encode_type_and_len(5, len(obj)))
        for k, v in obj.items():
            _encode(k, out)
            _encode(v, out)
        return

    raise CBOREncodeError(f"Unsupported type for CBOR encoding: {type(obj)!r}")


def _encode_type_and_len(major: int, length: int) -> bytes:
    if length < 0:
        raise CBOREncodeError("Negative length is invalid")
    if length < 24:
        return bytes([(major << 5) | length])
    if length < 256:
        return bytes([(major << 5) | 24, length])
    if length < 65536:
        return bytes([(major << 5) | 25]) + struct.pack("!H", length)
    if length < 2**32:
        return bytes([(major << 5) | 26]) + struct.pack("!I", length)
    if length < 2**64:
        return bytes([(major << 5) | 27]) + struct.pack("!Q", length)
    raise CBOREncodeError("Length too large")


def _read_n(data: bytes, idx: int, n: int) -> Tuple[bytes, int]:
    end = idx + n
    if end > len(data):
        raise CBORDecodeError("Unexpected end of data")
    return data[idx:end], end


def _read_uint(data: bytes, idx: int, addl: int) -> Tuple[int | None, int]:
    if addl < 24:
        return addl, idx
    if addl == 24:
        b, idx = _read_n(data, idx, 1)
        return b[0], idx
    if addl == 25:
        b, idx = _read_n(data, idx, 2)
        return struct.unpack("!H", b)[0], idx
    if addl == 26:
        b, idx = _read_n(data, idx, 4)
        return struct.unpack("!I", b)[0], idx
    if addl == 27:
        b, idx = _read_n(data, idx, 8)
        return struct.unpack("!Q", b)[0], idx
    if addl == 31:
        return None, idx  # Indefinite length.
    raise CBORDecodeError(f"Invalid additional info: {addl}")


def _decode(data: bytes, idx: int) -> Tuple[Any, int]:
    if idx >= len(data):
        raise CBORDecodeError("Unexpected end of data")
    initial = data[idx]
    idx += 1
    major = initial >> 5
    addl = initial & 0x1F

    if major in (0, 1, 2, 3, 4, 5, 6):
        length, idx = _read_uint(data, idx, addl)

    if major == 0:
        assert length is not None
        return int(length), idx

    if major == 1:
        assert length is not None
        return -1 - int(length), idx

    if major == 2:
        if length is None:
            chunks = []
            while True:
                chunk, idx = _decode(data, idx)
                if chunk is _BREAK:
                    break
                if not isinstance(chunk, (bytes, bytearray)):
                    raise CBORDecodeError("Indefinite byte string contained non-bytes chunk")
                chunks.append(bytes(chunk))
            return b"".join(chunks), idx
        b, idx = _read_n(data, idx, int(length))
        return b, idx

    if major == 3:
        if length is None:
            parts = []
            while True:
                part, idx = _decode(data, idx)
                if part is _BREAK:
                    break
                if not isinstance(part, str):
                    raise CBORDecodeError("Indefinite text string contained non-text chunk")
                parts.append(part)
            return "".join(parts), idx
        b, idx = _read_n(data, idx, int(length))
        try:
            return b.decode("utf-8"), idx
        except UnicodeDecodeError as e:
            raise CBORDecodeError("Invalid UTF-8 text string") from e

    if major == 4:
        if length is None:
            items = []
            while True:
                item, idx = _decode(data, idx)
                if item is _BREAK:
                    break
                items.append(item)
            return items, idx
        items = []
        for _ in range(int(length)):
            item, idx = _decode(data, idx)
            if item is _BREAK:
                raise CBORDecodeError("Unexpected break in definite-length array")
            items.append(item)
        return items, idx

    if major == 5:
        if length is None:
            m = {}
            while True:
                k, idx = _decode(data, idx)
                if k is _BREAK:
                    break
                v, idx = _decode(data, idx)
                if v is _BREAK:
                    raise CBORDecodeError("Unexpected break in indefinite-length map value")
                m[k] = v
            return m, idx
        m = {}
        for _ in range(int(length)):
            k, idx = _decode(data, idx)
            v, idx = _decode(data, idx)
            if k is _BREAK or v is _BREAK:
                raise CBORDecodeError("Unexpected break in definite-length map")
            m[k] = v
        return m, idx

    if major == 6:
        # Tag; ignore and return the tagged value.
        # length is the tag number.
        _tag = length
        value, idx = _decode(data, idx)
        return value, idx

    if major == 7:
        if addl == 20:
            return False, idx
        if addl == 21:
            return True, idx
        if addl == 22:
            return None, idx
        if addl == 23:
            # Undefined; map to None for simplicity.
            return None, idx
        if addl == 24:
            b, idx = _read_n(data, idx, 1)
            return b[0], idx
        if addl == 25:
            b, idx = _read_n(data, idx, 2)
            return _half_to_float(struct.unpack("!H", b)[0]), idx
        if addl == 26:
            b, idx = _read_n(data, idx, 4)
            return struct.unpack("!f", b)[0], idx
        if addl == 27:
            b, idx = _read_n(data, idx, 8)
            return struct.unpack("!d", b)[0], idx
        if addl == 31:
            return _BREAK, idx
        # Unassigned simple values: return the raw addl info.
        return addl, idx

    raise CBORDecodeError(f"Unsupported major type: {major}")


def _half_to_float(h: int) -> float:
    # IEEE 754 half-precision to float.
    sign = (h >> 15) & 0x1
    exp = (h >> 10) & 0x1F
    frac = h & 0x3FF

    if exp == 0:
        if frac == 0:
            return -0.0 if sign else 0.0
        # Subnormal.
        return (-1.0 if sign else 1.0) * (frac / 2**10) * 2**(-14)

    if exp == 0x1F:
        if frac == 0:
            return float("-inf") if sign else float("inf")
        return float("nan")

    return (-1.0 if sign else 1.0) * (1.0 + frac / 2**10) * 2 ** (exp - 15)

