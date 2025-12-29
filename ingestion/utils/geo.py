import hashlib


def compute_checksum(payload: str, geom_wkb: bytes | None) -> str:
    """
    Compute SHA256 checksum of payload + geometry.
    Geometry may be None.
    """
    h = hashlib.sha256()
    h.update(payload.encode("utf-8"))
    if geom_wkb:
        h.update(geom_wkb)
    return h.hexdigest()
