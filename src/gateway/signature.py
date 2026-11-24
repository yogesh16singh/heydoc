import hashlib
import hmac


def sign(body: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def verify(body: bytes, header: str | None, secret: str) -> bool:
    if not header or not header.startswith("sha256="):
        return False
    given = header.split("=", 1)[1].strip().lower()
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    if len(given) != len(expected):
        return False
    return hmac.compare_digest(given, expected)
