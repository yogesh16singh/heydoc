import json

from gateway.catalog import detect_test, selected_slot, wants_handoff
from gateway.payload import cloud_payload
from gateway.signature import sign, verify


def test_signature_round_trip():
    body = b'{"ok":true}'
    header = sign(body, "test-secret")
    assert verify(body, header, "test-secret")
    assert not verify(body + b" ", header, "test-secret")
    assert not verify(body, "sha256=abcd", "test-secret")
    assert not verify(body, None, "test-secret")
    assert not verify(body, "deadbeef", "test-secret")


def test_detect_and_select():
    assert detect_test("Book a Vitamin D test") == "VITD"
    assert detect_test("CBC please") == "CBC"
    assert wants_handoff("I need a doctor")
    assert not wants_handoff("thanks")
    offered = [{"id": "CBC:2099-01-01:0900", "label": "09:00"}, {"id": "CBC:2099-01-01:1000", "label": "10:00"}]
    assert selected_slot("1", offered) == "CBC:2099-01-01:0900"
    assert selected_slot("10:00", offered) == "CBC:2099-01-01:1000"
    assert selected_slot("no", offered) is None


def test_webhook_signature_and_challenge(http):
    secret = "test-secret"
    raw = json.dumps(cloud_payload("919800000001", "Hi", "wamid.sig.1")).encode()
    bad = http.post("/webhook", content=raw, headers={"x-hub-signature-256": sign(raw, "other")})
    assert bad.status_code == 401
    missing = http.post("/webhook", content=raw)
    assert missing.status_code == 401
    tampered = http.post(
        "/webhook",
        content=raw + b" ",
        headers={"x-hub-signature-256": sign(raw, secret)},
    )
    assert tampered.status_code == 401
    ok = http.post("/webhook", content=raw, headers={"x-hub-signature-256": sign(raw, secret)})
    assert ok.status_code == 200
    again = http.post("/webhook", content=raw, headers={"x-hub-signature-256": sign(raw, secret)})
    assert again.status_code == 200

    challenge = http.get("/webhook", params={"hub.mode": "subscribe", "hub.verify_token": "test-verify", "hub.challenge": "42"})
    assert challenge.status_code == 200
    assert challenge.text == "42"
    denied = http.get("/webhook", params={"hub.mode": "subscribe", "hub.verify_token": "nope", "hub.challenge": "42"})
    assert denied.status_code == 403

    malformed = http.post("/webhook", content=b"not-json", headers={"x-hub-signature-256": sign(b"not-json", secret)})
    assert malformed.status_code == 400
    status_only = {
        "object": "whatsapp_business_account",
        "entry": [{"changes": [{"value": {"statuses": [{"id": "x", "status": "read"}]}}]}],
    }
    raw_status = json.dumps(status_only).encode()
    accepted = http.post("/webhook", content=raw_status, headers={"x-hub-signature-256": sign(raw_status, secret)})
    assert accepted.status_code == 200
    assert http.get("/healthz").status_code == 200
    assert http.get("/readyz").status_code == 200
