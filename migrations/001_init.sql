CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS lab;

CREATE TABLE conversations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  phone text NOT NULL UNIQUE,
  state text NOT NULL DEFAULT 'collecting',
  memory jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT conversations_state CHECK (state IN ('collecting', 'offering', 'booked', 'handed_off'))
);

CREATE TABLE inbound_messages (
  id bigserial PRIMARY KEY,
  wa_message_id text NOT NULL UNIQUE,
  phone text NOT NULL,
  body text,
  msg_type text NOT NULL DEFAULT 'text',
  raw jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT inbound_status CHECK (status IN ('received', 'queued', 'rate_limited', 'ignored'))
);

CREATE INDEX inbound_messages_phone_id ON inbound_messages (phone, id);

CREATE TABLE rate_limits (
  phone text NOT NULL,
  window_start timestamptz NOT NULL,
  count integer NOT NULL,
  PRIMARY KEY (phone, window_start)
);

CREATE TABLE jobs (
  id bigserial PRIMARY KEY,
  message_id bigint NOT NULL UNIQUE REFERENCES inbound_messages (id),
  conversation_id uuid NOT NULL REFERENCES conversations (id),
  status text NOT NULL DEFAULT 'pending',
  attempts integer NOT NULL DEFAULT 0,
  next_run_at timestamptz NOT NULL DEFAULT now(),
  locked_until timestamptz,
  claim_token uuid,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT jobs_status CHECK (status IN ('pending', 'running', 'done', 'dead'))
);

CREATE UNIQUE INDEX jobs_one_running ON jobs (conversation_id) WHERE status = 'running';
CREATE INDEX jobs_pending_order ON jobs (id) WHERE status = 'pending';

CREATE TABLE bookings (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_id uuid NOT NULL REFERENCES conversations (id),
  phone text NOT NULL,
  test_code text NOT NULL,
  slot_id text NOT NULL,
  attempt_id uuid NOT NULL UNIQUE,
  lab_booking_id text UNIQUE,
  patient_name text,
  status text NOT NULL,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT bookings_status CHECK (status IN ('pending', 'confirmed', 'failed', 'cancelled'))
);

CREATE UNIQUE INDEX bookings_one_active_slot
  ON bookings (slot_id) WHERE status IN ('pending', 'confirmed');
CREATE INDEX bookings_pending_updated ON bookings (updated_at) WHERE status = 'pending';

CREATE TABLE outbox (
  id bigserial PRIMARY KEY,
  conversation_id uuid NOT NULL REFERENCES conversations (id),
  phone text NOT NULL,
  body text NOT NULL,
  message_id bigint UNIQUE,
  status text NOT NULL DEFAULT 'sent',
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE handoffs (
  id bigserial PRIMARY KEY,
  conversation_id uuid NOT NULL REFERENCES conversations (id),
  reason text NOT NULL,
  summary text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE llm_usage (
  id bigserial PRIMARY KEY,
  conversation_id uuid REFERENCES conversations (id),
  message_id bigint,
  model text NOT NULL,
  input_tokens integer NOT NULL DEFAULT 0,
  output_tokens integer NOT NULL DEFAULT 0,
  cost_inr numeric(14, 6) NOT NULL DEFAULT 0,
  outcome text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX llm_usage_conversation ON llm_usage (conversation_id);

CREATE TABLE llm_budget (
  id integer PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  spent_inr numeric(14, 6) NOT NULL DEFAULT 0,
  calls integer NOT NULL DEFAULT 0,
  cap_inr numeric(14, 6) NOT NULL DEFAULT 500,
  max_calls integer NOT NULL DEFAULT 100000
);

INSERT INTO llm_budget (id) VALUES (1);

CREATE TABLE lab_calls (
  id bigserial PRIMARY KEY,
  op text NOT NULL,
  outcome text NOT NULL,
  attempt_id text,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Append-only. A single counter row would serialize every webhook.
CREATE TABLE ingress_events (
  id bigserial PRIMARY KEY,
  kind text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX ingress_events_kind ON ingress_events (kind);

CREATE TABLE lab.slots (
  slot_id text PRIMARY KEY,
  test_code text NOT NULL,
  slot_date date NOT NULL,
  label text NOT NULL,
  starts_at timestamptz NOT NULL
);

CREATE INDEX lab_slots_lookup ON lab.slots (test_code, slot_date);

CREATE TABLE lab.bookings (
  idempotency_key text PRIMARY KEY,
  slot_id text NOT NULL UNIQUE,
  test_code text NOT NULL,
  phone text NOT NULL,
  patient_name text NOT NULL,
  lab_booking_id text NOT NULL UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE lab.call_log (
  id bigserial PRIMARY KEY,
  op text NOT NULL,
  outcome text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- One round trip from the webhook. Statements inside the function see each
-- other's writes; a CTE chain would not, because those share one snapshot.
CREATE OR REPLACE FUNCTION accept_inbound(
  p_wa_message_id text,
  p_phone text,
  p_body text,
  p_msg_type text,
  p_raw jsonb,
  p_limit integer
) RETURNS TABLE (
  inserted boolean,
  queued boolean,
  rate_limited boolean,
  duplicate boolean,
  ignored boolean
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_id bigint;
  v_count integer;
  v_conv uuid;
BEGIN
  INSERT INTO inbound_messages (wa_message_id, phone, body, msg_type, raw, status)
  VALUES (p_wa_message_id, p_phone, p_body, COALESCE(NULLIF(p_msg_type, ''), 'text'), p_raw, 'received')
  ON CONFLICT (wa_message_id) DO NOTHING
  RETURNING id INTO v_id;

  IF v_id IS NULL THEN
    INSERT INTO ingress_events (kind) VALUES ('duplicates');
    RETURN QUERY SELECT false, false, false, true, false;
    RETURN;
  END IF;

  IF COALESCE(p_msg_type, 'text') <> 'text' OR p_body IS NULL OR length(btrim(p_body)) = 0 THEN
    UPDATE inbound_messages SET status = 'ignored' WHERE id = v_id;
    INSERT INTO ingress_events (kind) VALUES ('ignored');
    RETURN QUERY SELECT true, false, false, false, true;
    RETURN;
  END IF;

  INSERT INTO rate_limits (phone, window_start, count)
  VALUES (p_phone, date_trunc('minute', now()), 1)
  ON CONFLICT (phone, window_start)
  DO UPDATE SET count = rate_limits.count + 1
  RETURNING rate_limits.count INTO v_count;

  IF v_count > p_limit THEN
    UPDATE inbound_messages SET status = 'rate_limited' WHERE id = v_id;
    INSERT INTO ingress_events (kind) VALUES ('rate_limited');
    RETURN QUERY SELECT true, false, true, false, false;
    RETURN;
  END IF;

  INSERT INTO conversations (phone)
  VALUES (p_phone)
  ON CONFLICT (phone) DO UPDATE SET updated_at = now()
  RETURNING id INTO v_conv;

  UPDATE inbound_messages SET status = 'queued' WHERE id = v_id;

  INSERT INTO jobs (message_id, conversation_id, status, next_run_at)
  VALUES (v_id, v_conv, 'pending', now());

  INSERT INTO ingress_events (kind) VALUES ('accepted');
  PERFORM pg_notify('jobs', v_id::text);

  RETURN QUERY SELECT true, true, false, false, false;
END;
$$;
