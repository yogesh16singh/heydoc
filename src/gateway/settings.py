from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://heydoc:heydoc@localhost:5432/heydoc"
    app_secret: str = "dev-app-secret"
    verify_token: str = "dev-verify-token"
    rate_limit_per_minute: int = 20
    lab_base_url: str = "http://localhost:8001"
    lab_fail_rate: float = 0.2
    lab_min_latency_ms: int = 500
    lab_max_latency_ms: int = 1500
    lab_seed: int = 7
    lab_client_timeout_s: float = 4.0
    lab_client_chaos: str | None = None
    lab_client_max_attempts: int = 5
    worker_concurrency: int = 16
    lease_seconds: int = 90
    max_job_attempts: int = 5
    poll_ms: int = 250
    reconcile_interval_s: float = 2.0
    reconcile_min_age_s: float = 3.0
    llm_provider: str = "mock"
    openrouter_api_key: str = ""
    llm_base_url: str = "https://openrouter.ai/api/v1"
    llm_model: str = "deepseek/deepseek-chat"
    max_spend_inr: float = 500
    max_llm_calls: int = 100000
    usd_inr: float = 84.0
    input_usd_per_mtok: float = 0.10
    output_usd_per_mtok: float = 0.40
    dev_mode: bool = True
    api_port: int = 8000
    lab_port: int = 8001
    db_pool_max: int = 48
    chaos_hold_s: float = 2.0


settings = Settings()
