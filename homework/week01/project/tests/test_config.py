from __future__ import annotations

from app.config import load_settings


def test_yaml_loads_environment_variables_and_resolves_database_path(tmp_path, monkeypatch):
    config = tmp_path / "gateway.yaml"
    config.write_text(
        """
api_keys:
  - ${GATEWAY_KEY}
database_url: data/test.db
providers:
  upstream:
    protocol: responses
    base_url: https://example.test/
    api_key: ${UPSTREAM_KEY}
models:
  pro:
    provider: upstream
    upstream_model: real-pro
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("GATEWAY_KEY", "gateway-secret")
    monkeypatch.setenv("UPSTREAM_KEY", "upstream-secret")

    settings = load_settings(config)

    assert settings.api_keys[0].get_secret_value() == "gateway-secret"
    assert settings.providers["upstream"].api_key.get_secret_value() == "upstream-secret"
    assert settings.providers["upstream"].base_url == "https://example.test"
    assert settings.database_url == str((tmp_path / "data/test.db").resolve())
