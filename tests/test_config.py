"""Unit tests for config module path construction and threshold values."""

import os
import importlib
import sys


def _import_config():
    """Import config.py as a plain Python module (skip Databricks magic)."""
    config_path = os.path.join(
        os.path.dirname(__file__), "..", "notebooks", "config.py"
    )
    spec = importlib.util.spec_from_file_location("config", config_path)
    mod = importlib.util.module_from_spec(spec)

    # Stub out Databricks-only names so the module loads in plain Python
    original_lines = open(config_path, encoding="utf-8").read()
    clean_lines = "\n".join(
        line for line in original_lines.splitlines()
        if not line.strip().startswith("# MAGIC")
        and not line.strip().startswith("# COMMAND")
        and "dbutils" not in line
    )

    # Execute the cleaned source in the module namespace
    exec(compile(clean_lines, config_path, "exec"), mod.__dict__)  # noqa: S102
    return mod


config = _import_config()


class TestPaths:
    def test_bronze_path_contains_bronze(self):
        assert "bronze" in config.BRONZE_PATH

    def test_silver_path_contains_silver(self):
        assert "silver" in config.SILVER_PATH

    def test_gold_base_ends_with_slash(self):
        assert config.GOLD_BASE.endswith("/")

    def test_audit_log_path_set(self):
        assert config.AUDIT_LOG_PATH is not None
        assert "audit" in config.AUDIT_LOG_PATH

    def test_export_path_set(self):
        assert config.EXPORT_PATH is not None


class TestGoldRegistry:
    def test_gold_tables_has_entries(self):
        assert len(config.GOLD_TABLES) >= 6

    def test_gold_table_names_includes_portfolio(self):
        assert "portfolio_summary" in config.GOLD_TABLE_NAMES

    def test_gold_table_names_superset_of_gold_tables(self):
        for key in config.GOLD_TABLES:
            assert key in config.GOLD_TABLE_NAMES


class TestThresholds:
    def test_age_range_sane(self):
        assert config.CUSTOMER_AGE_MIN < config.CUSTOMER_AGE_MAX
        assert config.CUSTOMER_AGE_MIN >= 0

    def test_vehicle_age_range_sane(self):
        assert config.VEHICLE_AGE_MIN < config.VEHICLE_AGE_MAX
        assert config.VEHICLE_AGE_MIN >= 0

    def test_ncap_range_sane(self):
        assert config.NCAP_RATING_MIN < config.NCAP_RATING_MAX
        assert config.NCAP_RATING_MIN >= 0

    def test_retry_settings(self):
        assert config.RETRY_MAX_ATTEMPTS >= 1
        assert config.RETRY_BASE_DELAY_SECONDS > 0

    def test_max_row_drop_pct(self):
        assert 0 < config.MAX_ROW_DROP_PCT <= 100


class TestEnvironment:
    def test_default_env_is_prod(self):
        # When no widget or env var is set, defaults to prod
        assert config.ENV == "prod"

    def test_env_override_via_env_var(self):
        os.environ["PIPELINE_ENV"] = "dev"
        reloaded = _import_config()
        assert reloaded.ENV == "dev"
        assert "/dev/" in reloaded.BRONZE_PATH
        del os.environ["PIPELINE_ENV"]
