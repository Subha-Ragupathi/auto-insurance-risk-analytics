"""Unit tests for transformation logic used in the silver layer.

These tests validate pure-Python equivalents of the PySpark transformation
rules so that logic correctness can be verified in CI without a Spark
cluster.
"""

import pytest


# ---------------------------------------------------------------------------
# Re-implement the transformation rules as pure functions so they can be
# tested without PySpark.  In production the same logic runs inside Spark
# withColumn expressions.
# ---------------------------------------------------------------------------

def bool_encode(value):
    """Replicate the Yes/No -> 1/0 encoding from silver layer."""
    if value is None:
        return None
    normalised = str(value).strip().lower()
    if normalised == "yes":
        return 1
    if normalised == "no":
        return 0
    return None


def standardise_text(value):
    """Replicate the trim + upper transformation from silver layer."""
    if value is None:
        return None
    return str(value).strip().upper()


def is_valid_claim_status(value):
    """Check domain rule: claim_status must be 0 or 1."""
    return value in (0, 1)


def fill_null_claim_status(value):
    """Replicate null handling: None -> 0."""
    return 0 if value is None else value


def is_within_range(value, min_val, max_val):
    """Replicate range filter logic."""
    if value is None:
        return False
    return min_val <= value <= max_val


def assign_vehicle_age_band(vehicle_age):
    """Replicate vehicle age band derivation from gold layer."""
    if vehicle_age is None:
        return None
    if vehicle_age <= 1:
        return "0-1"
    if vehicle_age <= 3:
        return "2-3"
    if vehicle_age <= 5:
        return "4-5"
    return "5+"


def assign_customer_age_band(customer_age):
    """Replicate customer age band derivation from gold layer."""
    if customer_age is None:
        return None
    if customer_age < 25:
        return "Under 25"
    if customer_age < 35:
        return "25-34"
    if customer_age < 45:
        return "35-44"
    if customer_age < 55:
        return "45-54"
    return "55+"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestBoolEncode:
    def test_yes_returns_1(self):
        assert bool_encode("Yes") == 1

    def test_no_returns_0(self):
        assert bool_encode("No") == 0

    def test_case_insensitive(self):
        assert bool_encode("YES") == 1
        assert bool_encode("no") == 0
        assert bool_encode("yEs") == 1

    def test_whitespace_trimmed(self):
        assert bool_encode("  Yes  ") == 1
        assert bool_encode(" No ") == 0

    def test_invalid_returns_none(self):
        assert bool_encode("Maybe") is None
        assert bool_encode("1") is None
        assert bool_encode("") is None

    def test_none_returns_none(self):
        assert bool_encode(None) is None


class TestStandardiseText:
    def test_basic_upper(self):
        assert standardise_text("petrol") == "PETROL"

    def test_trims_whitespace(self):
        assert standardise_text("  diesel  ") == "DIESEL"

    def test_already_upper(self):
        assert standardise_text("CNG") == "CNG"

    def test_none_returns_none(self):
        assert standardise_text(None) is None


class TestClaimStatus:
    def test_valid_values(self):
        assert is_valid_claim_status(0) is True
        assert is_valid_claim_status(1) is True

    def test_invalid_values(self):
        assert is_valid_claim_status(2) is False
        assert is_valid_claim_status(-1) is False
        assert is_valid_claim_status(None) is False

    def test_null_fill(self):
        assert fill_null_claim_status(None) == 0
        assert fill_null_claim_status(1) == 1
        assert fill_null_claim_status(0) == 0


class TestRangeFilters:
    """Uses the same thresholds defined in config.py."""

    def test_customer_age_valid(self):
        assert is_within_range(25, 18, 100) is True
        assert is_within_range(18, 18, 100) is True   # boundary
        assert is_within_range(100, 18, 100) is True   # boundary

    def test_customer_age_invalid(self):
        assert is_within_range(17, 18, 100) is False
        assert is_within_range(101, 18, 100) is False

    def test_vehicle_age_valid(self):
        assert is_within_range(0, 0, 25) is True
        assert is_within_range(25, 0, 25) is True

    def test_vehicle_age_invalid(self):
        assert is_within_range(-1, 0, 25) is False
        assert is_within_range(26, 0, 25) is False

    def test_ncap_rating_valid(self):
        assert is_within_range(0, 0, 5) is True
        assert is_within_range(5, 0, 5) is True

    def test_ncap_rating_invalid(self):
        assert is_within_range(-1, 0, 5) is False
        assert is_within_range(6, 0, 5) is False

    def test_none_is_invalid(self):
        assert is_within_range(None, 0, 100) is False


class TestVehicleAgeBand:
    @pytest.mark.parametrize("age, expected", [
        (0, "0-1"),
        (1, "0-1"),
        (2, "2-3"),
        (3, "2-3"),
        (4, "4-5"),
        (5, "4-5"),
        (6, "5+"),
        (10, "5+"),
    ])
    def test_bands(self, age, expected):
        assert assign_vehicle_age_band(age) == expected

    def test_none(self):
        assert assign_vehicle_age_band(None) is None


class TestCustomerAgeBand:
    @pytest.mark.parametrize("age, expected", [
        (18, "Under 25"),
        (24, "Under 25"),
        (25, "25-34"),
        (34, "25-34"),
        (35, "35-44"),
        (44, "35-44"),
        (45, "45-54"),
        (54, "45-54"),
        (55, "55+"),
        (80, "55+"),
    ])
    def test_bands(self, age, expected):
        assert assign_customer_age_band(age) == expected

    def test_none(self):
        assert assign_customer_age_band(None) is None
