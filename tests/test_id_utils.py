"""Tests for forcehound.utils.id_utils."""

import pytest
from forcehound.utils.id_utils import ensure_18_char_id, generate_hash_id


class TestEnsure18CharId:
    def test_none_returns_none(self):
        assert ensure_18_char_id(None) is None

    def test_18_char_passthrough(self):
        sf_id = "005XX0000001ATEAAA"
        assert len(sf_id) == 18
        assert ensure_18_char_id(sf_id) == sf_id

    def test_15_to_18_conversion(self):
        # Well-known conversion: 005000000000001 → 005000000000001AAA
        result = ensure_18_char_id("005000000000001")
        assert len(result) == 18

    def test_15_to_18_all_lowercase(self):
        result = ensure_18_char_id("005xx0000001ate")
        assert len(result) == 18
        # All lowercase → suffix should be "AAA" (all zero flags)
        assert result.endswith("AAA")

    def test_15_to_18_all_uppercase(self):
        result = ensure_18_char_id("ABCDEABCDEABCDE")
        assert len(result) == 18
        # All uppercase → each 5-char chunk has all flags set (0b11111 = 31 → '5')
        assert result.endswith("555")

    def test_15_to_18_mixed_case(self):
        # Standard Salesforce ID with mixed case
        sf_15 = "005B0000003g1AB"
        result = ensure_18_char_id(sf_15)
        assert len(result) == 18
        assert result[:15] == sf_15

    def test_invalid_length_raises(self):
        with pytest.raises(ValueError, match="15 or 18"):
            ensure_18_char_id("005XX")

    def test_invalid_length_12(self):
        with pytest.raises(ValueError):
            ensure_18_char_id("123456789012")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            ensure_18_char_id("")

    def test_idempotent(self):
        """Converting a 15-char ID to 18, then passing the 18-char back, should be stable."""
        sf_15 = "005B0000003g1AB"
        sf_18 = ensure_18_char_id(sf_15)
        assert ensure_18_char_id(sf_18) == sf_18

    def test_known_conversion(self):
        """Test against a known SF ID conversion."""
        # 00DgL000004Vrwb (15) → 00DgL000004VrwbUAC (18)
        result = ensure_18_char_id("00DgL000004Vrwb")
        assert result == "00DgL000004VrwbUAC"


class TestGenerateHashId:
    def test_basic(self):
        result = generate_hash_id("SF_Object", "Account")
        assert len(result) == 18
        assert (
            result.isupper()
            or result.replace("0", "")
            .replace("1", "")
            .replace("2", "")
            .replace("3", "")
            .replace("4", "")
            .replace("5", "")
            .replace("6", "")
            .replace("7", "")
            .replace("8", "")
            .replace("9", "")
            .isalpha()
        )

    def test_deterministic(self):
        r1 = generate_hash_id("SF_Object", "Account")
        r2 = generate_hash_id("SF_Object", "Account")
        assert r1 == r2

    def test_known_value(self):
        """Known hash from project memory."""
        result = generate_hash_id("SF_Object", "Account")
        assert result == "E38EBDF9CB2964E90D"

    def test_different_kind_different_id(self):
        r1 = generate_hash_id("SF_Object", "Account")
        r2 = generate_hash_id("SF_User", "Account")
        assert r1 != r2

    def test_different_identifier_different_id(self):
        r1 = generate_hash_id("SF_Object", "Account")
        r2 = generate_hash_id("SF_Object", "Contact")
        assert r1 != r2

    def test_length(self):
        result = generate_hash_id("test", "value")
        assert len(result) == 18

    def test_uppercase(self):
        result = generate_hash_id("test", "value")
        assert result == result.upper()

    def test_hex_chars_only(self):
        result = generate_hash_id("test", "value")
        assert all(c in "0123456789ABCDEF" for c in result)
