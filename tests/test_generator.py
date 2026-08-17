import sys
import os
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "data-generator"))

from faker_generator import weighted_choice


def test_weighted_choice_only_returns_known_options():
    options = {"a": 0.5, "b": 0.3, "c": 0.2}
    for _ in range(200):
        result = weighted_choice(options)
        assert result in options


def test_weighted_choice_respects_zero_weight():
    options = {"always": 1.0, "never": 0.0}
    results = [weighted_choice(options) for _ in range(200)]
    assert "never" not in results
    assert all(r == "always" for r in results)


def test_weighted_choice_roughly_matches_weights():
    options = {"common": 0.9, "rare": 0.1}
    results = Counter(weighted_choice(options) for _ in range(2000))
    assert results["common"] > results["rare"] * 4