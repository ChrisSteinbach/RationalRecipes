"""Unit tests for rational_recipes.units.

Refactor context: ``Factory`` was a class-state singleton whose ``_UNITS`` dict
was mutated as a side effect of every ``Unit.__init__``. That made the registry
untestable — you couldn't construct a unit without polluting the singleton, and
you couldn't observe registration/conflict/lookup behavior in isolation.

The refactor (this bead, RationalRecipes-ciu) makes ``Factory`` an instance
class with a per-instance ``_units`` dict; ``Unit.__init__`` no longer
auto-registers. A module-level default ``Factory`` instance holds the built-in
units (GRAM, CUP, etc.), and ``Factory.get_by_name(name)`` is a classmethod that
delegates to it so existing call sites keep working unchanged. Tests in this
file construct fresh ``Factory()`` instances to exercise registry behavior
without touching the default registry.
"""

from __future__ import annotations

import pytest

from rational_recipes.units import (
    GRAM,
    KG,
    BadUnitException,
    Factory,
    VolumeUnit,
    WeightUnit,
    WholeUnit,
)


class _FakeIngredient:
    """Minimal stand-in for Ingredient.

    Units only depend on three Ingredient methods: ``milliliters2grams``,
    ``wholeunits2grams``, and ``name``. A handwritten fake keeps these tests
    free of the ingredients.db dependency that pulling in the real Ingredient
    class would impose.
    """

    def __init__(
        self,
        density: float = 1.0,
        wholeunit_weights: dict[str, float] | None = None,
        name: str = "fake",
    ) -> None:
        self._density = density
        self._wholeunit_weights = wholeunit_weights or {}
        self._name = name

    def milliliters2grams(self, milliliters: float) -> float:
        return milliliters * self._density

    def wholeunits2grams(self, size: str) -> float | None:
        return self._wholeunit_weights.get(size)

    def name(self) -> str:
        return self._name


class TestFactoryRegistration:
    """Factory builds an isolated registry per instance."""

    def test_fresh_factory_starts_empty(self) -> None:
        factory = Factory()
        assert factory.lookup("anything") is None

    def test_register_adds_all_synonyms(self) -> None:
        factory = Factory()
        unit = WeightUnit(["foo", "foos", "f"], 1)

        factory.register(unit)

        assert factory.lookup("foo") is unit
        assert factory.lookup("foos") is unit
        assert factory.lookup("f") is unit

    def test_register_lowercases_synonyms(self) -> None:
        """Mixed-case synonyms register under their lowercase form."""
        factory = Factory()
        unit = WholeUnit(["XL"])

        factory.register(unit)

        assert factory.lookup("xl") is unit

    def test_register_strips_synonyms(self) -> None:
        """Whitespace around synonym names is stripped at registration."""
        factory = Factory()
        unit = WeightUnit(["  spaced  "], 1)

        factory.register(unit)

        assert factory.lookup("spaced") is unit

    def test_lookup_is_case_insensitive(self) -> None:
        factory = Factory()
        unit = WeightUnit(["gram"], 1)
        factory.register(unit)

        assert factory.lookup("GRAM") is unit
        assert factory.lookup("Gram") is unit

    def test_lookup_returns_none_for_unknown(self) -> None:
        factory = Factory()
        factory.register(WeightUnit(["gram"], 1))

        assert factory.lookup("ounce") is None

    def test_later_registration_shadows_earlier_for_same_name(self) -> None:
        """Last write wins — registering a second unit under an existing name
        replaces the first. This documents the current behavior; if it's ever
        meant to raise instead, this test is the place to flip."""
        factory = Factory()
        first = WeightUnit(["gram"], 1)
        second = WeightUnit(["gram"], 999)

        factory.register(first)
        factory.register(second)

        assert factory.lookup("gram") is second

    def test_separate_factories_have_independent_registries(self) -> None:
        """The whole point of the refactor: tests can register units without
        leaking into other tests' registries."""
        a = Factory()
        b = Factory()
        unit = WeightUnit(["only_in_a"], 1)

        a.register(unit)

        assert a.lookup("only_in_a") is unit
        assert b.lookup("only_in_a") is None


class TestDefaultRegistry:
    """The module-level default Factory is populated at import time and is
    what production callers reach via the ``Factory.get_by_name`` classmethod."""

    def test_get_by_name_finds_builtin_weight_unit(self) -> None:
        assert Factory.get_by_name("gram") is GRAM

    def test_get_by_name_finds_builtin_via_synonym(self) -> None:
        assert Factory.get_by_name("kilograms") is KG

    def test_get_by_name_is_case_insensitive(self) -> None:
        assert Factory.get_by_name("GRAM") is GRAM

    def test_get_by_name_returns_none_for_unknown(self) -> None:
        assert Factory.get_by_name("not-a-real-unit") is None

    def test_unit_construction_does_not_register_in_default(self) -> None:
        """Constructing a Unit has no side effect on the default registry —
        that's the property the refactor exists to enforce."""
        WeightUnit(["totally_made_up_unit_name"], 1)

        assert Factory.get_by_name("totally_made_up_unit_name") is None


class TestUnitConversion:
    """Each Unit subclass converts a (value, ingredient) pair to grams."""

    def test_weight_unit_multiplies_by_conversion_factor(self) -> None:
        kg = WeightUnit(["kg"], 1000)
        ingredient = _FakeIngredient()

        assert kg.norm(2.5, ingredient) == pytest.approx(2500.0)

    def test_volume_unit_multiplies_then_routes_through_ingredient_density(
        self,
    ) -> None:
        """1 cup × 240 ml/cup × 0.5 g/ml = 120 g"""
        cup = VolumeUnit(["cup"], 240.0)
        ingredient = _FakeIngredient(density=0.5)

        assert cup.norm(1.0, ingredient) == pytest.approx(120.0)

    def test_whole_unit_multiplies_value_by_size_specific_weight(self) -> None:
        large = WholeUnit(["LARGE"])
        ingredient = _FakeIngredient(wholeunit_weights={"LARGE": 50.0})

        assert large.norm(3, ingredient) == pytest.approx(150.0)

    def test_whole_unit_raises_when_ingredient_has_no_weight_for_size(self) -> None:
        """Non-applicable size (e.g. "stick" of an ingredient that doesn't
        have stick weights) is the user-error path: WholeUnit.norm signals
        with BadUnitException so the caller can attach line context."""
        stick = WholeUnit(["stick"])
        ingredient = _FakeIngredient(wholeunit_weights={}, name="flour")

        with pytest.raises(BadUnitException) as exc_info:
            stick.norm(1, ingredient, line_nr=42)

        # The exception message should carry enough context for a human to
        # locate the offending line in the source recipe.
        message = str(exc_info.value)
        assert "stick" in message
        assert "flour" in message
        assert "42" in message


class TestUnitSynonyms:
    """Unit.synonyms exposes the list of names registered for a unit — used
    by Factory.register to populate the lookup table."""

    def test_synonyms_returns_constructor_names(self) -> None:
        unit = WeightUnit(["a", "b", "c"], 1)

        assert unit.synonyms() == ["a", "b", "c"]
