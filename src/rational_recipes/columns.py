"""Column id translation. Provides translation of ingredient names to
column indexes."""

from collections.abc import Generator

from rational_recipes.errors import InvalidInputException
from rational_recipes.ingredient import Ingredient


class ColumnTranslator:
    """Convert column identities to indexes"""

    def __init__(self, ingredients: tuple[Ingredient, ...]) -> None:
        self.name_to_column_index: dict[str, list[int]] = {}
        self.column_index_to_columns: dict[int, list[int]] = {}
        self.map_ingredient_names(ingredients)

    def map_ingredient_names(self, ingredients: tuple[Ingredient, ...]) -> None:
        """Map ingredient names to column indexes"""
        for i in range(0, len(ingredients)):
            for name in ingredients[i].synonyms():
                column_indexes = self.name_to_column_index.get(name, [])
                column_indexes.append(i)
                self.name_to_column_index[name.lower()] = column_indexes

    def _id_to_indexes(
        self, column_identifier: str | int
    ) -> Generator[int, None, None]:
        """Normalize column identifier to one or more column indexes"""
        try:
            if isinstance(column_identifier, str):
                yield from self.name_to_column_index[column_identifier.lower()]
            else:
                yield column_identifier
        except KeyError as err:
            raise InvalidInputException(
                f"Missing column specified: '{column_identifier}'"
            ) from err

    def id_to_indexes(self, column_identifier: str | int) -> list[int]:
        """Normalize column identifier to one or more column indexes"""
        return list(self._id_to_indexes(column_identifier))
