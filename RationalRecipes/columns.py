"""Column id translation. Provides translation of ingredient names to 
column indexes."""
import types
from RationalRecipes.errors import InvalidInputException

class ColumnTranslator(object):
    """Convert column identities to indexes"""

    def __init__(self, ingredients):
        self.name_to_column_index = {}
        self.column_index_to_columns = {}
        self.map_ingredient_names(ingredients)
 
    def map_ingredient_names(self, ingredients):
        """Map ingredient names to column indexes"""
        for i in range(0, len(ingredients)):
            for name in ingredients[i].synonyms():
                column_indexes = self.name_to_column_index.get(name, [])
                column_indexes.append(i)
                self.name_to_column_index[name.lower()] = column_indexes

    def _id_to_indexes(self, column_identifier):
        """Normalize column identifier to one or more column indexes"""
        try:
            if type(column_identifier) == types.StringType:
                for column_index in \
                  self.name_to_column_index[column_identifier.lower()]:
                    yield column_index
            else:
                yield column_identifier
        except KeyError:
            raise InvalidInputException(
                "Missing column specified: '%s'" % column_identifier)

    def id_to_indexes(self, column_identifier):
        """Normalize column identifier to one or more column indexes"""
        return list(self._id_to_indexes(column_identifier))
