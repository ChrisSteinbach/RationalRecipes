"""Merge ingredient measures.

Sometimes it's interesting to analyze ingredient ratios with alternative
ingredients, either because,

 - It is difficult to identify a 'core' set of ingredients
 - A particular substitution occurs regularly
 - We want to learn how often a substitution is made (perhaps with a view to
   mixing ingredients, e.g. water and milk in pancake batter)
   
When we do this we might want to combine column data so that, say all liquid
ingredient appears as one column. Or we may want the fat content to be combined
with one column and the remainder with another.

Equally, when a single ingredient appears multiple times in a recipe it is
useful to combine the columns so that we can measure out the total amount of
that ingredient at the start of cooking.

This module allows column merging for these situations.
"""
import types

class Merge(object):
    """Responsible for applying a column merge specification to return a new
       set of merged columns.
    """
    
    def _map_ingredient_names(self, ingredients):
        """Map ingredient names to column indexes"""
        for i in range(0, len(ingredients)):
            for name in ingredients[i].synonyms():
                column_indexes = self.name_to_column_index.get(name, [])
                column_indexes.append(i)
                self.name_to_column_index[name.lower()] = column_indexes

    def _column_id_to_indexes(self, column_identifier):
        """Normalize column identifier to a column index"""
        if type(column_identifier) == types.StringType:
            for column_index in \
              self.name_to_column_index[column_identifier.lower()]:
                yield column_index
        else:
            yield column_identifier
            
    def _convert_spec_to_indexes(self, merge_specification, ingredients):
        """Normalize merge specification so that it only uses column indexes
           and not mixed indexes and ingredient names."""
        self._map_ingredient_names(ingredients)
        new_merge_specification = []
        for combine_spec in merge_specification:
            new_combine_spec = []
            for column_identifier, percentage in combine_spec:
                for column_index in \
                        self._column_id_to_indexes(column_identifier):
                    new_combine_spec.append((column_index, percentage))
            new_merge_specification.append(new_combine_spec)
        return new_merge_specification
            

    def map_column_indexes(self, merge_specification, ingredients):
        """Map column indexes to combination of columns to merge. For
           columns that will be removed, the column index maps to None. Column
           combinations are a list of tuples where each tuple has two elements:
           the column index to merge followed by the percentage of that column's
           value to add."""
        last_column = len(ingredients) - 1
        accumulating = {}
        remove = set()
        # default behavior, no column merge
        for column_index in range(0, last_column + 1):
            self.column_index_to_columns[column_index] \
                = [(column_index, 1.0)]
        
        for columns in merge_specification:
            accumulating_column = columns[0][0]
            if accumulating_column > last_column or accumulating_column < 0:
                raise Exception("Attempted to merge missing column %d" % \
                                accumulating_column)
            # specifies which columns should be merged into this one
            accumulating[accumulating_column] = columns
            for column_index, _percentage in columns[1:]:
                column_index = column_index
                if column_index > last_column or column_index < 0:
                    raise Exception("Attempted to merge missing column %d" \
                                    % column_index) 
                # drop this column; it will be merged into another
                remove.add(column_index)
        
        # drop columns first so that any columns both specified as
        # accumulating *and* merged columns do not get dropped
        for column_index in remove:
            self.column_index_to_columns[column_index] = None
        
        for column_index, columns in accumulating.items():
            self.column_index_to_columns[column_index] = columns

    def __init__(self, merge_specification, ingredients):
        self.name_to_column_index = {}
        self.column_index_to_columns = {}
        merge_specification = self._convert_spec_to_indexes(merge_specification,
                                                            ingredients)
        self.map_column_indexes(merge_specification, ingredients)

    def merge_one_row(self, row, combine):
        """Yield a new row by combining columns"""
        for index in range(0, len(row)):
            columns_to_combine = self.column_index_to_columns[index]
            if columns_to_combine is not None:
                yield combine(row, columns_to_combine)

    def merge_rows(self, rows):
        """Merge all rows of measurements"""
        for row in rows:
            yield tuple(self.merge_one_row(row, combine_measurements))
            
    def merge_ingredients(self, ingredients):
        """Merge ingredients"""
        return tuple(self.merge_one_row(ingredients, combine_ingredients))

def combine_measurements(row, columns_to_combine):
    """Combine columns in a row of measurements in grams according to
       specification."""
    return sum(row[column_index] * percentage for column_index,
                percentage in columns_to_combine)

def combine_ingredients(ingredients, columns_to_combine):
    """Combine columns in a row of ingredients according to specification."""
    return ingredients[columns_to_combine[0][0]]
    
           
def merge_columns(ingredients, rows, merge=None):
    """Merge columns of input data according to specification."""
    if merge is None or len(merge) == 0:
        return ingredients, rows
    merge = Merge(merge, ingredients)
    new_rows = list(merge.merge_rows(rows))
    new_ingredients = merge.merge_ingredients(ingredients)
    return new_ingredients, new_rows

