"""Functions for adding and parsing command line options"""

from read import read_files
from normalize import to_grams
from merge import merge_columns
from ratio import calculate_ratio_and_stats
from errors import InvalidInputException

def get_ratio_and_stats(filenames, distinct, merge, desired_interval=0.05,
                        zero_columns=None):
    """Parse input files to produce mean recipe ratio and related statistics
    """
    files = [open(filename) for filename in filenames]
    ingredients, proportions = read_files(files)
    all_proportions = to_grams(ingredients, proportions)
    if distinct:
        all_proportions = set(all_proportions)
    ingredients, all_proportions = merge_columns(ingredients, all_proportions,
                                                 merge)
    stats, ratio = calculate_ratio_and_stats(ingredients, all_proportions,
                            desired_interval=desired_interval,
                            zero_columns=zero_columns)
    return ingredients, ratio, stats, len(all_proportions)

def get_ratio(filenames, distinct, merge, desired_interval=0.05):
    """Parse input files to produce mean recipe ratio
    """
    ingredients, ratio, _, _ = get_ratio_and_stats(filenames, distinct,
                                                           merge,
                                                           desired_interval)
    return ingredients, ratio

def add_merge_option(parser):
    """Add option used to specify column merge"""
    parser.add_option("-m", "--merge", type="string", dest="merge", 
        help="merge columns where MAPPING is <col>[.percent][+<col>[.percent]]"
        "[:<col>[.percent][+<col>[.percent]]...",
        default=None, metavar="MAPPING")
    
def add_include_option(parser):
    """Add option to choose whether duplicate input rows are removed"""
    parser.add_option("-i", "--include", action="store_false", dest="distinct",
                      default=True,
                      help="include duplicate proportions in ratio calculation")

def parse_column_merge(merge_option):
    """Parse specification of column merge"""
    merge = []
    if merge_option is not None:
        for mappings in merge_option.split(":"):
            mapping = []
            for column_spec in mappings.split("+"):
                column_spec = column_spec.split(".")
                column_id = column_spec[0]
                percentage = 1.0
                if len(column_spec) == 2:
                    if not column_spec[1].isdigit():
                        raise Exception("Expected percentage after period in"
                                        " merge specification")
                    percentage = float("0." + column_spec[1])
                if column_id.isdigit():
                    mapping.append((int(column_id), percentage))
                else:
                    mapping.append((column_id, percentage))
            merge.append(mapping)
    return merge

def parse_restrictions(options):
    """Parse specification of column merge"""
    column_options = []
    if options is not None and len(options) > 0:
        for mappings in options.split(","):
            column_spec = mappings.split("=")
            column_id = column_spec[0]
            if len(column_spec) != 2:
                raise InvalidInputException("Expected column option")
            weight = float(column_spec[1])
            if column_id.isdigit():
                column_options.append((int(column_id), weight))
            else:
                column_options.append((column_id, weight))
    return column_options