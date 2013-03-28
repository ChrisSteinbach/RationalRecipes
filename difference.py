"""Functions for calculating difference between two ratios"""

def diff(lhs, rhs, diff_func):
    """Return the mean percentage difference and the percentage difference
        for individual ingredient proportions between two ratios."""
    ingredients = lhs.ingredients
    lhs = lhs.values()
    rhs = rhs.values()
    differences = []
    for i in range(0, len(lhs)):
        difference = diff_func(lhs[i], rhs[i])
        differences.append((difference, ingredients[i]))
    return differences

def percentage_difference(lhs, rhs):
    """Return the mean percentage difference and the percentage difference
        for individual ingredient proportions between two ratios."""
    differences = diff(lhs, rhs, calc_percentage_difference)
    total = sum(difference for difference, _ in differences)
    mean_difference = total / len(lhs)
    return mean_difference, differences

def percentage_change(lhs, rhs):
    """Calculate percentage difference between two values. Used for comparing
       two different ratios. Percentage difference calculated this way can
       exceed 100%."""    
    return diff(lhs, rhs, calc_percentage_change)

def calc_percentage_change(src, dest):
    """Calculate percentage change from one value (src) to another (dest)"""
    return (dest - src) / src

def calc_percentage_difference(value1, value2):
    """Calculate percentage difference between two values. Used for comparing
       two different ratios. Percentage difference calculated this way can
       exceed 100%."""
    mean = (value1 + value2) / 2.0
    mean_diff = abs(value2 - value1)
    return mean_diff / mean

def percentage_difference_from_mean(value1, value2):
    """Calculate percentage difference between two values. Used to help make
       confidence interval sizes more intuitive by keeping the percentage
       difference under 100%"""
    mean = (value1 + value2) / 2.0
    mean_diff = abs(mean - value1)
    return mean_diff / mean
