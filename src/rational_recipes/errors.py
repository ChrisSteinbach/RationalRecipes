"""Base exception classes"""

class RationalRecipeException(Exception):
    """Base class for all rational recipe exceptions"""
    pass

class InvalidArgumentException(RationalRecipeException):
    """Base class for exceptions generated from invalid configuration or
       program arguments."""
    pass 

class InvalidInputException(RationalRecipeException):
    """Base class for handling of invalid input."""
    pass