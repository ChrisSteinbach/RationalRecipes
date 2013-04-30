"""Unit tests for output utility class"""
import unittest
from RationalRecipes.output import Output

class TestOutput(unittest.TestCase):
    """Output unit tests"""
    
    def test_no_output(self):
        """Check that no output produces an empty string"""
        self.assertEquals(str(Output()), "")
        
    def test_single_line(self):
        """Check that a single line of output produces
           a string with a single line and no line breaks"""
        output = Output()
        output.line("test")
        self.assertEquals(str(output), "test")
        
    def test_multiline(self):
        """Check that multiple lines of output produce
           correctly formatted string"""
        output = Output()
        output.line("test1")
        output.line("test2")
        self.assertEquals(str(output), "test1\ntest2")
        
    def test_add_empty_line(self):
        """Check that adding a line produces a line break in the output
           string"""
        output = Output()
        output.line()
        output.line("test")
        self.assertEquals(str(output), "\ntest")

    def test_empty_line_interspersed(self):
        """Check that multiple lines of output with empty lines
           interspersed produce correctly formatted string"""
        output = Output()
        output.line("test1")
        output.line()
        output.line("test2")
        self.assertEquals(str(output), "test1\n\ntest2")
        
    def test_title(self):
        """Check that a title is written and underlined with the
           same number of underline characters as text to underline"""
        output = Output()
        output.title("a")
        self.assertEquals(str(output), "a\n-")
        
    def test_title_in_context(self):
        """Check that a title is formatted correctly when surrounded
           by other text"""
        output = Output()
        output.line("test1")
        output.title("ab")
        output.line("test2")
        self.assertEquals(str(output), "test1\nab\n--\ntest2")
        
