"""Utility class for formatting terminal output"""

class Output(object):
    """Simplifies formatting of terminal output"""
    
    def __init__(self):
        self.output = []
        
    def __str__(self):
        return "\n".join(self.output)
    
    def line(self, text=""):
        """Add a line of output text"""
        self.output.append(text)
            
    def title(self, text):
        """Print underlined text"""
        self.line(text)
        self.line("-" * len(text))