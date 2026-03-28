"""Utility class for formatting terminal output"""


class Output:
    """Simplifies formatting of terminal output"""

    def __init__(self) -> None:
        self.output: list[str] = []

    def __str__(self) -> str:
        return "\n".join(self.output)

    def line(self, text: str = "") -> None:
        """Add a line of output text"""
        self.output.append(text)

    def title(self, text: str) -> None:
        """Print underlined text"""
        self.line(text)
        self.line("-" * len(text))
