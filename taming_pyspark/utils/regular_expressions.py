"""
Utility functions related to regular expressions
"""
import re


def normalize_words(text: str):
    """
    Breaks up text based on words ('r/W+') with a unicode encoding
    :param text: string input variable
    :return:
    """
    return re.compile(pattern=r'/W+', flags=re.UNICODE).split(text.lower())
