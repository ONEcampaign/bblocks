from typing import Type

from numpy import nan
import re


def clean_number(number: str, to: Type = float) -> float | int:
    """Clean a string and return as float or integer"""

    if not isinstance(number, str):
        number = str(number)

    number = re.sub(r"[^\d.]", "", number)

    if number == "":
        return nan

    if to == float:
        return float(number)

    if to == int:
        return int(round(float(number)))
