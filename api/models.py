"""
Models
"""

from pydantic import BaseModel


class GeographyField(BaseModel):
    """
    Geography field model
    """

    potential_names: list[str]


class Geography(BaseModel):
    """
    Geography model
    """

    name: str
    fields: dict[str, GeographyField]
    rank: int
