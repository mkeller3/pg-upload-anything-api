"""
Models
"""

from pydantic import BaseModel


class HealthCheckResponse(BaseModel):
    """HealthCheckResponse"""

    status: str


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
