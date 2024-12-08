from pydantic import BaseModel


class uploadUrlRequestModel(BaseModel):
    """Request model for the upload_url endpoint"""

    url: str


class ResponseModel(BaseModel):
    """Response model"""

    status: bool
    table_name: str
