import csv
import mimetypes
import os
import shutil
import zipfile
from typing import List

import aiofiles
import openpyxl
from fastapi import APIRouter, File, HTTPException, Request, UploadFile, status

from api.config import DEFAULT_CHUNK_SIZE
from api.routers.upload_anything.upload_models import (
    ResponseModel,
    uploadUrlRequestModel,
)
from api.routers.upload_anything.url_utilities import (
    download_arcgis_service_information,
    download_data_from_url,
    upload_google_sheets,
    upload_ogc_api_feature_collection,
    upload_ogc_wfs,
)
from api.routers.upload_anything.utilities import (
    upload_csv_file,
    upload_geographic_file,
)

router = APIRouter()


@router.post(path="/upload_file", response_model=List[ResponseModel])
async def upload_file(request: Request, file: UploadFile = File(...)):
    """
    Upload a file to the server and import it into a PostgreSQL database.

    The file can be in any of the following formats:
    - CSV
    - Excel (xlsx)
    - GeoPackage
    - GeoJSON
    - GML
    - GPX
    - KML
    - SQLite
    - Shapefile
    - ZIP (containing one of the above formats)
    """

    valid_file_type = False

    valid_file_types = [
        "text/csv",
        "application/vnd.ms-excel",
        "application/geopackage+sqlite3",
        "application/geo+json",
        "application/gml+xml",
        "application/gpx+xml",
        "application/vnd.google-earth.kml+xml",
        "application/vnd.sqlite3",
        "application/vnd.shp",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/zip",
    ]

    if file.content_type in valid_file_types:
        valid_file_type = True

    if valid_file_type is False:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Please upload a valid file type. {" ,".join(valid_file_types)}',
        )

    file_name = str(file.filename)

    write_file_path = f"{os.getcwd()}/media/{file.filename}"

    try:
        if os.path.exists(f"{os.getcwd()}/media/") is False:
            os.mkdir(f"{os.getcwd()}/media/")
        async with aiofiles.open(write_file_path, "wb") as new_file:
            while chunk := await file.read(DEFAULT_CHUNK_SIZE):
                await new_file.write(chunk)
    except Exception as e:
        if file.filename:
            media_directory = os.listdir(
                f"{os.getcwd()}/media/{file.filename.split('.')[0]}"
            )
            for uploaded_file in media_directory:
                if file_name in uploaded_file:
                    os.remove(
                        f"{os.getcwd()}/media/{file.filename.split('.')[0]}/{uploaded_file}"
                    )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="There was an error uploading the file. Error: " + str(e),
        )

    if (
        file.content_type
        == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    ):
        new_file_name = file_name.split(".")[0]

        os.makedirs(f"{os.getcwd()}/media/{new_file_name}")
        workbook = openpyxl.load_workbook(f"{os.getcwd()}/media/{new_file_name}.xlsx")
        results = []
        for sheet in workbook:
            worksheet = workbook[sheet.title]
            with open(
                f"{os.getcwd()}/media/{new_file_name}/{sheet.title}.csv",
                "w",
                newline="",
            ) as csvfile:
                csvwriter = csv.writer(csvfile)
                for row in worksheet.iter_rows(values_only=True):
                    csvwriter.writerow(row)
            result = upload_csv_file(
                write_file_path=f"{os.getcwd()}/media/{new_file_name}/{sheet.title}.csv",
                file_name=sheet.title,
                app=request.app,
            )
            results.append(result)

        shutil.rmtree(f"{os.getcwd()}/media/{new_file_name}")
        media_directory = os.listdir(f"{os.getcwd()}/media/")
        for uploaded_file in media_directory:
            if file_name in uploaded_file:
                os.remove(f"{os.getcwd()}/media/{uploaded_file}")
    elif file.content_type == "text/csv":
        result = upload_csv_file(
            write_file_path=write_file_path,
            file_name=file_name,
            app=request.app,
        )

        if result["status"] is False:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result["message"],
            )

        media_directory = os.listdir(f"{os.getcwd()}/media/")
        for uploaded_file in media_directory:
            if file_name in uploaded_file:
                os.remove(f"{os.getcwd()}/media/{uploaded_file}")

        results = [result]
    else:
        new_file_name = file_name.split(".")[0]
        valid_file_type = False
        valid_file_extension = ""
        if file.content_type == "application/zip":
            with zipfile.ZipFile(write_file_path, "r") as zip_ref:
                zip_ref.extractall(f"{os.getcwd()}/media/{new_file_name}")
            media_directory = os.listdir(f"{os.getcwd()}/media/{new_file_name}")
            file_extension = media_directory[0].split(".")[-1]
            for uploaded_file in media_directory:
                file_path = f"{media_directory}/{uploaded_file}"
                mime_type, _ = mimetypes.guess_type(file_path)
                file_extension = file_path.split(".")[-1]
                if mime_type in valid_file_types:
                    valid_file_type = True
                    valid_file_extension = file_path.split(".")[-1]
                if file_extension.lower() in ["gdb", "tab", "shp"]:
                    valid_file_extension = file_path.split(".")[-1]
                    valid_file_type = True
        else:
            media_directory = os.listdir(f"{os.getcwd()}/media")
            for uploaded_file in media_directory:
                file_path = f"{media_directory}/{uploaded_file}"
                mime_type, _ = mimetypes.guess_type(file_path)
                file_extension = file_path.split(".")[-1]
                if mime_type in valid_file_types:
                    valid_file_type = True
                    valid_file_extension = file_path.split(".")[-1]

        if valid_file_type is False:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Please upload a valid file type within your zip file. {" ,".join(valid_file_types)}',
            )

        if file_extension.lower() == "csv":
            result = upload_csv_file(
                write_file_path=f"{os.getcwd()}/media/{new_file_name}/{new_file_name}.{file_extension}",
                file_name=file_name.split(".")[0],
                app=request.app,
            )

            if result["status"] is False:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=result["message"],
                )

            results = [result]

        elif file_extension.lower() == "xlsx":
            new_file_name = file_name.split(".")[0]
            os.makedirs(f"{os.getcwd()}/media/{new_file_name}/{new_file_name}")
            workbook = openpyxl.load_workbook(
                f"{os.getcwd()}/media/{new_file_name}/{new_file_name}.xlsx"
            )
            results = []
            for sheet in workbook:
                worksheet = workbook[sheet.title]
                with open(
                    f"{os.getcwd()}/media/{new_file_name}/{new_file_name}/{sheet.title}.csv",
                    "w",
                    newline="",
                ) as csvfile:
                    csvwriter = csv.writer(csvfile)
                    for row in worksheet.iter_rows(values_only=True):
                        csvwriter.writerow(row)
                result = upload_csv_file(
                    write_file_path=f"{os.getcwd()}/media/{new_file_name}/{new_file_name}/{sheet.title}.csv",
                    file_name=sheet.title,
                    app=request.app,
                )
                results.append(result)
            shutil.rmtree(f"{os.getcwd()}/media/{new_file_name}")
            media_directory = os.listdir(f"{os.getcwd()}/media/")
            for uploaded_file in media_directory:
                if file_name in uploaded_file:
                    os.remove(f"{os.getcwd()}/media/{uploaded_file}")
        else:
            results = upload_geographic_file(
                file_path=f"{os.getcwd()}/media/{new_file_name}.{valid_file_extension}",
                table_name=file_name.split(".")[0],
                app=request.app,
            )
        media_directory = os.listdir(f"{os.getcwd()}/media/")
        for uploaded_file in media_directory:
            if file_name in uploaded_file:
                os.remove(f"{os.getcwd()}/media/{uploaded_file}")
        if os.path.exists(f"{os.getcwd()}/media/{new_file_name}"):
            shutil.rmtree(f"{os.getcwd()}/media/{new_file_name}")

    return results


@router.post(path="/upload_url", response_model=List[ResponseModel])
async def upload_url(
    request: Request,
    info: uploadUrlRequestModel,
):
    """
    Upload a data to the server and import it into a PostgreSQL database.

    Google Sheets
    ESRI Feature Service
    ESRI Map Service
    OGC WFS
    OGC API - Features
    Flat Files
    """

    if "arcgis" in info.url.lower():
        results = download_arcgis_service_information(url=info.url, app=request.app)

    elif "docs.google.com/spreadsheets" in info.url.lower():
        results = upload_google_sheets(url=info.url, app=request.app)

    elif "collection" in info.url.lower():
        results = upload_ogc_api_feature_collection(url=info.url, app=request.app)

    elif "service=wfs" in info.url.lower():
        results = upload_ogc_wfs(url=info.url, app=request.app)

    else:
        results = download_data_from_url(url=info.url, app=request.app)

    return results
