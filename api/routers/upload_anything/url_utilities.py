import json
import os
import subprocess

import requests
from fastapi import FastAPI, HTTPException, status

from api.config import DB_HOST, DB_NAME, DB_PASSWORD, DB_USER
from api.routers.upload_anything.utilities import (
    clean_string,
    upload_csv_file,
    upload_geographic_file,
)


def upload_arcgis_service(url: str, service_name: str):
    subprocess.call(
        f"""ogr2ogr -f "PostgreSQL" PG:"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST}" \
        "{url}/query?where=1=1&outfields=*&f=geojson" -nln {service_name} -lco FID=gid -lco GEOMETRY_NAME=geom  -overwrite""",
        shell=True,
    )


def download_arcgis_service_information(url: str):
    """
    Downloads an ArcGIS service using the ArcGIS REST API and then uploads each layer
    in the service to the server.

    Args:
        url (str): The URL of the ArcGIS service to be downloaded.

    Raises:
        HTTPException: If there is an error downloading the service.
    """
    response = requests.get(f"{url}?f=pjson")

    if response.status_code != 200 or "error" in response.json():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="There was an error downloading the service",
        )

    data = response.json()

    if "layers" in data:
        if url[-1] != "/":
            url += "/"
        results = []
        for layer in data["layers"]:
            layer_response = requests.get(f"{url}{layer['id']}?f=json")

            if layer_response.status_code != 200 or "error" in layer_response.json():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="There was an error downloading the service",
                )

            layer_information = layer_response.json()

            if "Query" in layer_information["capabilities"]:
                upload_arcgis_service(
                    url=f"{url}{layer_information['id']}",
                    service_name=layer_information["name"].lower().replace(" ", "_"),
                )

                results.append(
                    {
                        "status": True,
                        "table_name": layer_information["name"]
                        .lower()
                        .replace(" ", "_"),
                    }
                )

        return results

    else:
        if "Query" in data["capabilities"]:
            upload_arcgis_service(
                url=url, service_name=data["name"].lower().replace(" ", "_")
            )

        return [{"status": True, "table_name": data["name"].lower().replace(" ", "_")}]


def upload_google_sheets(url: str, app: FastAPI):
    """
    Downloads a Google Sheets spreadsheet and imports it into a PostgreSQL database.

    The file is downloaded using the requests library and imported into a PostgreSQL database using the psycopg2 library.

    Args:
        url (str): The URL of the Google Sheets spreadsheet to be imported.
        app (FastAPI): The FastAPI application object.
    """
    # TODO support multiple sheets
    google_doc_id = url.split("d/")[1].split("/")[0]
    url = f"https://docs.google.com/spreadsheets/d/{google_doc_id}/export?format=csv&gid=0"
    response = requests.get(url)

    if response.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="There was an error downloading the spreadsheet",
        )

    file_name = (
        response.headers.get("content-disposition")
        .split("filename=")[1]
        .split(";")[0]
        .replace('"', "")
        .replace(".csv", "")
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
    )

    with open(f"{os.getcwd()}/media/{google_doc_id}.csv", "wb") as f:
        f.write(response.content)

    upload_csv_file(
        write_file_path=f"{os.getcwd()}/media/{google_doc_id}.csv",
        file_name=file_name,
        app=app,
    )

    os.remove(f"{os.getcwd()}/media/{google_doc_id}.csv")

    return [{"status": True, "table_name": file_name}]


def upload_ogc_api_feature_collection(url: str):
    """
    Downloads a OGC API feature collection and imports it into a PostgreSQL database.

    The collection is downloaded using the requests library and imported into a PostgreSQL database using the psycopg2 library.

    Args:
        url (str): The URL of the OGC API feature collection to be imported.
    """
    collection_id = url.split("collections/")[1].split("/")[0]
    response = requests.get(f"{url}/items")

    if response.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="There was an error downloading the feature collection",
        )

    feature_collection = response.json()

    numberMatched = feature_collection["numberMatched"]
    numberGenerated = feature_collection["numberReturned"]

    while numberMatched > numberGenerated:
        response = requests.get(f"{url}/items?offset={numberGenerated}")
        feature_collection["features"].extend(response.json()["features"])
        numberGenerated += response.json()["numberReturned"]

    with open(f"{os.getcwd()}/media/{collection_id}.geojson", "w") as f:
        json.dump(feature_collection, f)

    upload_geographic_file(
        file_path=f"{os.getcwd()}/media/{collection_id}.geojson",
        table_name=collection_id,
    )

    os.remove(f"{os.getcwd()}/media/{collection_id}.geojson")

    return [{"status": True, "table_name": collection_id}]


def upload_ogc_wfs(url: str):
    """
    Downloads an OGC WFS feature collection and imports it into a PostgreSQL database.

    This function retrieves features from an OGC WFS service in batches of 50,
    converting them into a GeoJSON file, which is then imported into a PostgreSQL
    database table. The table name is derived from the typeName parameter in the URL.

    Args:
        url (str): The URL of the OGC WFS service from which to download features.

    Raises:
        HTTPException: If there is an error downloading the WFS feature collection.
    """
    response = requests.get(f"{url}&maxFeatures=50&outputFormat=application%2Fjson")
    collection_id = clean_string(url.split("typeName=")[1].split("&")[0])

    if response.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="There was an error downloading the wfs",
        )

    if "ServiceException" in response.text:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="There was an error downloading the wfs",
        )

    feature_collection = response.json()
    total_features = 50
    more_features = True

    while more_features:
        response = requests.get(
            f"{url}&maxFeatures=50&startIndex={total_features}&outputFormat=application%2Fjson"
        )
        if response.json()["features"] == []:
            more_features = False
        feature_collection["features"].extend(response.json()["features"])
        total_features += 50

    with open(f"{os.getcwd()}/media/{collection_id}.geojson", "w") as f:
        json.dump(feature_collection, f)

    upload_geographic_file(
        file_path=f"{os.getcwd()}/media/{collection_id}.geojson",
        table_name=collection_id,
    )

    os.remove(f"{os.getcwd()}/media/{collection_id}.geojson")

    return [{"status": True, "table_name": collection_id}]


def download_data_from_url(url: str):
    """
    Downloads data from a URL and imports it into a PostgreSQL database.

    This function is a wrapper around the requests library and the
    upload_geographic_file function. It downloads a file from the given
    URL and then uploads it to the PostgreSQL database as a new table.

    Args:
        url (str): The URL of the file to be downloaded and imported.

    Raises:
        HTTPException: If there is an error downloading the file.
    """
    response = requests.get(url)

    if response.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="There was an error downloading the file",
        )

    with open(f"{os.getcwd()}/media/{url.split('/')[-1]}", "wb") as f:
        f.write(response.content)

    upload_geographic_file(
        file_path=f"{os.getcwd()}/media/{url.split('/')[-1]}",
        table_name=url.split("/")[-1].split(".")[0],
    )

    os.remove(f"{os.getcwd()}/media/{url.split('/')[-1]}")

    return [{"status": True, "table_name": url.split("/")[-1].split(".")[0]}]
