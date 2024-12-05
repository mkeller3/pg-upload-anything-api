import csv
import json
import os
import subprocess

import geojson
import psycopg2
from fastapi import FastAPI
from shapely import wkb, wkt

from api.config import DB_HOST, DB_NAME, DB_PASSWORD, DB_USER


def clean_string(string: str):
    """
    Clean a string by removing spaces, dashes, periods, and colons, and
    converting to lowercase. This is used to clean strings that are used
    as database table names.

    Args:
        string (str): The string to be cleaned.

    Returns:
        str: The cleaned string.
    """
    return (
        string.replace(" ", "_")
        .replace("-", "_")
        .replace(".", "")
        .replace(":", "_")
        .lower()
    )


def find_matching_geographies(column_names: list, app: FastAPI):
    """
    Finds and returns a list of geographies that match the provided column names.

    Iterates through the predefined `GEOGRAPHIES` list, checking if the given
    column names match the potential names for each field in a geography. A geography
    is considered a match if all its fields have at least one column name that matches
    one of their potential names.

    Args:
        column_names (list): A list of column names to match against the potential names of geography fields.
        app (FastAPI): The FastAPI application instance.

    Returns:
        list: A list of geographies that have fields matching the provided column names. Each geography
              includes a `field_matches` dictionary indicating which column names matched each field.
    """
    matching_geographies = []

    for geography in app.state.geographies:
        valid_match = []
        field_matches = {}
        for column in geography["fields"]:
            valid_field_match = False
            for geo_column in geography["fields"][column]["potential_names"]:
                for column_name in column_names:
                    if column_name.strip() == geo_column:
                        valid_field_match = True
                        field_matches[column] = column_name.strip()
            valid_match.append(valid_field_match)
        if all(valid_match):
            geography["field_matches"] = field_matches
            matching_geographies.append(geography)

    return matching_geographies


def import_point_dataset(
    file_path: str,
    latitude: str,
    longitude: str,
    table_name: str,
):
    """
    Imports a point dataset into a PostgreSQL database using the ogr2ogr command.

    This function utilizes ogr2ogr to convert a file containing point data into a PostgreSQL
    table. The function specifies the latitude and longitude fields to be used for point
    geometry creation.

    Args:
        file_path (str): The path to the file containing the point dataset.
        latitude (str): The column name in the file that represents the latitude values.
        longitude (str): The column name in the file that represents the longitude values.
        table_name (str): The name of the PostgreSQL table where the data will be imported.

    """
    subprocess.call(
        f"""ogr2ogr \
        -f "PostgreSQL" PG:"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST}" \
        {file_path} \
        -oo X_POSSIBLE_NAMES={longitude}* \
        -oo Y_POSSIBLE_NAMES={latitude}* \
        -nln {table_name} \
        -a_srs "EPSG:4326" \
        -lco GEOMETRY_NAME=geom \
        -lco FID=gid \
        -nlt POINT \
        -overwrite""",
        shell=True,
    )

    return {
        "status": True,
        "table_name": table_name,
    }


def join_to_map_service(
    file_path: str,
    table_name: str,
    map_name: str,
    table_match_column: str,
    map_match_column: str,
):
    """
    Joins a file containing data to a map service using the ogr2ogr command.

    This function utilizes ogr2ogr to convert a file containing data into a PostgreSQL
    table and then joins that table to a map service table based on matching values between
    the two tables.

    Args:
        file_path (str): The path to the file containing the data.
        table_name (str): The name of the PostgreSQL table where the data will be imported.
        map_name (str): The name of the map service table that the data will be joined to.
        table_match_column (str): The column name in the file that will be used to join the data to the map service.
        map_match_column (str): The column name in the map service table that will be used to join the data to the map service.
    """
    subprocess.call(
        f"""ogr2ogr -f "PostgreSQL" PG:"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST}" \
        {file_path} -nln {table_name}_temp -lco FID=gid  -overwrite""",
        shell=True,
    )
    drop_table_query = f"""DROP TABLE IF EXISTS "{table_name}";"""
    join_sql = f"""CREATE TABLE "{table_name}" AS
            SELECT a.*, b."{map_match_column}", b.geom
            FROM "{table_name}_temp" as a
            LEFT JOIN "{map_name}" as b
            ON LOWER(a."{table_match_column}") = LOWER(b."{map_match_column}");
        """
    drop_temp_table_query = f"""DROP TABLE IF EXISTS "{table_name}_temp";"""

    connection = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
    )
    cursor = connection.cursor()
    cursor.execute(drop_table_query)
    cursor.execute(join_sql)
    cursor.execute(drop_temp_table_query)
    connection.commit()
    cursor.close()
    connection.close()

    return {
        "status": True,
        "table_name": table_name,
    }


def upload_geographic_file(file_path: str, table_name: str):
    """
    Uploads a geographic file to a PostgreSQL database using the ogr2ogr command.

    This function utilizes ogr2ogr to import a geographic file into a PostgreSQL
    table. The function assigns a geometry name and FID to the table and overwrites
    any existing table with the same name.

    Args:
        file_path (str): The path to the geographic file to be uploaded.
        table_name (str): The name of the PostgreSQL table where the data will be stored.
    """
    subprocess.call(
        f"""ogr2ogr -f "PostgreSQL" PG:"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST}" \
        {file_path} -nln {table_name} -lco FID=gid -lco GEOMETRY_NAME=geom  -overwrite""",
        shell=True,
    )

    return [
        {
            "status": True,
            "table_name": table_name,
        }
    ]


def convert_csv_to_geojson(file_path: str):
    """
    Converts a CSV file to a GeoJSON file.

    Args:
        file_path (str): The path to the CSV file to be converted.
    """
    geojson_object = {"type": "FeatureCollection", "features": []}
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            geojson_object["features"].append(
                {
                    "type": "Feature",
                    "geometry": json.loads(row["geojson"]),
                    "properties": {k: v for k, v in row.items() if k != "geojson"},
                }
            )
    with open(file_path.replace(".csv", ".geojson"), "w") as f:
        json.dump(geojson_object, f)


def convert_wkt_geometry_to_geojson(wkt_string: str):
    """
    Converts a WKT geometry to a GeoJSON geometry.

    Args:
        wkt_string (str): The WKT geometry to be converted.

    Returns:
        dict: A GeoJSON geometry.
    """

    geometry = wkt.loads(wkt_string)

    geojson_string = geojson.dumps(geometry.__geo_interface__)

    return json.loads(geojson_string)


def convert_wkt_to_geojson(file_path: str):
    """
    Converts a CSV file with WKT geometries to a GeoJSON file.

    Args:
        file_path (str): The path to the CSV file to be converted.
    """
    geojson_object = {"type": "FeatureCollection", "features": []}
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            geojson_object["features"].append(
                {
                    "type": "Feature",
                    "geometry": convert_wkt_geometry_to_geojson(row["wkt"]),
                    "properties": {k: v for k, v in row.items() if k != "wkt"},
                }
            )
    with open(file_path.replace(".csv", ".geojson"), "w") as f:
        json.dump(geojson_object, f)


def convert_wkb_geometry_to_geojson(wkb_string: str):
    """
    Converts a WKB geometry to a GeoJSON geometry.

    Args:
        wkb_string (str): The WKB geometry to be converted.

    Returns:
        dict: A GeoJSON geometry.
    """

    geometry = wkb.loads(wkb_string, hex=True)

    geojson_string = geojson.dumps(geometry.__geo_interface__)

    return json.loads(geojson_string)


def convert_wkb_to_geojson(file_path: str):
    """
    Converts a CSV file with WKB geometries to a GeoJSON file.

    Args:
        file_path (str): The path to the CSV file to be converted.
    """
    geojson_object = {"type": "FeatureCollection", "features": []}
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            geojson_object["features"].append(
                {
                    "type": "Feature",
                    "geometry": convert_wkb_geometry_to_geojson(row["wkb"]),
                    "properties": {k: v for k, v in row.items() if k != "wkb"},
                }
            )
    with open(file_path.replace(".csv", ".geojson"), "w") as f:
        json.dump(geojson_object, f)


def upload_csv_file(write_file_path: str, file_name: str, app: FastAPI) -> object:
    """
    Uploads a CSV file to a PostgreSQL database by finding a matching geography and either importing as points or joining to a map service.

    This function reads the first two lines of the CSV file, finds a matching geography by comparing the column names against the potential names of geography fields, and either imports the file as point data or joins it to a map service table based on the matching geography.

    Args:
        write_file_path (str): The path to the CSV file to be uploaded.
        file_name (str): The name of the CSV file.
        app (FastAPI): The FastAPI application instance.

    Returns:
        object: A dictionary containing a status and message. The status is True if the upload is successful and False if no matching geography is found. The message is a human-readable description of the result.
    """
    with open(write_file_path) as input_file:
        head = [next(input_file) for _ in range(2)]
    matching_geographies = find_matching_geographies(head[0].split(","), app)
    if len(matching_geographies) == 0:
        return {"status": False, "message": "No matching geography found"}
    matching_geography = sorted(matching_geographies, key=lambda x: x["rank"])[0]
    if matching_geography["name"] == "latitude_and_longitude":
        import_point_dataset(
            file_path=write_file_path,
            latitude=matching_geography["field_matches"]["latitude"],
            longitude=matching_geography["field_matches"]["longitude"],
            table_name=file_name.split(".")[0],
        )

    elif matching_geography["name"] == "geojson_geometry":
        convert_csv_to_geojson(file_path=write_file_path)
        upload_geographic_file(
            file_path=write_file_path.replace(".csv", ".geojson"),
            table_name=file_name.split(".")[0],
        )
        os.remove(write_file_path.replace(".csv", ".geojson"))

    elif matching_geography["name"] == "wkt_geometry":
        convert_wkt_to_geojson(file_path=write_file_path)
        upload_geographic_file(
            file_path=write_file_path.replace(".csv", ".geojson"),
            table_name=file_name.split(".")[0],
        )
        os.remove(write_file_path.replace(".csv", ".geojson"))

    elif matching_geography["name"] == "wkb_geometry":
        convert_wkb_to_geojson(file_path=write_file_path)
        upload_geographic_file(
            file_path=write_file_path.replace(".csv", ".geojson"),
            table_name=file_name.split(".")[0],
        )
        os.remove(write_file_path.replace(".csv", ".geojson"))

    else:
        map_match_column = list(matching_geography["field_matches"].keys())[0]

        join_to_map_service(
            file_path=write_file_path,
            table_name=file_name.split(".")[0],
            map_name=matching_geography["name"],
            table_match_column=matching_geography["field_matches"][map_match_column],
            map_match_column=map_match_column,
        )

    return {
        "status": True,
        "table_name": file_name.split(".")[0],
    }
