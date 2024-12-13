import csv
import json
import os
import shutil
import subprocess

import geojson
import openpyxl
import psycopg2
from fastapi import FastAPI, HTTPException, status
from shapely import wkb, wkt

VALID_OGR_RETURN_CODES = ['0', '139']


def upload_flat_file(
    file_name: str,
    file_extension: str,
    file_path: str,
    app: FastAPI,
    zip_file: bool = False,
):
    """
    Upload a flat file to the server and import it into a PostgreSQL database.
    """
    if file_extension.lower() == "csv":
        result = upload_csv_file(
            write_file_path=file_path,
            file_name=file_name,
            app=app,
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
                app=app,
            )
            results.append(result)
        shutil.rmtree(f"{os.getcwd()}/media/{new_file_name}")
        media_directory = os.listdir(f"{os.getcwd()}/media/")
        for uploaded_file in media_directory:
            if file_name in uploaded_file:
                os.remove(f"{os.getcwd()}/media/{uploaded_file}")
    else:
        results = upload_geographic_file(
            file_path=file_path, table_name=file_name, app=app, zip_file=zip_file
        )
    media_directory = os.listdir(f"{os.getcwd()}/media/")
    for uploaded_file in media_directory:
        if file_name in uploaded_file and os.path.isfile(
            f"{os.getcwd()}/media/{uploaded_file}"
        ):
            os.remove(f"{os.getcwd()}/media/{uploaded_file}")
    if os.path.exists(f"{os.getcwd()}/media/{file_name}"):
        shutil.rmtree(f"{os.getcwd()}/media/{file_name}")

    if zip_file:
        return results[0]
    return results


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


def delete_files(file_name: str):
    """
    Deletes files and directories from the media directory that match the given file name.

    This function iterates through the files in the media directory and removes any files
    that contain the specified file name. It also removes a directory with the specified
    file name if it exists.

    Args:
        file_name (str): The name of the file or directory to be deleted.
    """
    media_directory = os.listdir(f"{os.getcwd()}/media/")
    for file in media_directory:
        if file_name in file and os.path.isfile(f"{os.getcwd()}/media/{file}"):
            os.remove(f"{os.getcwd()}/media/{file}")
    if os.path.exists(f"{os.getcwd()}/media/{file_name}"):
        shutil.rmtree(f"{os.getcwd()}/media/{file_name}")


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
    app: FastAPI,
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
    table_name = clean_string(table_name)

    result = subprocess.run(
        f"""ogr2ogr \
        -f "PostgreSQL" PG:"dbname={app.state.dbname} user={app.state.dbuser} password={app.state.dbpass} host={app.state.dbhost} port={app.state.dbport}" \
        {file_path} \
        -oo X_POSSIBLE_NAMES={longitude}* \
        -oo Y_POSSIBLE_NAMES={latitude}* \
        -nln {table_name} \
        -a_srs "EPSG:4326" \
        -lco GEOMETRY_NAME=geom \
        -lco FID=gid \
        -nlt POINT \
        -overwrite""",
        capture_output=True,
        text=True,
        shell=True,
    )

    if str(result.returncode) not in VALID_OGR_RETURN_CODES:
        default_error = result.stderr

        if "Unable to open datasource" in default_error:
            default_error = "The file provided is not a valid geographic file or has invalid geometry."
        delete_files(table_name)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=default_error
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
    app: FastAPI,
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
    table_name = clean_string(table_name)

    result = subprocess.run(
        f"""ogr2ogr -f "PostgreSQL" PG:"dbname={app.state.dbname} user={app.state.dbuser} password={app.state.dbpass} host={app.state.dbhost} port={app.state.dbport}" \
        {file_path} -nln {table_name}_temp -lco FID=gid  -overwrite""",
        capture_output=True,
        text=True,
        shell=True,
    )

    if str(result.returncode) not in VALID_OGR_RETURN_CODES:
        default_error = result.stderr

        if "Unable to open datasource" in default_error:
            default_error = "The file provided is not a valid geographic file or has invalid geometry."

        delete_files(table_name)

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=default_error
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
        dbname=app.state.dbname,
        user=app.state.dbuser,
        password=app.state.dbpass,
        host=app.state.dbhost,
        port=app.state.dbport,
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


def upload_geographic_file(
    file_path: str, table_name: str, app: FastAPI, zip_file: bool = False
):
    """
    Uploads a geographic file to a PostgreSQL database using the ogr2ogr command.

    This function utilizes ogr2ogr to import a geographic file into a PostgreSQL
    table. The function assigns a geometry name and FID to the table and overwrites
    any existing table with the same name.

    Args:
        file_path (str): The path to the geographic file to be uploaded.
        table_name (str): The name of the PostgreSQL table where the data will be stored.
    """
    table_name = clean_string(table_name)

    result = subprocess.run(
        [
            f"""ogr2ogr -f "PostgreSQL" PG:"dbname={app.state.dbname} user={app.state.dbuser} password={app.state.dbpass} host={app.state.dbhost} port={app.state.dbport}" \
        {file_path} -nln {table_name} -lco FID=gid -lco GEOMETRY_NAME=geom  -overwrite"""
        ],
        capture_output=True,
        text=True,
        shell=True,
    )


    if str(result.returncode) not in VALID_OGR_RETURN_CODES:
        default_error = result.stderr

        if "Unable to open datasource" in default_error:
            default_error = "The file provided is not a valid geographic file or has invalid geometry."

        delete_files(table_name)

        if zip_file:
            return [
                {
                    "status": False,
                    "table_name": table_name,
                    "error": default_error,
                }
            ]

        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=default_error
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
    features = []
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            features.append(
                {
                    "type": "Feature",
                    "geometry": json.loads(row["geojson"]),
                    "properties": {k: v for k, v in row.items() if k != "geojson"},
                }
            )
    geojson_object = {"type": "FeatureCollection", "features": []}
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
    features = []
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            features.append(
                {
                    "type": "Feature",
                    "geometry": convert_wkt_geometry_to_geojson(row["wkt"]),
                    "properties": {k: v for k, v in row.items() if k != "wkt"},
                }
            )
    geojson_object = {"type": "FeatureCollection", "features": []}
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
    features = []
    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            features.append(
                {
                    "type": "Feature",
                    "geometry": convert_wkb_geometry_to_geojson(row["wkb"]),
                    "properties": {k: v for k, v in row.items() if k != "wkb"},
                }
            )
    geojson_object = {"type": "FeatureCollection", "features": features}
    with open(file_path.replace(".csv", ".geojson"), "w") as f:
        json.dump(geojson_object, f)


def upload_csv_file(write_file_path: str, file_name: str, app: FastAPI) -> dict:
    """
    Uploads a CSV file to a PostgreSQL database by finding a matching geography
    and either importing as points or joining to a map service.

    This function reads the first two lines of the CSV file, finds a matching geography
    by comparing the column names against the potential names of geography fields, and
    either imports the file as point data or joins it to a map service table based on
    the matching geography.

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
            app=app,
        )

    elif matching_geography["name"] == "geojson_geometry":
        convert_csv_to_geojson(file_path=write_file_path)
        upload_geographic_file(
            file_path=write_file_path.replace(".csv", ".geojson"),
            table_name=file_name.split(".")[0],
            app=app,
        )
        os.remove(write_file_path.replace(".csv", ".geojson"))

    elif matching_geography["name"] == "wkt_geometry":
        convert_wkt_to_geojson(file_path=write_file_path)
        upload_geographic_file(
            file_path=write_file_path.replace(".csv", ".geojson"),
            table_name=file_name.split(".")[0],
            app=app,
        )
        os.remove(write_file_path.replace(".csv", ".geojson"))

    elif matching_geography["name"] == "wkb_geometry":
        convert_wkb_to_geojson(file_path=write_file_path)
        upload_geographic_file(
            file_path=write_file_path.replace(".csv", ".geojson"),
            table_name=file_name.split(".")[0],
            app=app,
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
            app=app,
        )

    return {
        "status": True,
        "table_name": file_name.split(".")[0],
    }
