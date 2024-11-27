# pg-upload-anything-api

**pg-upload-anything-api**  is an api that allows you to easily upload geographical vector data into a PostgreSQL database. This API allows you take the guess work out of loading geograhphic or non geographic files into the database. This project was built with inspiration from Felt's [Upload Anything](https://felt.com/blog/upload-anything) API endpoints.

## Usage

### Running Locally

To run the app locally `uvicorn api.main:app --reload`

## Endpoints


| Method | URL                         | Description                               |
| ------ | --------------------------- | ----------------------------------------- |
| `POST`  | `/api/v1/upload_file`       | [Upload File](#upload-file)               |
| `POST`  | `/api/v1/upload_url`        | [Upload Url](#upload-url)                 |

### Upload File

The upload file endpoint imports common geogpraphic file types into the database.

#### Vector Data Files

- GeoPackage (`.gpkg`)
- GeoJSON (`.geojson`)
- GML (`.gml`)
- GPX (`.gpx`)
- KML (`.kml`)
- SQLite (`.sqlite`)
- Shapefile (`.shp`)
- ZIP (containing one of the above formats)

#### Spreadsheets
- CSV (`.csv`)
- Excel (`.xlsx`)

Read data directly from all sheets within the spreadsheet.

- If your data contains coordinates, it will convert your data to a point dataset.
- If your data contains geojson, WKT, or WKB. It will convert your data to the correct geometry.
- If your data contains a column that matches a pre-determined geography, it will convert your data to that geography.


#### Parameters

- `file=file`

#### Response

```json
{"status": "success", "table_name": "table_name"}
```

### Upload URL

The upload url endpoints imports data from a url into the database.

The url can contain data from the following endpoints:

- Google Sheets
- ESRI Feature Service
- ESRI Map Service
- OGC WFS
- OGC API - Features
- Flat Files

#### Spreadsheets
- CSV (`.csv`)
- Excel (`.xlsx`)

Read data directly from all sheets within the spreadsheet.

- If your data contains coordinates, it will convert your data to a point dataset.
- If your data contains geojson, WKT, or WKB. It will convert your data to the correct geometry.
- If your data contains a column that matches a pre-determined geography, it will convert your data to that geography.

#### Parameters

- `url=url`

#### Response

```json
{"status": "success", "table_name": "table_name"}
```