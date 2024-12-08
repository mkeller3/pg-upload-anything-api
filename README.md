<p align="center">
  <img width="500" src="docs/logos/logo.png"/ alt-text="main image">
  <h1 align="center">pg-upload-anything-api</h1>
</p>

---

[![test](https://github.com/mkeller3/pg-upload-anything-api/actions/workflows/python-tests.yml/badge.svg?branch=main)](https://github.com/mkeller3/pg-upload-anything-api/actions/workflows/unit_tests.yml)
[![Codacy Badge](https://app.codacy.com/project/badge/Coverage/5dfc4af2e8f640298197a37a4c2ea993)](https://app.codacy.com/gh/mkeller3/pg-upload-anything-api/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_coverage)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/5dfc4af2e8f640298197a37a4c2ea993)](https://app.codacy.com/gh/mkeller3/pg-upload-anything-api/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
![GitHub contributors](https://img.shields.io/github/contributors/mkeller3/pg-upload-anything-api)
![GitHub last commit](https://img.shields.io/github/last-commit/mkeller3/pg-upload-anything-api?logo=github)

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
[{"status": "success", "table_name": "table_name"}]
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
[{"status": "success", "table_name": "table_name"}]
```