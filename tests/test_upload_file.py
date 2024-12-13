import os

def test_upload_file_csv(app):
    """
    Test the upload file endpoint with a CSV file.
    """

    # Test with a valid CSV file that contains WKT
    with open(f"{os.getcwd()}/tests/files/pass/wkt_test.csv", "rb") as f:
        response = app.post("/api/v1/upload_anything/upload_file", files={"file": f})
        assert response.status_code == 200

    # Test with a valid CSV file that contains WKB
    with open(f"{os.getcwd()}/tests/files/pass/wkb_test.csv", "rb") as f:
        response = app.post("/api/v1/upload_anything/upload_file", files={"file": f})
        assert response.status_code == 200

    # Test with a valid CSV file that contains GeoJSON
    with open(f"{os.getcwd()}/tests/files/pass/cities_geojson.csv", "rb") as f:
        response = app.post("/api/v1/upload_anything/upload_file", files={"file": f})
        assert response.status_code == 200

    # Test with a valid CSV file that contains lat and long
    with open(f"{os.getcwd()}/tests/files/pass/us_capitals.csv", "rb") as f:
        response = app.post("/api/v1/upload_anything/upload_file", files={"file": f})
        assert response.status_code == 200

    # Test with a valid CSV file that contains geo boundaries
    with open(f"{os.getcwd()}/tests/files/pass/us_states.csv", "rb") as f:
        response = app.post("/api/v1/upload_anything/upload_file", files={"file": f})
        assert response.status_code == 200

    # # Test with a valid xlsx file
    with open(f"{os.getcwd()}/tests/files/pass/us_capitals_excel.xlsx", "rb") as f:
        response = app.post("/api/v1/upload_anything/upload_file", files={"file": f})
        assert response.status_code == 200

    # Test with a invalid file type
    with open(f"{os.getcwd()}/tests/files/fail/test.txt", "rb") as f:
        response = app.post("/api/v1/upload_anything/upload_file", files={"file": f})
        assert response.status_code == 400

    # Test with a geojson file
    with open(f"{os.getcwd()}/tests/files/pass/valid_geojson.geojson", "rb") as f:
        response = app.post("/api/v1/upload_anything/upload_file", files={"file": f})
        assert response.status_code == 200
    
    # Test with a zip file
    with open(f"{os.getcwd()}/tests/files/pass/valid_geojson.zip", "rb") as f:
        response = app.post("/api/v1/upload_anything/upload_file", files={"file": f})
        assert response.status_code == 200

    # Test with a zip file - csv
    with open(f"{os.getcwd()}/tests/files/pass/us_states.zip", "rb") as f:
        response = app.post("/api/v1/upload_anything/upload_file", files={"file": f})
        assert response.status_code == 200
    
    # Test with a zip file - xlsx
    with open(f"{os.getcwd()}/tests/files/pass/us_capitals_excel.zip", "rb") as f:
        response = app.post("/api/v1/upload_anything/upload_file", files={"file": f})
        assert response.status_code == 200
    
    # Test with a doc file
    with open(f"{os.getcwd()}/tests/files/fail/unvalid_geojson_file.doc", "rb") as f:
        response = app.post("/api/v1/upload_anything/upload_file", files={"file": f})
        assert response.status_code == 400
    
    # Test with a bad geojson
    with open(f"{os.getcwd()}/tests/files/fail/unvalid_geojson.geojson", "rb") as f:
        response = app.post("/api/v1/upload_anything/upload_file", files={"file": f})
        assert response.status_code == 400
    
    # Test with a shapefile
    with open(f"{os.getcwd()}/tests/files/pass/valid_shapefile.zip", "rb") as f:
        response = app.post("/api/v1/upload_anything/upload_file", files={"file": f})
        assert response.status_code == 200