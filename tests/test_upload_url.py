def test_arcgis_url(app):
    """
    Test the upload URL endpoint with an ArcGIS URL.
    """

    # Test with a valid ArcGIS URL
    response = app.post("/api/v1/upload_anything/upload_url", json={"url": "https://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/USA_States_Generalized_Boundaries/FeatureServer"})
    assert response.status_code == 200

    # Test with a valid ArcGIS URL with a layer
    response = app.post("/api/v1/upload_anything/upload_url", json={"url": "https://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/USA_States_Generalized_Boundaries/FeatureServer/0"})
    assert response.status_code == 200

    # Test with a invalid ArcGIS URL layer
    response = app.post("/api/v1/upload_anything/upload_url", json={"url": "https://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/USA_States_Generalized_Boundaries/FeatureServer/1"})
    assert response.status_code == 400

    # Test with a invalid ArcGIS URL
    response = app.post("/api/v1/upload_anything/upload_url", json={"url": "https://services1.arcgis.com/abc/xyz"})
    assert response.status_code == 400

def test_google_sheets_url(app):
    """
    Test the upload URL endpoint with a Google Sheets URL.
    """

    # Test with a valid Google Sheets URL
    response = app.post("/api/v1/upload_anything/upload_url", json={"url": "https://docs.google.com/spreadsheets/d/1zaQkIhLZ8xIXmbPw0YVGesuMxZQqHvA-Lj_cIUgwVg0/edit?usp=sharing"})
    assert response.status_code == 200

    # Test with an invalid Google Sheets URL
    response = app.post("/api/v1/upload_anything/upload_url", json={"url": "https://docs1.google.com/spreadsheets/d/1kWVJYbCv7xY6rX5wJQXx7YpKJl9Qa7lJ0B5WZvQ0E/edit#gid=0"})
    assert response.status_code == 400

def test_ogc_wfs_url(app):
    """
    Test the upload URL endpoint with an OGC WFS URL.
    """

    # Test with a valid OGC WFS URL
    response = app.post("/api/v1/upload_anything/upload_url", json={"url": "https://sedac.ciesin.columbia.edu/geoserver/superfund/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=superfund:superfund-epa-national-priorities-list-ciesin-mod-v2"})
    assert response.status_code == 200

    # Test with a invalid OGC WFS URL
    response = app.post("/api/v1/upload_anything/upload_url", json={"url": "https://sedac.ciesin.columbia.edu/geoserver/superfund/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=superfund:superfund-epa-national-priorities-list-ciesin-mod-v1"})
    assert response.status_code == 400

def test_ogc_api_features_url(app):
    """
    Test the upload URL endpoint with an OGC API - Features URL.
    """

    # Test with a valid OGC API - Features URL
    response = app.post("/api/v1/upload_anything/upload_url", json={"url": "https://demo.pygeoapi.io/master/collections/lakes"})
    assert response.status_code == 200

    # Test with an invalid OGC API - Features URL
    response = app.post("/api/v1/upload_anything/upload_url", json={"url": "https://demo.pygeoapi.io/master/collections/lakes/1"})
    assert response.status_code == 400

def test_flat_file_url(app):
    """
    Test the upload URL endpoint with a flat file URL.
    """

    # Test with a valid flat file URL
    response = app.post("/api/v1/upload_anything/upload_url", json={"url": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"})
    assert response.status_code == 200

    # Test with an invalid flat file URL
    response = app.post("/api/v1/upload_anything/upload_url", json={"url": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson/1"})
    assert response.status_code == 400