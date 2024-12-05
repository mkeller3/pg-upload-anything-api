
def test_health_check(app):
    response = app.get("/api/v1/health_check")
    assert response.status_code == 200
    assert response.json() == {"status": "UP"}