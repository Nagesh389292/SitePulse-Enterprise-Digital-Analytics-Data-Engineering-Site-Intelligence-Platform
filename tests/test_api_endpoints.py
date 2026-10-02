"""Test suite for FastAPI operational and scenario endpoints."""
import pytest
from fastapi.testclient import TestClient
from sitepulse.api.app import app

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "UP"

def test_dashboard_ui_served():
    response = client.get("/")
    assert response.status_code == 200
    assert "SitePulse" in response.text
    assert "Pipeline Stage Flow Visualizer" in response.text
    assert "Multi-Selection Pipeline Step Inspector" in response.text

def test_scenarios_catalog():
    response = client.get("/api/v1/scenarios")
    assert response.status_code == 200
    data = response.json()
    assert "scenarios" in data
    assert len(data["scenarios"]) == 6

def test_scenario_simulate_clean():
    response = client.post("/api/v1/scenarios/simulate", json={"scenario": "NONE"})
    assert response.status_code == 200
    data = response.json()
    assert data["scenario"] == "NONE"
    assert data["composite_score"] == 100.0
    assert len(data["steps"]) == 9

def test_scenario_simulate_mobile_drop():
    response = client.post("/api/v1/scenarios/simulate", json={"scenario": "FAIL_MOBILE_DROP"})
    assert response.status_code == 200
    data = response.json()
    assert data["scenario"] == "FAIL_MOBILE_DROP"
    assert data["structural_score"] == 100.0
    assert data["business_score"] == 0.0
    assert len(data["steps"]) == 9
