from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.speed.ingest import router
from app.speed.schema import EnrichedRide


@pytest.fixture
def mock_processor():
    p = MagicMock()

    def process_side_effect(event):
        if event.service_id == "yellow" and event.vendor_id is not None:
            return EnrichedRide(
                trip_id=12345,
                service_id="yellow",
                pickup_datetime=event.pickup_datetime,
                dropoff_datetime=event.dropoff_datetime,
                pu_location_id=event.pu_location_id,
                do_location_id=event.do_location_id,
                pu_borough="Manhattan",
                pu_zone="Midtown",
                do_borough="Brooklyn",
                do_zone="Williamsburg",
                bloque_horario="Mediodía",
                franja_horaria="Tarde",
                dia_categoria="Día Laborable",
                is_weekend=False,
                pickup_hour=14,
                trip_duration_minutes=15.0,
                passenger_group="Solo",
                revenue=20.0,
                fare_amount=15.0,
                tolls_amount=2.5,
            )
        return None

    p.process = process_side_effect
    return p


@pytest.fixture
def mock_bus():
    bus = MagicMock()
    bus.publish = AsyncMock()
    return bus


@pytest.fixture
def client(mock_processor, mock_bus):
    app = FastAPI()
    app.state.processor = mock_processor
    app.state.event_bus = mock_bus
    app.include_router(router)
    with TestClient(app) as c:
        yield c


class TestIngestEndpoint:
    def test_valid_event_returns_accepted(self, client, mock_bus):
        payload = {
            "service_id": "yellow",
            "pickup_datetime": "2025-06-15T14:30:00",
            "dropoff_datetime": "2025-06-15T14:45:00",
            "vendor_id": 1,
            "pu_location_id": 237,
            "do_location_id": 238,
            "passenger_count": 1,
            "fare_amount": 15.0,
            "total_amount": 20.0,
        }
        resp = client.post("/api/v1/ingest", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "accepted"
        assert data["trip_id"] == 12345

    def test_valid_event_triggers_publish(self, client, mock_bus):
        payload = {
            "service_id": "yellow",
            "pickup_datetime": "2025-06-15T14:30:00",
            "dropoff_datetime": "2025-06-15T14:45:00",
            "vendor_id": 1,
            "pu_location_id": 237,
            "do_location_id": 238,
        }
        client.post("/api/v1/ingest", json=payload)
        mock_bus.publish.assert_awaited_once()

    def test_invalid_event_returns_rejected(self, client, mock_bus):
        payload = {
            "service_id": "yellow",
            "pickup_datetime": "2025-06-15T14:30:00",
            "vendor_id": None,
            "pu_location_id": 237,
            "do_location_id": 238,
        }
        resp = client.post("/api/v1/ingest", json=payload)
        assert resp.status_code == 422
        data = resp.json()
        assert data["status"] == "rejected"

    def test_invalid_event_does_not_publish(self, client, mock_bus):
        payload = {
            "service_id": "yellow",
            "pickup_datetime": "2025-06-15T14:30:00",
            "vendor_id": None,
        }
        client.post("/api/v1/ingest", json=payload)
        mock_bus.publish.assert_not_awaited()

    def test_missing_required_field_returns_422(self, client):
        payload = {
            "service_id": "yellow",
        }
        resp = client.post("/api/v1/ingest", json=payload)
        assert resp.status_code == 422
