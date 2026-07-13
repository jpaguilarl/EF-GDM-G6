from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


class TestCreateApp:
    def test_returns_fastapi_instance(self):
        from app.serving.app import create_app
        app = create_app()
        assert isinstance(app, FastAPI)
        assert app.title == "NY TLC Serving Layer"
        assert app.version == "0.1.0"

    def test_included_routers_registered(self):
        from app.serving.app import create_app
        app = create_app()
        included = [r for r in app.routes if type(r).__name__ == "_IncludedRouter"]
        assert len(included) == 5

    def test_engine_set_before_lifespan(self):
        from app.serving.app import create_app
        from app.serving.query_engine import PolarsQueryEngine
        mock_redis = MagicMock()
        mock_redis.connect = AsyncMock()
        mock_redis.close = AsyncMock()
        mock_loader = MagicMock()
        mock_loader.if_models = {}
        mock_loader.kmodes_models = {}

        with patch("app.serving.app.RedisClient", return_value=mock_redis), \
             patch("app.serving.app.ZoneLookup"), \
             patch("app.serving.app.EventBus"), \
             patch("app.serving.app.EventProcessor"), \
             patch("app.serving.app.RealtimeAggregator"), \
             patch("app.serving.app.FraudScorer"), \
             patch("app.serving.app.TripProfiler"), \
             patch("app.serving.app.ModelLoader", return_value=mock_loader):
            app = create_app()
            assert isinstance(app.state.engine, PolarsQueryEngine)

    def test_health_endpoint_returns_200(self):
        from app.serving.app import create_app
        mock_redis = MagicMock()
        mock_redis.connect = AsyncMock()
        mock_redis.close = AsyncMock()
        mock_loader = MagicMock()
        mock_loader.if_models = {}
        mock_loader.kmodes_models = {}

        with patch("app.serving.app.ZoneLookup"), \
             patch("app.serving.app.RedisClient", return_value=mock_redis), \
             patch("app.serving.app.ModelLoader", return_value=mock_loader), \
             patch("app.serving.app.EventProcessor"), \
             patch("app.serving.app.RealtimeAggregator"), \
             patch("app.serving.app.FraudScorer"), \
             patch("app.serving.app.TripProfiler"), \
             patch("app.serving.app.PolarsQueryEngine"), \
             patch("app.serving.app.MergedViewReader"):

            app = create_app()
            with TestClient(app) as client:
                resp = client.get("/health")
                assert resp.status_code == 200
                assert resp.json()["status"] == "ok"

    def test_reload_models_endpoint_registered(self):
        from app.serving.app import create_app
        mock_redis = MagicMock()
        mock_redis.connect = AsyncMock()
        mock_redis.close = AsyncMock()
        mock_loader = MagicMock()
        mock_loader.if_models = {}
        mock_loader.kmodes_models = {}
        mock_loader.load = MagicMock()

        with patch("app.serving.app.ZoneLookup"), \
             patch("app.serving.app.RedisClient", return_value=mock_redis), \
             patch("app.serving.app.ModelLoader", return_value=mock_loader), \
             patch("app.serving.app.EventProcessor"), \
             patch("app.serving.app.RealtimeAggregator"), \
             patch("app.serving.app.FraudScorer"), \
             patch("app.serving.app.TripProfiler"), \
             patch("app.serving.app.PolarsQueryEngine"), \
             patch("app.serving.app.MergedViewReader"):

            app = create_app()
            with TestClient(app) as client:
                resp = client.post("/api/v1/admin/reload-models")
                assert resp.status_code == 200
                assert resp.json()["status"] == "ok"
