import pytest
from flask import request
from unittest.mock import Mock, patch

from services.slack_access_token import app

WHALE_DOCS_PATCH = "/slack-api-path" # Flask cannot test external redirects

@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client

@patch('requests.get')
@patch('services.slack_access_token.REDIRECT_URL', WHALE_DOCS_PATCH)
def test_passes_access_token(mock_get, client):
    token = "000"
    mock_get.return_value = Mock(ok=True)
    mock_get.return_value.json.return_value = {"ok": True, "app_id": "ABC", "access_token": token}

    response = client.get("/?code=123", follow_redirects=True)

    assert request.full_path == f"{WHALE_DOCS_PATCH}?token={token}"

@patch('requests.get')
@patch('services.slack_access_token.REDIRECT_URL', WHALE_DOCS_PATCH)
def test_passes_on_error(mock_get, client):
    error_message = "invalid_code"
    mock_get.return_value = Mock(ok=False)
    mock_get.return_value.json.return_value = {"ok": False, "error": error_message}

    rv = client.get("/?code=123", follow_redirects=True)

    assert request.full_path == f"{WHALE_DOCS_PATCH}?error={error_message}"
