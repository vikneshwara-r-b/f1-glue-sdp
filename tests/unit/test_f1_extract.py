from unittest.mock import Mock, patch

import pytest
import requests

from f1_pipeline_lib.extract import fetch_race_payload, stage_payload

BASE_URL = "https://f1api.dev/api"
STAGING_PATH = "s3://my-bucket/my-prefix/bronze-staging/"


def test_fetch_race_payload_returns_response_text_on_success():
    mock_response = Mock(status_code=200, text='{"season": 2024}')
    mock_response.raise_for_status.return_value = None
    with patch("f1_pipeline_lib.extract.requests.get", return_value=mock_response) as mock_get:
        result = fetch_race_payload(BASE_URL, "2024", "1")

    mock_get.assert_called_once_with(f"{BASE_URL}/2024/1/race", timeout=30)
    assert result == '{"season": 2024}'


def test_fetch_race_payload_raises_on_connection_error():
    with patch(
        "f1_pipeline_lib.extract.requests.get",
        side_effect=requests.exceptions.ConnectionError("boom"),
    ):
        with pytest.raises(requests.exceptions.ConnectionError):
            fetch_race_payload(BASE_URL, "2024", "1")


def test_fetch_race_payload_raises_on_non_2xx_status():
    mock_response = Mock(status_code=404)
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404")
    with patch("f1_pipeline_lib.extract.requests.get", return_value=mock_response):
        with pytest.raises(requests.exceptions.HTTPError):
            fetch_race_payload(BASE_URL, "2024", "1")


@pytest.mark.parametrize("season,round_", [(None, "1"), ("2024", None), (None, None), ("", "1")])
def test_fetch_race_payload_raises_on_missing_conf(season, round_):
    with pytest.raises(ValueError):
        fetch_race_payload(BASE_URL, season, round_)


def test_stage_payload_writes_to_bucket_and_key_under_prefix():
    mock_s3 = Mock()
    with patch("f1_pipeline_lib.extract.boto3.client", return_value=mock_s3) as mock_client:
        result = stage_payload('{"season": 2024}', STAGING_PATH, "2024", "1")

    mock_client.assert_called_once_with("s3")
    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Bucket"] == "my-bucket"
    assert call_kwargs["Key"].startswith("my-prefix/bronze-staging/2024_1_")
    assert call_kwargs["Key"].endswith(".json")
    assert call_kwargs["Body"] == b'{"season": 2024}'
    assert result.startswith("s3://my-bucket/my-prefix/bronze-staging/2024_1_")


def test_stage_payload_keys_are_unique_across_calls():
    import time

    mock_s3 = Mock()
    with patch("f1_pipeline_lib.extract.boto3.client", return_value=mock_s3):
        first = stage_payload("a", STAGING_PATH, "2024", "1")
        time.sleep(0.01)  # guarantee a different millisecond timestamp
        second = stage_payload("b", STAGING_PATH, "2024", "1")

    assert first != second
