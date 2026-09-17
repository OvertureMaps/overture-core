"""Unit tests for ``overture_core.maproulette.client``."""

import math
from unittest import mock

import maproulette
import pandas as pd
import pytest
import requests
import shapely
from maproulette.api.errors import HttpError, InvalidJsonError

from overture_core.apis import maproulette as client_module
from overture_core.apis.maproulette import MapRouletteClient, simplified


class _FakeClient:
    """Stand-in for a maproulette client (skips the network health check on init)."""

    def __init__(self, configuration):
        self.configuration = configuration
        self.session = requests.Session()


def test_simplified_slugifies_name():
    assert simplified("Fix Invalid Intersections!") == "fix_invalid_intersections"
    assert simplified("  --Leading/Trailing--  ") == "leading_trailing"


def test_retrying_client_mounts_retry():
    client = client_module._retrying_client(_FakeClient, configuration="cfg")
    retry = client.session.get_adapter("https://maproulette.org").max_retries

    assert retry.total == 3
    assert 502 in retry.status_forcelist
    assert 524 in retry.status_forcelist
    assert retry.backoff_factor == 2
    assert retry.allowed_methods is None  # retry every HTTP method


def _bare_client(**overrides):
    """A MapRouletteClient with its API clients replaced by mocks, skipping __init__'s
    network calls."""
    client = MapRouletteClient.__new__(MapRouletteClient)
    client.name = overrides.get("name", "Fix Invalid Intersections")
    client.simple_name = simplified(client.name)
    client.description = overrides.get("description", "desc")
    client.common_instructions = overrides.get("common_instructions", "common")
    client.violation_descriptions = overrides.get("violation_descriptions", {})
    client.checkin_comment = overrides.get("checkin_comment", "checkin")
    client.locations = overrides.get("locations", [{"name": "California"}])
    client.output_bucket = overrides.get("output_bucket", "bucket")
    client.keywords = overrides.get("keywords", ["osm_checks", client.simple_name])
    client.admins = overrides.get("admins", [])
    client.limit = overrides.get("limit", client_module.DEFAULT_TASK_LIMIT)
    client._project_api = overrides.get("project_api", mock.MagicMock())
    client._challenge_api = overrides.get("challenge_api", mock.MagicMock())
    client._user_api = overrides.get("user_api", mock.MagicMock())
    return client


class _FakeUserApi:
    """In-memory MapRoulette user client that records add_user_to_project calls."""

    def __init__(self, manager_ids, users):
        self._manager_ids = manager_ids  # ids already managing the project
        self._users = users  # username -> user id (missing = not found)
        self.added = []

    def get(self, endpoint):
        return {"data": [{"userId": uid} for uid in self._manager_ids]}

    def find_user_by_username(self, username):
        if username not in self._users:
            return {"data": []}
        return {"data": [{"id": self._users[username]}]}

    def add_user_to_project(self, user_id, project_id, group_type, is_osm_user_id):
        self.added.append(user_id)


def test_get_project_manager_ids_parses_managers_endpoint():
    user_api = _FakeUserApi(manager_ids={11, 22, 33}, users={})

    ids = MapRouletteClient._get_project_manager_ids(user_api, project_id=58349)

    assert ids == {11, 22, 33}


def test_get_project_manager_ids_handles_no_managers():
    user_api = _FakeUserApi(manager_ids=set(), users={})

    assert MapRouletteClient._get_project_manager_ids(user_api, 1) == set()


def test_sync_project_admins_only_adds_missing():
    api = _FakeUserApi(
        manager_ids={11},  # alice already manages
        users={"alice": 11, "bob": 22},  # carol intentionally absent -> not found
    )
    client = _bare_client(admins=["alice", "bob", "carol"], user_api=api)

    client._sync_project_admins(project_id=58349)

    assert api.added == [22]  # only bob; alice skipped, carol not found


class _FakeRebuildApi:
    """Fake Project/Challenge client; rebuild_challenge raises the configured error."""

    def __init__(self, rebuild_error=None):
        self._rebuild_error = rebuild_error

    def get_project_by_name(self, name):
        return {"data": {"id": 1}}

    def get_challenge_by_name(self, project_id, challenge_name):
        return {"data": {"id": 2}}

    def rebuild_challenge(self, **kwargs):
        if self._rebuild_error:
            raise self._rebuild_error


def _run_rebuild(rebuild_error):
    api = _FakeRebuildApi(rebuild_error)
    client = _bare_client(project_api=api, challenge_api=api)
    client.rebuild_challenge({"name": "California"})


def test_rebuild_treats_in_progress_400_as_success():
    err = InvalidJsonError(
        message="Task build is already in progress for this challenge", status=400
    )
    _run_rebuild(err)  # no raise


def test_rebuild_treats_502_as_success():
    _run_rebuild(HttpError(message="bad gateway", status=502))  # no raise


def test_rebuild_treats_dict_message_400_as_success():
    # Defensive: even if .message arrives as a dict, str(e) still carries the phrase.
    err = InvalidJsonError(
        message={"message": "Task build is already in progress for this challenge"},
        status=400,
    )
    _run_rebuild(err)  # no raise


def test_rebuild_reraises_other_400():
    err = InvalidJsonError(message="Something genuinely wrong", status=400)
    with pytest.raises(InvalidJsonError):
        _run_rebuild(err)


def test_rebuild_reraises_other_http_status():
    err = HttpError(message="teapot", status=418)
    with pytest.raises(HttpError):
        _run_rebuild(err)


class _FakeProjectApi:
    """Records create/update calls; get_project_by_name raises NotFoundError once."""

    def __init__(self, existing_project=None):
        self.existing_project = existing_project
        self.created = None
        self.updated = None

    def get_project_by_name(self, name):
        if self.existing_project is None:
            raise maproulette.api.errors.NotFoundError(message="not found", status=404)
        return self.existing_project

    def create_project(self, data):
        self.created = data
        return {"data": {"id": 99, **data}}

    def update_project(self, project_id, data):
        self.updated = (project_id, data)


def test_get_or_create_project_creates_when_missing():
    api = _FakeProjectApi(existing_project=None)
    client = _bare_client(project_api=api)

    project_id = client._get_or_create_project()

    assert project_id == 99
    assert api.created == {
        "name": client.simple_name,
        "description": client.description,
        "displayName": client.name,
    }


def test_get_or_create_project_updates_when_present():
    api = _FakeProjectApi(existing_project={"data": {"id": 7}})
    client = _bare_client(project_api=api)

    project_id = client._get_or_create_project()

    assert project_id == 7
    assert api.updated == (7, {"description": client.description})


class _FakeChallengeApi:
    def __init__(self, existing_challenge=None):
        self.existing_challenge = existing_challenge
        self.created = None
        self.updated = None

    def get_challenge_by_name(self, project_id, challenge_name):
        if self.existing_challenge is None:
            raise maproulette.api.errors.NotFoundError(message="not found", status=404)
        return self.existing_challenge

    def create_challenge(self, data):
        self.created = data

    def update_challenge(self, challenge_id, data):
        self.updated = (challenge_id, data)


def test_upsert_challenge_creates_when_missing():
    api = _FakeChallengeApi(existing_challenge=None)
    client = _bare_client(challenge_api=api)

    client._upsert_challenge(project_id=1, location={"name": "California"})

    assert api.created is not None
    assert api.created.name == "California - Fix Invalid Intersections"


def test_upsert_challenge_skips_when_up_to_date():
    client = _bare_client()
    challenge_data = client._challenge_api = mock.MagicMock()
    remote_url = client._remote_geojson_url({"name": "California"})
    challenge_data.get_challenge_by_name.return_value = {
        "data": {
            "id": 5,
            "description": client.description,
            "instruction": f"{{{{violation_description}}}} {client.common_instructions}",
            "checkinComment": f"{client.checkin_comment}: {{{{violation_description}}}}",
            "remoteGeoJson": remote_url,
        }
    }

    client._upsert_challenge(project_id=1, location={"name": "California"})

    challenge_data.update_challenge.assert_not_called()
    challenge_data.create_challenge.assert_not_called()


def test_upsert_challenge_updates_on_drift():
    api = _FakeChallengeApi(
        existing_challenge={
            "data": {
                "id": 5,
                "description": "stale description",
                "instruction": "stale",
                "checkinComment": "stale",
                "remoteGeoJson": "stale",
            }
        }
    )
    client = _bare_client(challenge_api=api)

    client._upsert_challenge(project_id=1, location={"name": "California"})

    assert api.updated is not None
    assert api.updated[0] == 5


def test_delete_project_skips_when_project_not_found():
    api = mock.MagicMock()
    api.get_project_by_name.side_effect = maproulette.api.errors.NotFoundError(
        message="not found", status=404
    )
    client = _bare_client(project_api=api)

    client.delete_project()  # no raise

    api.delete_project.assert_not_called()


def test_client_init_sets_attributes_and_builds_apis():
    with mock.patch.object(
        client_module, "_retrying_client", return_value=mock.MagicMock()
    ):
        client = MapRouletteClient(
            configuration=mock.MagicMock(),
            name="Fix Invalid Intersections",
            description="desc",
            common_instructions="common",
            violation_descriptions={},
            checkin_comment="checkin",
            locations=[{"name": "California"}],
            output_bucket="my-bucket",
            keywords=["extra"],
            admins=["alice"],
        )

    assert client.simple_name == "fix_invalid_intersections"
    assert client.keywords == ["osm_checks", "fix_invalid_intersections", "extra"]
    assert client.admins == ["alice"]
    assert client.limit == client_module.DEFAULT_TASK_LIMIT
    assert client._remote_geojson_url({"name": "California"}) == (
        "https://my-bucket.s3.us-west-2.amazonaws.com/maproulette/"
        "fix_invalid_intersections/california.json"
    )


def _real_client(**overrides):
    """A real (non-``__new__``-bypassed) MapRouletteClient, for exercising the
    lazy API client construction in ``__init__``."""
    return MapRouletteClient(
        configuration=mock.MagicMock(),
        name=overrides.get("name", "Fix Invalid Intersections"),
        description=overrides.get("description", "desc"),
        common_instructions=overrides.get("common_instructions", "common"),
        violation_descriptions=overrides.get("violation_descriptions", {}),
        checkin_comment=overrides.get("checkin_comment", "checkin"),
        locations=overrides.get("locations", [{"name": "California"}]),
        output_bucket=overrides.get("output_bucket", "bucket"),
    )


def test_init_does_not_eagerly_build_any_api_client():
    with mock.patch.object(client_module, "_retrying_client") as retrying_client:
        _real_client()

    retrying_client.assert_not_called()


def test_build_feature_collection_builds_no_api_clients():
    with mock.patch.object(client_module, "_retrying_client") as retrying_client:
        client = _real_client(
            violation_descriptions={"missing_name": {"default": "desc"}}
        )
        df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "type": "node",
                    "flag_id": "flag-1",
                    "severity": 1,
                    "violation_name": "missing_name",
                    "context": None,
                    "tags": {},
                    "geometry": shapely.wkb.dumps(shapely.Point(0, 0)),
                }
            ]
        )
        client.build_feature_collection(df)

    retrying_client.assert_not_called()


def test_rebuild_challenge_does_not_build_user_api():
    api = _FakeRebuildApi()
    with mock.patch.object(
        client_module, "_retrying_client", return_value=api
    ) as retrying_client:
        client = _real_client()
        client.rebuild_challenge({"name": "California"})

    assert retrying_client.call_args_list == [
        mock.call(maproulette.Project, client._configuration),
        mock.call(maproulette.Challenge, client._configuration),
    ]


def test_delete_project_does_not_build_user_api():
    project_api = mock.MagicMock()
    project_api.get_project_by_name.return_value = {"data": {"id": 1}}
    challenge_api = mock.MagicMock()
    challenge_api.get_challenge_by_name.return_value = {"data": {"id": 2}}

    def fake_retrying_client(api_cls, configuration):
        return project_api if api_cls is maproulette.Project else challenge_api

    with mock.patch.object(
        client_module, "_retrying_client", side_effect=fake_retrying_client
    ) as retrying_client:
        client = _real_client()
        client.delete_project()

    called_classes = {call.args[0] for call in retrying_client.call_args_list}
    assert called_classes == {maproulette.Project, maproulette.Challenge}
    assert maproulette.User not in called_classes


def test_project_api_is_cached_across_accesses():
    with mock.patch.object(
        client_module, "_retrying_client", return_value=mock.MagicMock()
    ) as retrying_client:
        client = _real_client()
        first = client._project_api
        second = client._project_api

    assert first is second
    retrying_client.assert_called_once_with(maproulette.Project, client._configuration)


def test_sync_project_admins_swallows_maproulette_errors():
    api = mock.MagicMock()
    api.find_user_by_username.return_value = {"data": [{"id": 1}]}
    api.get.return_value = {"data": []}
    api.add_user_to_project.side_effect = (
        maproulette.api.errors.MapRouletteBaseException(
            message="boom", status=500, payload={}
        )
    )
    client = _bare_client(admins=["alice"], user_api=api)

    client._sync_project_admins(project_id=1)  # no raise


def test_update_project_syncs_project_admins_and_all_challenges():
    client = _bare_client(locations=[{"name": "California"}, {"name": "Texas"}])
    client._get_or_create_project = mock.MagicMock(return_value=42)
    client._sync_project_admins = mock.MagicMock()
    client._upsert_challenge = mock.MagicMock()

    project_id = client.update_project()

    assert project_id == 42
    client._sync_project_admins.assert_called_once_with(42)
    assert client._upsert_challenge.call_args_list == [
        mock.call(42, {"name": "California"}),
        mock.call(42, {"name": "Texas"}),
    ]


def test_delete_project_swallows_maproulette_errors_on_challenge_delete():
    project_api = mock.MagicMock()
    project_api.get_project_by_name.return_value = {"data": {"id": 1}}
    challenge_api = mock.MagicMock()
    challenge_api.get_challenge_by_name.return_value = {"data": {"id": 2}}
    challenge_api.delete_challenge.side_effect = (
        maproulette.api.errors.MapRouletteBaseException(
            message="boom", status=500, payload={}
        )
    )
    client = _bare_client(project_api=project_api, challenge_api=challenge_api)

    client.delete_project()  # no raise


def test_build_feature_collection_uses_context_kind_template():
    client = _bare_client(
        violation_descriptions={
            "missing_name": {
                "generic": "Generic template",
                "restaurant": "Missing name for restaurant $osmIdentifier",
            }
        },
    )
    df = pd.DataFrame(
        [
            {
                "id": 1,
                "type": "node",
                "flag_id": "flag-1",
                "severity": 1,
                "violation_name": "missing_name",
                "context": '{"kind": "restaurant"}',
                "tags": {},
                "geometry": shapely.wkb.dumps(shapely.Point(0, 0)),
            }
        ]
    )

    collection = client.build_feature_collection(df)

    description = collection["features"][0]["properties"]["violation_description"]
    assert "Missing name for restaurant n1" in description
    assert collection["features"][0]["properties"]["kind"] == "restaurant"


def test_delete_project_deletes_project_and_challenges():
    project_api = mock.MagicMock()
    project_api.get_project_by_name.return_value = {"data": {"id": 1}}
    challenge_api = mock.MagicMock()
    challenge_api.get_challenge_by_name.return_value = {"data": {"id": 2}}
    client = _bare_client(project_api=project_api, challenge_api=challenge_api)

    client.delete_project()

    project_api.delete_project.assert_called_once_with(project_id=1, immediate="true")
    challenge_api.delete_challenge.assert_called_once_with(
        challenge_id=2, immediate="true"
    )


def test_build_feature_collection_renders_and_sanitizes_rows():
    client = _bare_client(
        violation_descriptions={
            "missing_name": {"default": "Missing name near $osmIdentifier"}
        },
        limit=1,
    )
    point = shapely.Point(1, 2)
    df = pd.DataFrame(
        [
            {
                "id": 123,
                "type": "node",
                "flag_id": "flag-1",
                "severity": 1,
                "violation_name": "missing_name",
                "context": None,
                "tags": {"amenity": "cafe"},
                "geometry": shapely.wkb.dumps(point),
            },
            {
                # Lower severity, dropped by limit=1.
                "id": 456,
                "type": "way",
                "flag_id": "flag-2",
                "severity": 0,
                "violation_name": "missing_name",
                "context": None,
                "tags": {},
                "geometry": shapely.wkb.dumps(point),
            },
        ]
    )

    collection = client.build_feature_collection(df)

    assert len(collection["features"]) == 1
    feature = collection["features"][0]
    assert feature["properties"]["identifier"] == "flag-1"
    assert feature["properties"]["osmIdentifier"] == "n123"
    assert feature["properties"]["amenity"] == "cafe"
    assert "Missing name near n123" in feature["properties"]["violation_description"]


def test_build_feature_collection_replaces_nan_and_inf():
    client = _bare_client(
        violation_descriptions={"missing_name": {"default": "desc"}},
    )
    df = pd.DataFrame(
        [
            {
                "id": 1,
                "type": "node",
                "flag_id": "flag-1",
                "severity": 1,
                "violation_name": "missing_name",
                "context": None,
                "tags": {"bad": math.nan, "worse": math.inf},
                "geometry": shapely.wkb.dumps(shapely.Point(0, 0)),
            }
        ]
    )

    collection = client.build_feature_collection(df)

    properties = collection["features"][0]["properties"]
    assert properties["bad"] is None
    assert properties["worse"] is None
