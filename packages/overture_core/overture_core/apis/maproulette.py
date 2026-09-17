"""Framework-agnostic MapRoulette API client.

``MapRouletteClient`` owns the project/challenge lifecycle against the
MapRoulette API for a single project: create-or-update, admin sync (via the
raw ``/user/project/{id}`` endpoint the python client doesn't wrap), challenge
upsert with a drift check, rebuild with retry handling for transient
gateway/"already in progress" errors, delete, and building GeoJSON challenge
task collections from a violations dataframe.

It knows nothing about how it's invoked (Airflow, a script, a test) or where
its ``maproulette.Configuration`` credentials come from -- callers own both.
Location/admin lists (e.g. a fixed set of cities or countries, a default
admin roster) are deployment config owned by the caller, not this module.

The project/challenge/user API clients (each with its own MapRoulette
health-check ping on first use) are built lazily on first access rather than
eagerly in ``__init__``, so a caller that only needs ``rebuild_challenge`` or
``delete_project`` never pays for the user API client that only
``update_project``'s admin sync uses.

Requires the ``maproulette`` extra (``overture-core[maproulette]``): the
``maproulette``, ``geojson``, and ``shapely`` packages aren't hard
dependencies of ``overture_core`` since most consumers of this package don't
need them.
"""

from __future__ import annotations

import json
import math
import re
from string import Template
from typing import Any

import geojson
import maproulette
from maproulette.api.errors import (
    HttpError,
    InvalidJsonError,
    MapRouletteBaseException,
    NotFoundError,
)
from requests.adapters import HTTPAdapter
from shapely import wkb
from urllib3.util.retry import Retry

DEFAULT_TASK_LIMIT = 10000


def simplified(name: str) -> str:
    """Slugify *name*: lowercase, non-alphanumerics collapsed to underscores."""
    formatted = name.lower()
    formatted = re.sub(r"[^a-zA-Z0-9]+", "_", formatted)
    return formatted.strip("_")


def _retrying_client(api_cls: type, configuration: "maproulette.Configuration"):
    """Build a MapRoulette API client whose session retries transient gateway errors.

    The MapRoulette gateway intermittently returns 502 (bad gateway) and 524
    (origin timeout) under load. urllib3 retries the request up to 3 times
    (backoff 2s, 4s, 8s) at the transport layer.
    """
    # Retry all methods incl. non-idempotent POST/PUT: a gateway error may mean the
    # origin already applied the change, but MapRoulette upserts are name-keyed so a
    # replay is safe. Add 503/504 here if they start showing up too.
    client = api_cls(configuration)
    retry = Retry(
        total=3,
        status_forcelist=[502, 524],
        backoff_factor=2,
        allowed_methods=None,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    client.session.mount("https://", adapter)
    client.session.mount("http://", adapter)
    return client


class MapRouletteClient:
    """One MapRoulette project (and its per-location challenges), API-only."""

    def __init__(
        self,
        configuration: "maproulette.Configuration",
        name: str,
        description: str,
        common_instructions: str,
        violation_descriptions: dict,
        checkin_comment: str,
        locations: list[dict],
        output_bucket: str,
        keywords: list[str] | None = None,
        admins: list[str] | None = None,
        limit: int = DEFAULT_TASK_LIMIT,
    ):
        """
        Args:
            configuration: MapRoulette API credentials/host.
            name: Project display name.
            description: Project and challenge description.
            common_instructions: Appended to each task's instruction, after
                the per-violation ``violation_description``.
            violation_descriptions: Violation name -> {context kind ->
                instruction template} (or a single default template), used
                by `build_feature_collection` to render each task's
                instruction.
            checkin_comment: Prefix for each task's changeset comment.
            locations: One challenge is created per location, each a dict
                with at least a ``name`` key.
            output_bucket: S3 bucket hosting the published
                ``maproulette/{simple_name}/{location}.json`` GeoJSON that
                challenges point ``remote_geojson`` at. Publishing that
                GeoJSON to the bucket is the caller's responsibility.
            keywords: Extra challenge keywords, in addition to
                ``["osm_checks", simplified(name)]``.
            admins: Usernames granted admin on the project. No default --
                callers own their admin roster.
            limit: Max tasks per challenge, highest severity first.
        """
        self.name = name
        self.simple_name = simplified(name)
        self.description = description
        self.common_instructions = common_instructions
        self.violation_descriptions = violation_descriptions
        self.checkin_comment = checkin_comment
        self.locations = locations
        self.output_bucket = output_bucket
        self.keywords = ["osm_checks", self.simple_name] + (keywords or [])
        self.admins = admins or []
        self.limit = limit

        self._configuration = configuration
        self.__project_api: Any = None
        self.__challenge_api: Any = None
        self.__user_api: Any = None

    @property
    def _project_api(self):
        if self.__project_api is None:
            self.__project_api = _retrying_client(
                maproulette.Project, self._configuration
            )
        return self.__project_api

    @_project_api.setter
    def _project_api(self, value):
        self.__project_api = value

    @property
    def _challenge_api(self):
        if self.__challenge_api is None:
            self.__challenge_api = _retrying_client(
                maproulette.Challenge, self._configuration
            )
        return self.__challenge_api

    @_challenge_api.setter
    def _challenge_api(self, value):
        self.__challenge_api = value

    @property
    def _user_api(self):
        if self.__user_api is None:
            self.__user_api = _retrying_client(maproulette.User, self._configuration)
        return self.__user_api

    @_user_api.setter
    def _user_api(self, value):
        self.__user_api = value

    def _remote_geojson_url(self, location: dict) -> str:
        return (
            f"https://{self.output_bucket}.s3.us-west-2.amazonaws.com/maproulette/"
            f"{self.simple_name}/{simplified(location['name'])}.json"
        )

    def _challenge_name(self, location: dict) -> str:
        return f"{location['name']} - {self.name}"

    # ── Project + admins ────────────────────────────────────────────────────

    def _get_or_create_project(self) -> int:
        try:
            project = self._project_api.get_project_by_name(self.simple_name)
        except NotFoundError:
            print(f"Creating project {self.name}")
            project = self._project_api.create_project(
                data={
                    "name": self.simple_name,
                    "description": self.description,
                    "displayName": self.name,
                }
            )
        else:
            print(f"Updating project {self.name}")
            self._project_api.update_project(
                project_id=project["data"]["id"],
                data={"description": self.description},
            )
        return project["data"]["id"]

    @staticmethod
    def _get_project_manager_ids(user_api, project_id: int) -> set[int]:
        """Return the MapRoulette user IDs already managing the project.

        The python client has no wrapper for this, so we hit the raw endpoint:
        GET /user/project/{id} -> list of ProjectManager objects keyed by userId.
        """
        managers = user_api.get(endpoint=f"/user/project/{project_id}").get("data", [])
        return {manager["userId"] for manager in managers}

    def _sync_project_admins(self, project_id: int) -> None:
        """Grant each configured admin a role on the project, skipping those who
        already hold one to avoid throwaway writes (and upstream load) on every run."""
        existing_manager_ids = self._get_project_manager_ids(self._user_api, project_id)
        for user_name in self.admins:
            try:
                user = self._user_api.find_user_by_username(user_name)
                if not user.get("data"):
                    print(f"User {user_name} not found, skipping")
                    continue
                user_id = user["data"][0]["id"]
                if user_id in existing_manager_ids:
                    print(
                        f"User {user_name} already manages project {self.name}, skipping"
                    )
                    continue
                self._user_api.add_user_to_project(
                    user_id=user_id,
                    project_id=project_id,
                    group_type=1,  # (1 - Admin, 2 - Write, 3 - Read)
                    is_osm_user_id="false",
                )
            except NotFoundError:
                print(f"User {user_name} not found, skipping")
                continue
            except MapRouletteBaseException as e:
                print(
                    f"Failed to add user {user_name} to project {self.name}. ({e.message})"
                )

    # ── Challenges ───────────────────────────────────────────────────────────

    def _upsert_challenge(self, project_id: int, location: dict) -> None:
        challenge_name = self._challenge_name(location)
        challenge_data = maproulette.ChallengeModel(
            name=challenge_name,
            description=self.description,
            parent=project_id,
            instruction=f"{{{{violation_description}}}} {self.common_instructions}",
            check_in_comment=f"{self.checkin_comment}: {{{{violation_description}}}}",
            osm_id_property="identifier",
            keywords=self.keywords,
            remote_geojson=self._remote_geojson_url(location),
        )
        try:
            # If this line does not throw, a challenge with this name already exists.
            challenge = self._challenge_api.get_challenge_by_name(
                project_id, challenge_name
            )
        except NotFoundError:
            print(f"Creating challenge {challenge_name}")
            self._challenge_api.create_challenge(challenge_data)
            return

        if (
            challenge["data"]["description"] != challenge_data.description
            or challenge["data"]["instruction"] != challenge_data.instruction
            or challenge["data"]["checkinComment"] != challenge_data.check_in_comment
            or challenge["data"]["remoteGeoJson"] != challenge_data.remote_geojson
        ):
            print(f"Updating challenge {challenge_name}")
            self._challenge_api.update_challenge(
                challenge["data"]["id"], challenge_data
            )
        else:
            print(f"Challenge {challenge_name} up to date")

    def update_project(self) -> int:
        """Create or update the project, sync its admins, and upsert every
        configured challenge (creating it if missing, updating it on drift).

        Returns:
            The MapRoulette project ID.
        """
        project_id = self._get_or_create_project()
        self._sync_project_admins(project_id)
        for location in self.locations:
            self._upsert_challenge(project_id, location)
        return project_id

    def rebuild_challenge(self, location: dict, purge: bool = True) -> None:
        """Rebuild a single challenge's tasks from its ``remote_geojson``.

        Args:
            location: The location whose challenge to rebuild.
            purge: Whether to remove unmatched tasks (``remove_unmatched``).
        """
        project = self._project_api.get_project_by_name(self.simple_name)
        project_id = project["data"]["id"]
        challenge_name = self._challenge_name(location)
        print(f"Rebuilding {challenge_name}")
        challenge = self._challenge_api.get_challenge_by_name(
            project_id, challenge_name
        )
        try:
            self._challenge_api.rebuild_challenge(
                challenge_id=challenge["data"]["id"],
                remove_unmatched=purge,
                skip_snapshot=True,
            )
        except HttpError as e:
            if e.status == 502:
                print(
                    f"Rebuild triggered for {challenge_name} but server returned 502 "
                    f"(rebuild is likely still running). Treating as success."
                )
            else:
                raise
        except InvalidJsonError as e:
            # MapRoulette returns 400 "Task build is already in progress" when a rebuild
            # is still running upstream; same in-progress condition as the 502 above.
            # Match str(e): .message holds the parsed string, but the dict repr is what
            # surfaces in tracebacks, so this is robust to either form.
            if e.status == 400 and "already in progress" in str(e).lower():
                print(
                    f"Rebuild already in progress for {challenge_name}. Treating as success."
                )
            else:
                raise
        # TODO Archive challenges if no remaining tasks (swagger api only)

    def delete_project(self) -> None:
        """Delete the project and all of its challenges.

        Does not touch the published GeoJSON in ``output_bucket``: cleaning
        that up needs cloud-provider access this client intentionally doesn't
        have, so it stays the caller's responsibility.
        """
        try:
            project = self._project_api.get_project_by_name(self.simple_name)
        except NotFoundError:
            print(f"Project {self.name} not found")
            return

        project_id = project["data"]["id"]
        self._project_api.delete_project(project_id=project_id, immediate="true")
        for location in self.locations:
            challenge_name = self._challenge_name(location)
            print(f"deleting challenge {challenge_name}")
            try:
                challenge = self._challenge_api.get_challenge_by_name(
                    project_id, challenge_name
                )
                self._challenge_api.delete_challenge(
                    challenge_id=challenge["data"]["id"], immediate="true"
                )
            except NotFoundError:
                print(f"Challenge {challenge_name} not found, skipping")
                continue
            except MapRouletteBaseException as e:
                print(f"Failed to delete challenge {challenge_name} ({e.message})")

    # ── GeoJSON ──────────────────────────────────────────────────────────────

    def build_feature_collection(self, df: Any) -> geojson.FeatureCollection:
        """Convert violation rows into the challenge's GeoJSON task collection.

        Args:
            df: A pandas DataFrame of violation rows with columns ``id``,
                ``type``, ``flag_id``, ``severity``, ``violation_name``,
                ``context``, ``tags``, and ``geometry`` (WKB).
        """
        return geojson.FeatureCollection(self._build_features(df))

    def _build_features(self, df: Any) -> list[geojson.Feature]:
        features = []
        sorted_df = df.sort_values(by="severity", ascending=False).head(self.limit)

        for _, row in sorted_df.iterrows():
            osm_id = f"{row['type'][0]}{row['id']}"
            vdescs = self.violation_descriptions[row["violation_name"]]
            if (
                isinstance(row["context"], str)
                and json.loads(row["context"]).get("kind") in vdescs
            ):
                description = Template(vdescs[json.loads(row["context"]).get("kind")])
            else:
                description = Template(list(vdescs.values())[0])

            properties = {
                "identifier": row["flag_id"],
                "osmIdentifier": osm_id,
                "priority": row["severity"],
                "violation": row["violation_name"],
            }
            properties.update(dict(row["tags"]))
            if isinstance(row["context"], str):
                properties.update(json.loads(row["context"]))

            properties["violation_description"] = (
                f"{osm_id} {description.safe_substitute(properties)}"
            )

            # Replace NaN/Inf values that are not JSON-serializable.
            for k, v in properties.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    properties[k] = None

            features.append(
                geojson.Feature(
                    properties=properties,
                    geometry=wkb.loads(row["geometry"]),
                    priority=row["severity"],
                )
            )
        return features
