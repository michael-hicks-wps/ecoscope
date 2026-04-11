"""CCFN SMART Connect connection class for EcoScope Desktop."""
from __future__ import annotations

from typing import Annotated, ClassVar

from pydantic import Field, SecretStr
from pydantic.functional_validators import BeforeValidator
from pydantic.json_schema import WithJsonSchema

from ecoscope.platform.connections import DataConnection

from ._client import SMARTConnectClient


class CCFNConnection(DataConnection[SMARTConnectClient]):
    """Connection settings for the CCFN SMART Connect server.

    Credentials are read from environment variables following the EcoScope
    connection naming convention::

        ECOSCOPE_WORKFLOWS__CONNECTIONS__CCFN__<name>__SERVER
        ECOSCOPE_WORKFLOWS__CONNECTIONS__CCFN__<name>__USERNAME
        ECOSCOPE_WORKFLOWS__CONNECTIONS__CCFN__<name>__PASSWORD
        ECOSCOPE_WORKFLOWS__CONNECTIONS__CCFN__<name>__CA_UUID

    where ``<name>`` is the connection name passed to the task (e.g. ``ccfn``).
    """

    __ecoscope_connection_type__: ClassVar[str] = "ccfn"

    server: Annotated[str, Field(description="CCFN SMART Connect server URL")]
    username: Annotated[str, Field(description="CCFN username")]
    password: Annotated[SecretStr, Field(description="CCFN password")]
    ca_uuid: Annotated[str, Field(description="Conservation Area UUID")]

    def get_client(self) -> SMARTConnectClient:
        return SMARTConnectClient(
            server=self.server,
            username=self.username,
            password=self.password.get_secret_value(),
        )


CCFNConnectionParam = Annotated[
    CCFNConnection,
    BeforeValidator(CCFNConnection.from_named_connection),
    WithJsonSchema({"type": "string", "description": "A named CCFN SMART Connect connection."}),
]
