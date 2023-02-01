"""
Implementation of connection by the Python DBAPI 2.0 as described in
https://www.python.org/dev/peps/pep-0249/ .

"""

from cli.upsolver.query import RestQueryApi
from cli.upsolver.requester import Requester
from cli.upsolver.auth_filler import TokenAuthFiller

from upsolverapi.dbapi.utils import logger, get_duration_in_seconds, check_closed, DBAPIResponsePoller
from upsolverapi.dbapi.exceptions import NotSupportedError, InterfaceError
from upsolverapi.dbapi.cursor import Cursor


def connect(token, api_url):
    logger.debug(f"pep249 Creating connection for object ")
    return Connection(token, api_url)


class Connection:
    """A PEP 249 compliant Connection protocol."""

    def __init__(self, token, api_url, timeout_sec='60s'):
        self._api = RestQueryApi(
            requester=Requester(
                base_url=api_url,
                auth_filler=TokenAuthFiller(token)
            ),
            poller_builder=lambda to_sec: DBAPIResponsePoller(max_time_sec=to_sec)
        )

        self._timeout = get_duration_in_seconds(timeout_sec)
        self._closed = False

    @check_closed
    def cursor(self):
        logger.debug(f"pep249 Cursor creating for object {self.__class__.__name__}")

        if self._api is None:
            raise InterfaceError

        return Cursor(self)

    @check_closed
    def close(self) -> None:
        logger.debug(f"pep249 close {self.__class__.__name__}")
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def commit(self):
        raise NotSupportedError

    def rollback(self):
        raise NotSupportedError

    @check_closed
    def query(self, command):
        logger.debug(f"pep249 Execute query")
        if self._api is None:
            raise InterfaceError

        return self._api.execute(command, self._timeout)
