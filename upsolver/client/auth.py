from abc import ABCMeta, abstractmethod

from upsolver.client.errors import InternalErr


class AuthApi(metaclass=ABCMeta):
    @abstractmethod
    def authenticate(self, email: str, password: str):
        pass


class InvalidAuthApi(AuthApi):
    """
    This type is used to construct UpsolverApi implementations that are not expected to be able
    to authenticate users. Since authentication and other api actions are currently separated,
    it marks a logical error to authenticate outside configuration (i.e. auth token retrieval)
    phase.
    """
    def authenticate(self, email: str, password: str):
        raise InternalErr('This is a mistake, fix me.')
