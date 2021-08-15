class FlowmasterExeception(Exception):
    ...


class FatalError(FlowmasterExeception):
    ...


class AuthError(FatalError):
    ...


class ForbiddenError(AuthError):
    ...


class PermissionError(AuthError):
    ...
