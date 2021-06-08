class FlowmasterExeception(Exception):
    ...


class FatalError(FlowmasterExeception):
    ...


class AuthError(FatalError):
    ...
