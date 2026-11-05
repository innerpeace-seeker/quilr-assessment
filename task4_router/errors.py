from fastapi.responses import JSONResponse


class GatewayError(Exception):
    """An error that is safe to show the client. Everything else becomes a plain 500."""

    def __init__(self, status: int, type_: str, code: str, message: str, headers: dict | None = None):
        super().__init__(message)
        self.status = status
        self.type = type_
        self.code = code
        self.message = message
        self.headers = headers or {}


def error_body(type_: str, code: str, message: str, request_id: str) -> dict:
    return {"error": {"type": type_, "code": code, "message": message, "request_id": request_id}}


def error_response(err: GatewayError, request_id: str) -> JSONResponse:
    return JSONResponse(error_body(err.type, err.code, err.message, request_id), status_code=err.status,
                        headers={**err.headers, "x-request-id": request_id})


def internal_error(request_id: str) -> JSONResponse:
    return error_response(GatewayError(500, "internal_error", "internal_error",
                                       "The gateway hit an unexpected error"), request_id)
