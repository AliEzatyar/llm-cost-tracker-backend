from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


def register_exception_handlers(app: FastAPI):
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request,
        exc: RequestValidationError,
    ):
        body = await request.body()

        print("=" * 80)
        print("URL:", request.url)
        print("Method:", request.method)
        print("Raw body:")
        print(body.decode(errors="replace"))
        print()

        print("Validation errors:")
        for error in exc.errors():
            print(error)

        print("=" * 80)

        return JSONResponse(
            status_code=422,
            content={
                "detail": exc.errors(),
            },
        )

