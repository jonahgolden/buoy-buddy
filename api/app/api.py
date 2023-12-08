from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse
from . import buoys, php

# class ORJSONResponse(JSONResponse):
#     media_type = "application/json"

#     def render(self, content: typing.Any) -> bytes:
#         return orjson.dumps(content)

app = FastAPI()

origins = [
    "http://localhost:3000",
    "localhost:3000"
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.get("/")
def main():
    return RedirectResponse(url="/docs/")


app.include_router(
    buoys.router,
    prefix="/buoys",
    tags=["buoys"],
    # dependencies=[Depends(get_token_header)],
    # responses={404: {"description": "Not found"}},
)

app.include_router(
    php.router,
    prefix="/stations",
    tags=["stations"],
)