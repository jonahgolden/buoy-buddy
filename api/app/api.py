from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .buoy import Buoy
from . import buoys

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


@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Welcome to Buoy Buddy."}

# @app.get("/buoys/")
# async def get_buoys():
#     return Buoy.get_buoys()

app.include_router(
    buoys.router,
    prefix="/buoys",
    tags=["buoys"],
    # dependencies=[Depends(get_token_header)],
    # responses={404: {"description": "Not found"}},
)