from typing import Any, Callable, List, Type, cast, Optional, Union

from . import CRUDGenerator, NOT_FOUND
from ._types import DEPENDENCIES, PAGINATION, PYDANTIC_SCHEMA as SCHEMA

try:
    from motor.motor_asyncio import AsyncIOMotorClient
    motor_installed = True
    NoMatch = None  # type: ignore

except ImportError:
    NoMatch = None  # type: ignore
    motor_installed = False

CALLABLE = Callable[..., SCHEMA]
CALLABLE_LIST = Callable[..., List[SCHEMA]]


class MotorCRUDRouter(CRUDGenerator[SCHEMA]):
    def __init__(
        self,
        schema: Type[SCHEMA],
        db_url: str = "mongodb://localhost",
        database: str = "db",
        collection: str = "collection",
        create_schema: Optional[Type[SCHEMA]] = None,
        update_schema: Optional[Type[SCHEMA]] = None,
        prefix: Optional[str] = None,
        tags: Optional[List[str]] = None,
        paginate: Optional[int] = None,
        get_all_route: Union[bool, DEPENDENCIES] = True,
        get_one_route: Union[bool, DEPENDENCIES] = True,
        create_route: Union[bool, DEPENDENCIES] = True,
        update_route: Union[bool, DEPENDENCIES] = True,
        delete_one_route: Union[bool, DEPENDENCIES] = True,
        delete_all_route: Union[bool, DEPENDENCIES] = True,
        **kwargs: Any
    ) -> None:
        self.db_url = db_url
        self.database = database
        self.collection = collection
        self.client = AsyncIOMotorClient(
            self.db_url, uuidRepresentation="standard"
        )
        self.db = self.client[self.database]

        super().__init__(
            schema=schema,
            create_schema=create_schema,
            update_schema=update_schema,
            prefix=prefix,
            tags=tags,
            paginate=paginate,
            get_all_route=get_all_route,
            get_one_route=get_one_route,
            create_route=create_route,
            update_route=update_route,
            delete_one_route=delete_one_route,
            delete_all_route=delete_all_route,
            **kwargs
        )

        self.models: List[SCHEMA] = []

    def _get_all(self, *args: Any, **kwargs: Any) -> CALLABLE_LIST:
        async def route(pagination: PAGINATION = self.pagination) -> List[SCHEMA]:
            skip, limit = pagination.get("skip"), pagination.get("limit")
            skip = cast(int, skip)
            limit = limit or 100

            return await self.db[self.collection].find({}).skip(skip).limit(limit).to_list(length = limit)

        return route
