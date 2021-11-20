from typing import Any, Callable, List, Type, cast, Optional, Union

from . import CRUDGenerator, NOT_FOUND
from ._types import DEPENDENCIES, PAGINATION, PYDANTIC_SCHEMA as SCHEMA


try:
    from bson import ObjectId
    from motor.motor_asyncio import AsyncIOMotorClient
    from asyncio import create_task
    from beanie import init_beanie
    from beanie.odm.operators.update.general import Set

    motor_installed = True

except ImportError:
    motor_installed = False

CALLABLE = Callable[..., SCHEMA]
CALLABLE_LIST = Callable[..., List[SCHEMA]]


class MotorCRUDRouter(CRUDGenerator[SCHEMA]):
    def __init__(
        self,
        schema: Type[SCHEMA],
        client: Optional[AsyncIOMotorClient] = None,
        db_url: str = "mongodb://localhost",
        database: str = "db",
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
        assert (
            motor_installed
        ), "MotorCRUDRouter requires motor, beanie, and bson. Please install the required libraries and try again."
        self.schema = schema
        self.create_schema=create_schema
        self.update_schema=update_schema
        if client:
            self.client = client
        else:
            self.db_url = db_url
            self.client = AsyncIOMotorClient(
                self.db_url, uuidRepresentation="standard")

        self.db = self.client[database]
        task = create_task(
            init_beanie(database=self.db,
            document_models = [
                self.schema,
                self.create_schema,
                self.update_schema,
        ]))
        
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
            return await self.schema.find().skip(skip).to_list(limit)

        return route

    def _get_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(item_id: str) -> SCHEMA:
            doc = await self.schema.get(item_id)
            if not doc:
                raise NOT_FOUND
            return doc
            
        
        return route

    def _create(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(model: self.create_schema) -> SCHEMA:  # type: ignore
            return await model.create()

        return route

    def _update(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(item_id: str, model: self.update_schema) -> SCHEMA:  # type: ignore
            doc = await self.schema.get(item_id)
            if not doc:
                raise NOT_FOUND
            del model.id
            return await doc.update(Set(model))

        return route

    def _delete_all(self, *args: Any, **kwargs: Any) -> CALLABLE_LIST:
        async def route() -> List[SCHEMA]:
            return await self.schema.find().delete()

        return route

    def _delete_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(item_id: str) -> SCHEMA:
            doc = await self.schema.get(item_id)
            if not doc:
                raise NOT_FOUND
            return await doc.delete()

        return route

