from typing import Any, Callable, List, Type, cast, Optional, Union

from . import CRUDGenerator, NOT_FOUND
from ._types import DEPENDENCIES, PAGINATION, PYDANTIC_SCHEMA as SCHEMA


try:
    from bson import ObjectId
    from motor.motor_asyncio import AsyncIOMotorClient
    from odmantic import AIOEngine
    motor_installed = True

except ImportError:
    motor_installed = False

CALLABLE = Callable[..., SCHEMA]
CALLABLE_LIST = Callable[..., List[SCHEMA]]


class MotorCRUDRouter(CRUDGenerator[SCHEMA]):
    def __init__(
        self,
        schema: Type[SCHEMA],
        engine: Optional[AIOEngine],
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
        ), "MotorCRUDRouter requires motor, odmantic, and bson. Please install the required libraries and try again."
        self.schema = schema
        if engine:
            self.engine = engine
        else:
            self.db_url = db_url
            self.database = database
            self.client = AsyncIOMotorClient(
                self.db_url, uuidRepresentation="standard")
            self.engine = AIOEngine(motor_client=self.client, database=self.database)
        
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
            docs = await self.engine.find(self.schema, {}, skip=skip, limit=limit)
            if not docs:
                raise NOT_FOUND
            return docs

        return route

    def _get_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(item_id: str) -> SCHEMA:
            doc = await self.engine.find_one(self.schema, {"_id": ObjectId(item_id)})
            if not doc:
                raise NOT_FOUND
            return doc
        
        return route

    def _create(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(model: self.create_schema) -> SCHEMA:  # type: ignore
            doc = await self.engine.save(model)
            if not doc:
                raise NOT_FOUND
            return doc

        return route

    def _update(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(item_id: str, model: self.update_schema) -> SCHEMA:  # type: ignore
            doc = await self.engine.find_one(self.schema, self.schema.id == ObjectId(item_id))
            if not doc:
                raise NOT_FOUND
            patch_dict = model.dict(exclude_unset=True)
            for name, value in patch_dict.items():
                setattr(doc, name, value)
            return await self.engine.save(doc)

        return route

    def _delete_all(self, *args: Any, **kwargs: Any) -> CALLABLE_LIST:
        async def route() -> List[SCHEMA]:
            docs = await self.engine.find(self.schema, {})
            for doc in docs:
                await self.engine.delete(doc)

        return route

    def _delete_one(self, *args: Any, **kwargs: Any) -> CALLABLE:
        async def route(item_id: str) -> SCHEMA:
            doc = await self.engine.find_one(self.schema, self.schema.id == ObjectId(item_id))
            if not doc:
                raise NOT_FOUND
            return await self.engine.delete(doc)

        return route

