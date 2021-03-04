from functools import partial
import pydantic.dataclasses


class Config:
    arbitrary_types_allowed = True


dataclass = partial(pydantic.dataclasses.dataclass, config=Config)
