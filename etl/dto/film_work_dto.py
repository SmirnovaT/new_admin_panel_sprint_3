from typing import List, Optional

from pydantic import BaseModel, Field


class DirectorDto(BaseModel):
    id: str
    name: str = Field(..., alias="full_name")


class ActorDto(BaseModel):
    id: str
    name: str = Field(..., alias="full_name")


class WriterDto(BaseModel):
    id: str
    name: str = Field(..., alias="full_name")


class FilmWorkDto(BaseModel):
    fw_id: str = Field(..., alias="id")
    imdb_rating: Optional[float] = Field(..., alias="rating")
    title: str
    description: Optional[str]
    directors_names: List[str]
    writers_names: List[str]
    actors_names: List[str]
    genres: List[str]
    directors: List[DirectorDto]
    actors: List[ActorDto]
    writers: List[WriterDto]
