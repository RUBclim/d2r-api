from typing import Sequence
from enum import Enum
import numpy.typing as npt
from typing import Literal
from typing import TypeVar
import numpy as np

T = TypeVar('T', bound=np.floating | np.integer)

class Points:
    def __init__(
            self,
            lats: npt.NDArray[T],
            lons: npt.NDArray[T],
            elevs: npt.NDArray[T],
            lafs: npt.NDArray[T] = np.array([]),
            type: Literal[0, 1] = 0,
    ) -> None: ...

    def get_nearest_neighbour(
            self,
            lat: float,
            lon: float,
            include_match: bool = True
    ) -> int: ...

    def get_neighbours(
            self,
            lat: float,
            lon: float,
            radius: float,
            include_match: bool = True
        ) -> npt.NDArray[np.integer]: ...

    def get_neighbours_with_distance(
            self,
            lat: float,
            lon: float,
            radius: float,
            include_match: bool = True,
        ) -> npt.NDArray[np.integer]: ...

    def get_num_neighbours(self, lat: float, lon: float, radius: float, include_match: bool = True) -> npt.NDArray[np.integer]: ...

    def get_closest_neighbours(
            self,
            lat: float,
            lon: float,
            radius: float,
            include_match: bool = True,
    ) -> npt.NDArray[np.integer]: ...

    def get_lats(self) -> npt.NDArray[T]: ...

    def get_lons(self) -> npt.NDArray[T]: ...

    def get_elevs(self) -> npt.NDArray[T]: ...

    def get_lafs(self) -> npt.NDArray[T]: ...

    def size(self) -> int: ...

    def get_coordinate_type(self) -> Literal[0, 1]: ...

def buddy_check(
        points: Points,
        values: npt.NDArray[T],
        radius: npt.NDArray[T],
        num_min: npt.NDArray[np.integer],
        threshold: float,
        max_elev_diff: float,
        elev_gradient: float,
        min_std: float,
        num_iterations: int,
        obs_to_check: Sequence[int] = [],
) -> npt.NDArray[np.integer]: ...


def buddy_event_check(
        points: Points,
        values: npt.NDArray[T],
        radius: npt.NDArray[T],
        num_min: npt.NDArray[np.integer],
        event_threshold: float,
        threshold: float,
        max_elev_diff: float,
        elev_gradient: float,
        min_std: float,
        num_iterations: int,
        obs_to_check: Sequence[int] = [],
) -> npt.NDArray[np.integer]: ...
