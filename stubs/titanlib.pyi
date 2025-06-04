from typing import Sequence
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
            /,
    ) -> None: ...

    def get_nearest_neighbour(
            self,
            lat: float,
            lon: float,
            include_match: bool = True,
            /,
    ) -> int: ...

    def get_neighbours(
            self,
            lat: float,
            lon: float,
            radius: float,
            include_match: bool = True,
            /,
        ) -> npt.NDArray[np.integer]: ...

    def get_neighbours_with_distance(
            self,
            lat: float,
            lon: float,
            radius: float,
            include_match: bool = True,
            /,
        ) -> npt.NDArray[np.integer]: ...

    def get_num_neighbours(
            self,
            lat: float,
            lon: float,
            radius: float,
            include_match: bool = True,
            /,
        ) -> npt.NDArray[np.integer]: ...

    def get_closest_neighbours(
            self,
            lat: float,
            lon: float,
            radius: float,
            include_match: bool = True,
            /,
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
        /,
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
        /,
) -> npt.NDArray[np.integer]: ...


def isolation_check(
        points: Points,
        num_min: npt.NDArray[np.integer] | int,
        radius: npt.NDArray[T] | float,
        vertical_radius: npt.NDArray[T] | float = float('nan'),
        /,
) -> npt.NDArray[np.integer]: ...


def range_check_climatology(
        points: Points,
        values: npt.NDArray[T],
        unixtime: int,
        pos: npt.NDArray[T],
        neg: npt.NDArray[T],
        /,
) -> npt.NDArray[np.integer]: ...


def metadata_check(
        points: Points,
        check_lat: bool = True,
        check_lon: bool = True,
        check_elev: bool = True,
        check_laf: bool = True,
) -> npt.NDArray[np.integer]: ...

def range_check(
        values: npt.NDArray[T],
        min: npt.NDArray[T],
        max: npt.NDArray[T],
) -> npt.NDArray[np.integer]: ...

def sct(
        points: Points,
        values: npt.NDArray[T],
        num_min: int,
        num_max: int,
        inner_radius: float,
        outer_radius: float,
        num_iterations: int,
        num_min_prof: int,
        min_elev_diff: float,
        min_horizontal_scale: float,
        vertical_scale: float,
        pos: npt.NDArray[T],
        neg: npt.NDArray[T],
        eps2: npt.NDArray[T],
        prob_gross_error: npt.NDArray[T],
        rep: npt.NDArray[T],
        obs_to_check: Sequence[int] = [],
) -> npt.NDArray[np.integer]: ...



def sct_resistant(
        points: Points,
        values: npt.NDArray[T],
        obs_to_check: Sequence[int],
        background_values: npt.NDArray[T],
        background_elab_type: str,
        num_min_outer: int,
        num_max_outer: int,
        inner_radius: float,
        outer_radius: float,
        num_iterations: int,
        num_min_prof: int,
        min_elev_diff: float,
        min_horizontal_scale: float,
        max_horizontal_scale: float,
        kth_closest_obs_horizontal_scale: int,
        vertical_scale: float,
        values_mina: npt.NDArray[T],
        values_maxa: npt.NDArray[T],
        values_minv: npt.NDArray[T],
        values_maxv: npt.NDArray[T],
        eps2: npt.NDArray[T],
        tpos: npt.NDArray[T],
        tneg: npt.NDArray[T],
        debug: bool,
        basic: bool,
) -> npt.NDArray[np.integer]: ...


def sct_dual(
        points: Points,
        values: npt.NDArray[T],
        obs_to_check: Sequence[int],
        event_thresholds: npt.NDArray[T],
        condition: str,
        num_min_outer: int,
        num_max_outer: int,
        inner_radius: float,
        outer_radius: float,
        num_iterations: int,
        min_horizontal_scale: float,
        max_horizontal_scale: float,
        kth_closest_obs_horizontal_scale: int,
        vertical_scale: float,
        test_thresholds: npt.NDArray[T],
        debug: bool,
) -> npt.NDArray[np.integer]: ...
