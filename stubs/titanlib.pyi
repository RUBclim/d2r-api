from typing import Literal
from typing import Sequence
from typing import TypeAlias

import numpy as np
import numpy.typing as npt

type N = np.floating | np.integer

class Points:
    def __init__(
            self,
            lats: npt.NDArray[N],
            lons: npt.NDArray[N],
            elevs: npt.NDArray[N],
            lafs: npt.NDArray[N] = np.array([]),
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

    def get_lats(self) -> npt.NDArray[N]: ...

    def get_lons(self) -> npt.NDArray[N]: ...

    def get_elevs(self) -> npt.NDArray[N]: ...

    def get_lafs(self) -> npt.NDArray[N]: ...

    def size(self) -> int: ...

    def get_coordinate_type(self) -> Literal[0, 1]: ...

def buddy_check(
        points: Points,
        values: npt.NDArray[N],
        radius: npt.NDArray[N],
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
        values: npt.NDArray[N],
        radius: npt.NDArray[N],
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
        radius: npt.NDArray[N] | float,
        vertical_radius: npt.NDArray[N] | float = float('nan'),
        /,
) -> npt.NDArray[np.integer]: ...


def range_check_climatology(
        points: Points,
        values: npt.NDArray[N],
        unixtime: int,
        pos: npt.NDArray[N],
        neg: npt.NDArray[N],
        /,
) -> npt.NDArray[np.integer]: ...


def metadata_check(
        points: Points,
        check_lat: bool = True,
        check_lon: bool = True,
        check_elev: bool = True,
        check_laf: bool = True,
        /,
) -> npt.NDArray[np.integer]: ...

def range_check(
        values: npt.NDArray[N],
        min: npt.NDArray[N],
        max: npt.NDArray[N],
        /,
) -> npt.NDArray[np.integer]: ...

def sct(
        points: Points,
        values: npt.NDArray[N],
        num_min: int,
        num_max: int,
        inner_radius: float,
        outer_radius: float,
        num_iterations: int,
        num_min_prof: int,
        min_elev_diff: float,
        min_horizontal_scale: float,
        vertical_scale: float,
        pos: npt.NDArray[N],
        neg: npt.NDArray[N],
        eps2: npt.NDArray[N],
        prob_gross_error: npt.NDArray[N],
        rep: npt.NDArray[N],
        obs_to_check: Sequence[int] = [],
        /,
) -> npt.NDArray[np.integer]: ...



def sct_resistant(
        points: Points,
        values: npt.NDArray[N],
        obs_to_check: Sequence[int],
        background_values: npt.NDArray[N],
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
        values_mina: npt.NDArray[N],
        values_maxa: npt.NDArray[N],
        values_minv: npt.NDArray[N],
        values_maxv: npt.NDArray[N],
        eps2: npt.NDArray[N],
        tpos: npt.NDArray[N],
        tneg: npt.NDArray[N],
        debug: bool,
        basic: bool,
        /,
) -> npt.NDArray[np.integer]: ...


def sct_dual(
        points: Points,
        values: npt.NDArray[N],
        obs_to_check: Sequence[int],
        event_thresholds: npt.NDArray[N],
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
        test_thresholds: npt.NDArray[N],
        debug: bool,
        /,
) -> npt.NDArray[np.integer]: ...
