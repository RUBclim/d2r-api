import pandas as pd
import pytest

from app.qc import range_check


@pytest.mark.parametrize(
    ('data', 'lower_bound', 'upper_bound', 'expected'),
    (
        (pd.Series([2, 4, 6]), 3, 5, pd.Series([True, False, True])),
        (pd.Series([2, float('nan'), 6]), 3, 5, pd.Series([True, True, True])),
    ),
)
@pytest.mark.anyio
async def test_range_check(
        data: 'pd.Series[float]',
        lower_bound: float,
        upper_bound: float,
        expected: 'pd.Series[float]',
) -> None:
    result = await range_check(data, lower_bound=lower_bound, upper_bound=upper_bound)
    pd.testing.assert_series_equal(result, expected)
