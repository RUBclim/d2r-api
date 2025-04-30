import functools
from typing import Any

import pytest
from terracotta.drivers import TerracottaDriver

from app.tc_ingester import _RasterKeys
from app.tc_ingester import ingest_raster
from app.tc_ingester import InvalidRasterError


UTCI_CLASSES = {
    '0': 'extreme cold stress',
    '1': 'very strong cold stress',
    '2': 'strong cold stress',
    '3': 'moderate cold stress',
    '4': 'slight cold stress',
    '5': 'no thermal stress',
    '6': 'moderate heat stress',
    '7': 'strong heat stress',
    '8': 'very strong heat stress',
    '9': 'extreme heat stress',
}

PET_CLASSES = {
    '0': 'extreme cold stress',
    '1': 'strong cold stress',
    '2': 'moderate cold stress',
    '3': 'slight cold stress',
    '4': 'no thermal stress',
    '5': 'slight heat stress',
    '6': 'moderate heat stress',
    '7': 'strong heat stress',
    '8': 'extreme heat stress',
}


@pytest.mark.parametrize(
    ('fname', 'expected'),
    (
        # DO MRT
        (
            'DO_MRT_2024_122_00_v0.7.0_cog.tif',
            _RasterKeys(
                param='MRT',
                year='2024',
                doy='122',
                hour='00',
                city='DO',
                version='v0.7.0',
            ),
        ),
        (
            'DO_MRT_2024_122_00_v0.7.0.tif',
            _RasterKeys(
                param='MRT',
                year='2024',
                doy='122',
                hour='00',
                city='DO',
                version='v0.7.0',
            ),
        ),
        (
            'DO_Tmrt_3m_v0.7.0_2024_244_15.tif',
            _RasterKeys(
                param='MRT',
                year='2024',
                doy='244',
                hour='15',
                city='DO',
                version='v0.7.0',
                resolution='3m',
            ),
        ),
        # PET
        (
            'DO_PET_2024_209_16_v0.7.0_cog.tif',
            _RasterKeys(
                param='PET',
                year='2024',
                doy='209',
                hour='16',
                city='DO',
                version='v0.7.0',
            ),
        ),
        (
            'DO_PET_2024_209_16_v0.7.0.tif',
            _RasterKeys(
                param='PET',
                year='2024',
                doy='209',
                hour='16',
                city='DO',
                version='v0.7.0',
            ),
        ),
        (
            'PET_umep_3m_v0.7.0_2024_209_16.tif',
            _RasterKeys(
                param='PET',
                year='2024',
                doy='209',
                hour='16',
                method='umep',
                resolution='3m',
                version='v0.7.0',
            ),
        ),
        # PET_CLASS
        (
            'DO_PET-class_2025_027_04_v0.7.0_cog.tif',
            _RasterKeys(
                param='PET_CLASS',
                year='2025',
                doy='027',
                hour='04',
                city='DO',
                version='v0.7.0',
                is_categorical=True,
                categories=PET_CLASSES,
            ),
        ),
        (
            'DO_PET-class_2025_027_04_v0.7.0.tif',
            _RasterKeys(
                param='PET_CLASS',
                year='2025',
                doy='027',
                hour='04',
                city='DO',
                version='v0.7.0',
                is_categorical=True,
                categories=PET_CLASSES,
            ),
        ),
        # RH
        (
            'DO_RH_2024_349_09_v0.7.0_cog.tif',
            _RasterKeys(
                param='RH',
                year='2024',
                doy='349',
                hour='09',
                city='DO',
                version='v0.7.0',
            ),
        ),
        # TA
        (
            'DO_TA_2025_022_22_v0.7.0_cog.tif',
            _RasterKeys(
                param='TA',
                year='2025',
                doy='022',
                hour='22',
                city='DO',
                version='v0.7.0',
            ),
        ),
        # UTCI
        (
            'DO_UTCI_2025_021_09_v0.7.0.tif',
            _RasterKeys(
                param='UTCI',
                year='2025',
                doy='021',
                hour='09',
                city='DO',
                version='v0.7.0',
            ),
        ),
        (
            'DO_UTCI_2025_021_09_v0.7.0_cog.tif',
            _RasterKeys(
                param='UTCI',
                year='2025',
                doy='021',
                hour='09',
                city='DO',
                version='v0.7.0',
            ),
        ),
        (
            'DO_UTCI-class_2025_021_09_v0.7.0.tif',
            _RasterKeys(
                param='UTCI_CLASS',
                year='2025',
                doy='021',
                hour='09',
                city='DO',
                version='v0.7.0',
                is_categorical=True,
                categories=UTCI_CLASSES,
            ),
        ),
        (
            'DO_UTCI-class_2025_021_09_v0.7.0_cog.tif',
            _RasterKeys(
                param='UTCI_CLASS',
                year='2025',
                doy='021',
                hour='09',
                city='DO',
                version='v0.7.0',
                is_categorical=True,
                categories=UTCI_CLASSES,
            ),
        ),
        (
            'UTCI_pytherm_3m_v0.7.0_2024_243_15.tif',
            _RasterKeys(
                param='UTCI',
                year='2024',
                doy='243',
                hour='15',
                version='v0.7.0',
                method='pytherm',
                resolution='3m',
            ),
        ),
    ),
)
def test_regex_matches_filenames(fname: str, expected: _RasterKeys) -> None:
    assert _RasterKeys.from_string(fname) == expected


@pytest.mark.parametrize(
    'fname',
    ('2024_122_00_v0.7.0_cog.tif', 'DO_MRT_2024_122_00.tif'),
)
def test_raster_key_file_name_does_not_match(fname: str) -> None:
    with pytest.raises(ValueError) as excinfo:
        _RasterKeys.from_string(fname)
    assert excinfo.value.args[0] == (
        f'Filename {fname} does not match the expected pattern'
    )


def test_raster_key_file_invalid_param_values() -> None:
    with pytest.raises(ValueError) as excinfo:
        _RasterKeys.from_string('DO_UNKNOWN_2025_021_09_v0.7.0.tif')
    assert excinfo.value.args[0] == 'Invalid param value UNKNOWN'


@pytest.mark.parametrize(
    'fname',
    ('UTCI_pytherm_3m_v0.6.0_2024_177_23.tif', 'DO_UTCI-class_2025_096_14_v0.7.0.tif'),
)
def test_ingest_raster_errors_in_file(
        fname: str,
        raster_driver: TerracottaDriver,
) -> None:
    with pytest.raises(InvalidRasterError) as excinfo:
        ingest_raster(path=f'testing/rasters/{fname}')
    assert excinfo.value.args[0] == (
        f'Invalid raster file testing/rasters/{fname}'
    )
    # make sure it was not inserted
    datasets = raster_driver.meta_store.get_datasets()
    assert datasets == {}


apx = functools.partial(pytest.approx, abs=1e-1)


@pytest.mark.parametrize(
    ('param', 'fname', 'expected_metadata'),
    (
        (
            'UTCI',
            'DO_UTCI_2025_113_12_v0.7.2_cog.tif',
            {
                'categories': None,
                'city': 'DO',
                'doy': '113',
                'hour': '12',
                'is_categorical': False,
                'method': None,
                'param': 'UTCI',
                'resolution': None,
                'version': 'v0.7.2',
                'visualization': {'cmax': apx(28.47), 'cmin': apx(9.57)},
                'year': '2025',
            },
        ),
        (
            'UTCI_CLASS',
            'DO_UTCI-class_2025_113_12_v0.7.2_cog.tif',
            {
                'categories': {
                    '0': 'extreme cold stress',
                    '1': 'very strong cold stress',
                    '2': 'strong cold stress',
                    '3': 'moderate cold stress',
                    '4': 'slight cold stress',
                    '5': 'no thermal stress',
                    '6': 'moderate heat stress',
                    '7': 'strong heat stress',
                    '8': 'very strong heat stress',
                    '9': 'extreme heat stress',
                },
                'city': 'DO',
                'doy': '113',
                'hour': '12',
                'is_categorical': True,
                'method': None,
                'param': 'UTCI_CLASS',
                'resolution': None,
                'version': 'v0.7.2',
                'visualization': {'cmax': 5, 'cmin': 5},
                'year': '2025',
            },
        ),
        (
            'PET',
            'DO_PET_2025_113_12_v0.7.2_cog.tif',
            {
                'categories': None,
                'city': 'DO',
                'doy': '113',
                'hour': '12',
                'is_categorical': False,
                'method': None,
                'param': 'PET',
                'resolution': None,
                'version': 'v0.7.2',
                'visualization': {'cmax': apx(26.4), 'cmin': apx(5.4)},
                'year': '2025',
            },
        ),
        (
            'PET_CLASS',
            'DO_PET-class_2025_113_12_v0.7.2_cog.tif',
            {
                'categories': {
                    '0': 'extreme cold stress',
                    '1': 'strong cold stress',
                    '2': 'moderate cold stress',
                    '3': 'slight cold stress',
                    '4': 'no thermal stress',
                    '5': 'slight heat stress',
                    '6': 'moderate heat stress',
                    '7': 'strong heat stress',
                    '8': 'extreme heat stress',
                },
                'city': 'DO',
                'doy': '113',
                'hour': '12',
                'is_categorical': True,
                'method': None,
                'param': 'PET_CLASS',
                'resolution': None,
                'version': 'v0.7.2',
                'visualization': {'cmax': 4, 'cmin': 3},
                'year': '2025',
            },
        ),
        (
            'TA',
            'DO_TA_2025_113_12_v0.7.2_cog.tif',
            {
                'categories': None,
                'city': 'DO',
                'doy': '113',
                'hour': '12',
                'is_categorical': False,
                'method': None,
                'param': 'TA',
                'resolution': None,
                'version': 'v0.7.2',
                'visualization': {'cmax': apx(292.86), 'cmin': apx(286.86)},
                'year': '2025',
            },
        ),
        (
            'MRT',
            'DO_MRT_2025_113_12_v0.7.2_cog.tif',
            {
                'categories': None,
                'city': 'DO',
                'doy': '113',
                'hour': '12',
                'is_categorical': False,
                'method': None,
                'param': 'MRT',
                'resolution': None,
                'version': 'v0.7.2',
                'visualization': {'cmax': apx(39.25), 'cmin': apx(22.004)},
                'year': '2025',
            },
        ),
        (
            'RH',
            'DO_RH_2025_113_12_v0.7.2_cog.tif',
            {
                'categories': None,
                'city': 'DO',
                'doy': '113',
                'hour': '12',
                'is_categorical': False,
                'method': None,
                'param': 'RH',
                'resolution': None,
                'version': 'v0.7.2',
                'visualization': {'cmax': apx(68), 'cmin': apx(51.2)},
                'year': '2025',
            },
        ),
    ),
)
def test_ingest_raster_metadata_computed_correctly_categorized_raster(
        fname: str,
        param: str,
        expected_metadata: dict[str, Any],
        raster_driver: TerracottaDriver,
) -> None:
    ingest_raster(path=f'testing/rasters/{fname}')
    datasets = raster_driver.get_datasets()
    assert datasets == {(param, '2025', '113', '12'): fname}
    # check custom metadata
    metadata = raster_driver.get_metadata(keys=(param, '2025', '113', '12'))
    custom_metadata = metadata['metadata']
    assert custom_metadata == expected_metadata
