import pytest

from app.tc_ingester import _RasterKeys


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
