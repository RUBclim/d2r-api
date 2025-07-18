"""qc_score

Revision ID: 360e9646b9d2
Revises: cc38bcd8171d
Create Date: 2025-07-17 19:13:09.076312

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '360e9646b9d2'
down_revision: str | None = 'cc38bcd8171d'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

former_creation_sql = '''\
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_data AS
(
    SELECT DISTINCT ON (station_id)
        biomet_data.station_id,
        long_name,
        latitude,
        longitude,
        altitude,
        district,
        lcz,
        station_type,
        biomet_data.measured_at,
        air_temperature,
        relative_humidity,
        dew_point,
        absolute_humidity,
        specific_humidity,
        heat_index,
        wet_bulb_temperature,
        atmospheric_pressure,
        atmospheric_pressure_reduced,
        lightning_average_distance,
        lightning_strike_count,
        mrt,
        pet,
        pet_category,
        precipitation_sum,
        solar_radiation,
        utci,
        utci_category,
        vapor_pressure,
        wind_direction,
        wind_speed,
        maximum_wind_speed,
        u_wind,
        v_wind,
        sensor_temperature_internal,
        x_orientation_angle,
        y_orientation_angle,
        black_globe_temperature,
        thermistor_resistance,
        voltage_ratio,
        air_temperature_qc_range_check,
        air_temperature_qc_persistence_check,
        air_temperature_qc_spike_dip_check,
        relative_humidity_qc_range_check,
        relative_humidity_qc_persistence_check,
        relative_humidity_qc_spike_dip_check,
        atmospheric_pressure_qc_range_check,
        atmospheric_pressure_qc_persistence_check,
        atmospheric_pressure_qc_spike_dip_check,
        wind_speed_qc_range_check,
        wind_speed_qc_persistence_check,
        wind_speed_qc_spike_dip_check,
        wind_direction_qc_range_check,
        wind_direction_qc_persistence_check,
        u_wind_qc_range_check,
        u_wind_qc_persistence_check,
        u_wind_qc_spike_dip_check,
        v_wind_qc_range_check,
        v_wind_qc_persistence_check,
        v_wind_qc_spike_dip_check,
        maximum_wind_speed_qc_range_check,
        maximum_wind_speed_qc_persistence_check,
        precipitation_sum_qc_range_check,
        precipitation_sum_qc_persistence_check,
        precipitation_sum_qc_spike_dip_check,
        solar_radiation_qc_range_check,
        solar_radiation_qc_persistence_check,
        solar_radiation_qc_spike_dip_check,
        lightning_average_distance_qc_range_check,
        lightning_average_distance_qc_persistence_check,
        lightning_strike_count_qc_range_check,
        lightning_strike_count_qc_persistence_check,
        x_orientation_angle_qc_range_check,
        x_orientation_angle_qc_spike_dip_check,
        y_orientation_angle_qc_range_check,
        y_orientation_angle_qc_spike_dip_check,
        black_globe_temperature_qc_range_check,
        black_globe_temperature_qc_persistence_check,
        black_globe_temperature_qc_spike_dip_check,
        qc_flagged,
        air_temperature_qc_isolated_check,
        air_temperature_qc_buddy_check,
        relative_humidity_qc_isolated_check,
        relative_humidity_qc_buddy_check,
        atmospheric_pressure_qc_isolated_check,
        atmospheric_pressure_qc_buddy_check,
        battery_voltage,
        protocol_version
    FROM biomet_data
        INNER JOIN station ON biomet_data.station_id = station.station_id
        LEFT OUTER JOIN buddy_check_qc ON (
            biomet_data.station_id = buddy_check_qc.station_id AND
            biomet_data.measured_at = buddy_check_qc.measured_at
        )
    ORDER BY biomet_data.station_id, biomet_data.measured_at DESC
)
UNION ALL
(
    SELECT DISTINCT ON (station_id)
        temp_rh_data.station_id,
        long_name,
        latitude,
        longitude,
        altitude,
        district,
        lcz,
        station_type,
        temp_rh_data.measured_at,
        air_temperature,
        relative_humidity,
        dew_point,
        absolute_humidity,
        specific_humidity,
        heat_index,
        wet_bulb_temperature,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        air_temperature_qc_range_check,
        air_temperature_qc_persistence_check,
        air_temperature_qc_spike_dip_check,
        relative_humidity_qc_range_check,
        relative_humidity_qc_persistence_check,
        relative_humidity_qc_spike_dip_check,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        qc_flagged,
        air_temperature_qc_isolated_check,
        air_temperature_qc_buddy_check,
        relative_humidity_qc_isolated_check,
        relative_humidity_qc_buddy_check,
        NULL,
        NULL,
        battery_voltage,
        protocol_version
    FROM temp_rh_data
        INNER JOIN station ON temp_rh_data.station_id = station.station_id
        LEFT OUTER JOIN buddy_check_qc ON (
            temp_rh_data.station_id = buddy_check_qc.station_id AND
            temp_rh_data.measured_at = buddy_check_qc.measured_at
        )
    WHERE station.station_type <> 'double'
    ORDER BY temp_rh_data.station_id, temp_rh_data.measured_at DESC
)
'''
new_creation_sql = '''\
    CREATE MATERIALIZED VIEW IF NOT EXISTS latest_data AS
    (
        SELECT DISTINCT ON (station_id)
            biomet_data.station_id,
            long_name,
            latitude,
            longitude,
            altitude,
            district,
            lcz,
            station_type,
            biomet_data.measured_at,
            air_temperature,
            relative_humidity,
            dew_point,
            absolute_humidity,
            specific_humidity,
            heat_index,
            wet_bulb_temperature,
            atmospheric_pressure,
            atmospheric_pressure_reduced,
            lightning_average_distance,
            lightning_strike_count,
            mrt,
            pet,
            pet_category,
            precipitation_sum,
            solar_radiation,
            utci,
            utci_category,
            vapor_pressure,
            wind_direction,
            wind_speed,
            maximum_wind_speed,
            u_wind,
            v_wind,
            sensor_temperature_internal,
            x_orientation_angle,
            y_orientation_angle,
            black_globe_temperature,
            thermistor_resistance,
            voltage_ratio,
            air_temperature_qc_range_check,
            air_temperature_qc_persistence_check,
            air_temperature_qc_spike_dip_check,
            relative_humidity_qc_range_check,
            relative_humidity_qc_persistence_check,
            relative_humidity_qc_spike_dip_check,
            atmospheric_pressure_qc_range_check,
            atmospheric_pressure_qc_persistence_check,
            atmospheric_pressure_qc_spike_dip_check,
            wind_speed_qc_range_check,
            wind_speed_qc_persistence_check,
            wind_speed_qc_spike_dip_check,
            wind_direction_qc_range_check,
            wind_direction_qc_persistence_check,
            u_wind_qc_range_check,
            u_wind_qc_persistence_check,
            u_wind_qc_spike_dip_check,
            v_wind_qc_range_check,
            v_wind_qc_persistence_check,
            v_wind_qc_spike_dip_check,
            maximum_wind_speed_qc_range_check,
            maximum_wind_speed_qc_persistence_check,
            precipitation_sum_qc_range_check,
            precipitation_sum_qc_persistence_check,
            precipitation_sum_qc_spike_dip_check,
            solar_radiation_qc_range_check,
            solar_radiation_qc_persistence_check,
            solar_radiation_qc_spike_dip_check,
            lightning_average_distance_qc_range_check,
            lightning_average_distance_qc_persistence_check,
            lightning_strike_count_qc_range_check,
            lightning_strike_count_qc_persistence_check,
            x_orientation_angle_qc_range_check,
            x_orientation_angle_qc_spike_dip_check,
            y_orientation_angle_qc_range_check,
            y_orientation_angle_qc_spike_dip_check,
            black_globe_temperature_qc_range_check,
            black_globe_temperature_qc_persistence_check,
            black_globe_temperature_qc_spike_dip_check,
            qc_flagged,
            air_temperature_qc_isolated_check,
            air_temperature_qc_buddy_check,
            relative_humidity_qc_isolated_check,
            relative_humidity_qc_buddy_check,
            atmospheric_pressure_qc_isolated_check,
            atmospheric_pressure_qc_buddy_check,
            qc_score,
            battery_voltage,
            protocol_version
        FROM biomet_data
            INNER JOIN station ON biomet_data.station_id = station.station_id
            LEFT OUTER JOIN buddy_check_qc ON (
                biomet_data.station_id = buddy_check_qc.station_id AND
                biomet_data.measured_at = buddy_check_qc.measured_at
            )
        ORDER BY biomet_data.station_id, biomet_data.measured_at DESC
    )
    UNION ALL
    (
        SELECT DISTINCT ON (station_id)
            temp_rh_data.station_id,
            long_name,
            latitude,
            longitude,
            altitude,
            district,
            lcz,
            station_type,
            temp_rh_data.measured_at,
            air_temperature,
            relative_humidity,
            dew_point,
            absolute_humidity,
            specific_humidity,
            heat_index,
            wet_bulb_temperature,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            air_temperature_qc_range_check,
            air_temperature_qc_persistence_check,
            air_temperature_qc_spike_dip_check,
            relative_humidity_qc_range_check,
            relative_humidity_qc_persistence_check,
            relative_humidity_qc_spike_dip_check,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            qc_flagged,
            air_temperature_qc_isolated_check,
            air_temperature_qc_buddy_check,
            relative_humidity_qc_isolated_check,
            relative_humidity_qc_buddy_check,
            NULL,
            NULL,
            qc_score,
            battery_voltage,
            protocol_version
        FROM temp_rh_data
            INNER JOIN station ON temp_rh_data.station_id = station.station_id
            LEFT OUTER JOIN buddy_check_qc ON (
                temp_rh_data.station_id = buddy_check_qc.station_id AND
                temp_rh_data.measured_at = buddy_check_qc.measured_at
            )
        WHERE station.station_type <> 'double'
        ORDER BY temp_rh_data.station_id, temp_rh_data.measured_at DESC
    )
'''


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('buddy_check_qc', sa.Column('qc_score', sa.Numeric(), nullable=True))
    op.execute('DROP MATERIALIZED VIEW latest_data')
    op.execute(new_creation_sql)
    op.create_index(
        op.f('ix_latest_data_district'),
        'latest_data', ['district'], unique=False,
    )
    op.create_index(
        op.f('ix_latest_data_measured_at'),
        'latest_data', ['measured_at'], unique=False,
    )
    op.create_index(
        op.f('ix_latest_data_station_id'),
        'latest_data', ['station_id'], unique=True,
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    op.execute('DROP MATERIALIZED VIEW latest_data')
    op.execute(former_creation_sql)
    op.create_index(
        op.f('ix_latest_data_district'),
        'latest_data', ['district'], unique=False,
    )
    op.create_index(
        op.f('ix_latest_data_measured_at'),
        'latest_data', ['measured_at'], unique=False,
    )
    op.create_index(
        op.f('ix_latest_data_station_id'),
        'latest_data', ['station_id'], unique=True,
    )
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('buddy_check_qc', 'qc_score')
    # ### end Alembic commands ###
