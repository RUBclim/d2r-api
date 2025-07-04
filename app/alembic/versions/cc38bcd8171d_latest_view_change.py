"""latest_view_change

Revision ID: cc38bcd8171d
Revises: 910d22cb1e47
Create Date: 2025-07-02 12:23:21.226866

"""
from collections.abc import Sequence

from alembic import op

from app.models import LatestData

# revision identifiers, used by Alembic.
revision: str = 'cc38bcd8171d'
down_revision: str | None = '910d22cb1e47'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute('DROP MATERIALIZED VIEW latest_data')
    op.execute(LatestData.creation_sql)
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
    op.execute(LatestData.creation_sql)
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
