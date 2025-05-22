#! /usr/bin/env python3
import argparse
import difflib
import os
import re
import textwrap
from collections.abc import Sequence
from functools import partial
from typing import Any
from typing import Literal
from typing import NamedTuple

from sqlalchemy import Column
from sqlalchemy import func
from sqlalchemy import Function
from sqlalchemy import Index
from sqlalchemy import WithinGroup
from sqlalchemy.orm import InstrumentedAttribute

from app.models import _ATM41DataRawBase
from app.models import _BiometDerivatives
from app.models import _BLGDataRawBase
from app.models import _CalibrationDerivatives
from app.models import _Data
from app.models import _SHT35DataRawBase
from app.models import _TempRHDerivatives
from app.models import BiometData
from app.models import MaterializedView
from app.models import TempRHData

# definition of template strings for generating SQL code
_VIEW_TEMPLATE_BASE = '''
CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS
WITH data_bounds AS (
    SELECT
        station_id,
        MIN(measured_at) AS start_time,
        MAX(measured_at) AS end_time
    FROM {target_table}
    GROUP BY station_id
), filling_time_series AS (
    SELECT generate_series(
        DATE_TRUNC('hour', (SELECT MIN(measured_at) FROM {target_table})),
        DATE_TRUNC('hour', (SELECT MAX(measured_at) FROM {target_table}) + '1 hour'::INTERVAL),
        '1 hour'::INTERVAL
    ) AS measured_at
),
stations_subset AS (
    -- TODO: this could be faster if check the station table by station_type
    SELECT DISTINCT station_id FROM {target_table}
),
time_station_combinations AS (
    SELECT
        measured_at,
        stations_subset.station_id,
        start_time,
        end_time
    FROM filling_time_series
    CROSS JOIN stations_subset
    JOIN data_bounds
        ON data_bounds.station_id = stations_subset.station_id
    WHERE filling_time_series.measured_at >= data_bounds.start_time
    AND filling_time_series.measured_at <= data_bounds.end_time
), all_data AS(
    (
        SELECT
            measured_at AS ma,
            station_id,
{null_column_names}
        FROM time_station_combinations
    )
    UNION ALL
    (
        SELECT
            measured_at AS ma,
            station_id,
{basic_column_names}
        FROM {target_table}
    )
) SELECT
    {time_bucket_function} AS measured_at,
    station_id,
{columns}
FROM all_data
GROUP BY measured_at, station_id
ORDER BY measured_at, station_id'''  # noqa: E501

VIEW_TEMPLATE_DAILY = partial(
    _VIEW_TEMPLATE_BASE.format,
    time_bucket_function="(time_bucket('1day', ma, 'CET') + '1 hour'::INTERVAL)::DATE",
)
VIEW_TEMPLATE_HOURLY = partial(
    _VIEW_TEMPLATE_BASE.format,
    time_bucket_function="time_bucket('1 hour', ma) + '1 hour'::INTERVAL",
)


COL_TEMPLATE = '''\
CASE
    WHEN (count(*) FILTER (
            WHERE {column_name} IS NOT NULL) / {total_vals:.1f}
        ) > {threshold} THEN {agg_func}{filter}
    ELSE NULL
END AS {column_name}{col_suffix}'''

COL_TEMPLATE_NO_TH = '{agg_func}{filter} AS {column_name}{col_suffix}'

# definition of template strings for generating Python code
ATTR_TEMPLATE = '{attr_name}: Mapped[{attr_type}] = mapped_column({col_args})'

CLASS_TEMPLATE = """\
class {view_name}{inherits}:
    {docstring}
    __tablename__ = {table_name!r}{table_args}{is_cagg}

{attributes}
    def __repr__(self) -> str:
        return (
            f'{{type(self).__name__}}('
{repr_attrs}
            f')'
        )

    creation_sql = text('''\\{creation_sql}
    '''){noqa}
"""


AVG_EXCLUDES = {
    'name', 'measured_at', 'station_id', 'sensor_id',
    'blg_sensor_id', 'deployment_id',
}


class Col(NamedTuple):
    name: str
    sqlalchemy_col: Column[Any] | InstrumentedAttribute[Any] | Any
    suffix: str = ''
    total_vals: int = 1
    skip_py: bool = False
    threshold: float = 0.0

    @property
    def full_name(self) -> str:
        return f'{self.name}{self.suffix}'

    @property
    def sql_repr(self) -> str:
        """generate a SQL definition for this column"""
        agg_func: Function[Any] | WithinGroup[Any]
        filter = ''
        if 'max' in self.full_name:
            agg_func = func.max(self.sqlalchemy_col)
        elif '_min' in self.full_name:
            agg_func = func.min(self.sqlalchemy_col)
        elif 'category' in self.full_name or 'version' in self.full_name:
            agg_func = func.mode().within_group(self.sqlalchemy_col.asc())
        elif 'direction' in self.full_name:
            agg_func = func.avg_angle(self.sqlalchemy_col)
        elif ('sum' in self.full_name or 'count' in self.full_name):
            agg_func = func.sum(self.sqlalchemy_col)
        else:
            agg_func = func.avg(self.sqlalchemy_col)

        if self.threshold > 0:
            template = COL_TEMPLATE
        else:
            template = COL_TEMPLATE_NO_TH

        # we need special handling for distance-based columns since we cannot use
        # average on 0 values when there were no lightning strikes
        if 'distance' in self.full_name:
            filter = (
                f" FILTER (WHERE {
                    str(self.sqlalchemy_col).replace(
                        f'{self.sqlalchemy_col.table.name}.', ''
                    )
                } > 0.0)"
            )

        return template.format(
            column_name=self.name,
            total_vals=self.total_vals,
            threshold=self.threshold,
            # we don't want the fully qualified name (table_name.column_name)
            agg_func=str(agg_func).replace(f'{self.sqlalchemy_col.table.name}.', ''),
            col_suffix=self.suffix,
            filter=filter,
        )

    @property
    def py_repr(self) -> str:
        """generate a sqlalchemy mapping definition for this column"""
        if self.skip_py:
            return ''
        comment = self.sqlalchemy_col.comment
        nullable = self.sqlalchemy_col.nullable
        doc = self.sqlalchemy_col.doc
        if not doc:
            doc = comment
        args = []
        if nullable is not None:
            args.append(f'nullable={nullable!r}')
        if comment is not None:
            args.append(f'comment={comment!r}')
        if doc is not None:
            if 'min' in self.suffix:
                doc = f'minimum of {doc}'
            elif 'max' in self.suffix:
                doc = f'maximum of {doc}'
            # don't go through the trouble of splitting the docstring
            if len(doc) > 80:
                doc = f"{doc!r},  # noqa: E501"
            else:
                doc = f"{doc!r}"
            args.append(f'doc={doc}')

        row = ATTR_TEMPLATE.format(
            attr_name=self.full_name,
            attr_type=self.sqlalchemy_col.type.python_type.__name__,
            col_args=', '.join(args),
        )
        if len(row) > 84 and args:
            row = ATTR_TEMPLATE.format(
                attr_name=self.full_name,
                attr_type=self.sqlalchemy_col.type.python_type.__name__,
                col_args=f"\n    {',\n    '.join(args)},\n",
            )
        return row


def generate_sql_aggregate(
        table: type[_Data],
        view_name: str,
        columns: list[Col],
        target_agg: Literal['daily', 'hourly'],
) -> str:
    """Generate a SQL definition for a materialized view"""
    basic_columns = [
        i for i in columns if not i.full_name.endswith(('_min', '_max'))
    ]
    basic_column_names = textwrap.indent(
        ',\n'.join([i.full_name for i in basic_columns]),
        prefix=' ' * 12,
    )
    null_column_names = textwrap.indent(
        ',\n'.join([f'NULL AS {i.full_name}' for i in basic_columns]),
        prefix=' ' * 12,
    )
    if target_agg == 'daily':
        view_definition = VIEW_TEMPLATE_DAILY(
            view_name=view_name,
            columns=textwrap.indent(
                text=',\n'.join([i.sql_repr for i in columns]),
                prefix=' ' * 4,
            ),
            null_column_names=null_column_names,
            basic_column_names=basic_column_names,
            target_table=table.__tablename__,
        )
    else:
        view_definition = VIEW_TEMPLATE_HOURLY(
            view_name=view_name,
            column_names=textwrap.indent(
                ',\n'.join([i.full_name for i in columns]),
                prefix=' ' * 4,
            ),
            null_column_names=null_column_names,
            # only the basic columns without min/max/sum etc:
            basic_column_names=basic_column_names,
            columns=textwrap.indent(
                text=',\n'.join([i.sql_repr for i in columns]),
                prefix=' ' * 4,
            ),
            target_table=table.__tablename__,
        )
    return view_definition


def generate_sqlalchemy_class(
        table: type[_Data],
        target_agg: Literal['daily', 'hourly'],
        docstring: str | None = None,
        table_args: tuple[Any | Index, ...] | None = None,
        inherits: list[str] | None = None,
        avgs_defined_by_inheritance: bool = False,
        threshold: float = 0.0,
) -> str:
    """Generate the sqlalchemy python class for the view definition"""
    cols: list[Col] = []
    if target_agg == 'daily':
        total_vals = 288
    else:
        total_vals = 12

    for col in table.__table__.columns:
        if col.key in AVG_EXCLUDES or '_qc_' in col.key:
            continue
        # we set the names and determine its agg function via the name
        cols.append(
            Col(
                name=col.key,
                sqlalchemy_col=col,
                skip_py=avgs_defined_by_inheritance,
                total_vals=total_vals,
                threshold=threshold,
            ),
        )
        # we don't want them to get a _min or _max column
        other_aggs = {'category', 'count', 'sum', 'max', 'direction', 'version', 'qc'}
        if col.key not in AVG_EXCLUDES and not any(i in col.key for i in other_aggs):
            for suffix in ('_min', '_max'):
                cols.append(
                    Col(
                        name=col.key,
                        suffix=suffix,
                        sqlalchemy_col=col,
                        total_vals=total_vals,
                        threshold=threshold,
                    ),
                )

    view_name = f'{table.__tablename__}_{target_agg}'
    sorted_cols = sorted(cols, key=lambda x: x.name)
    view_def = generate_sql_aggregate(
        table=table,
        view_name=view_name,
        columns=sorted_cols,
        target_agg=target_agg,
    )

    # sort by name but keep measured as the first column
    cols = [
        Col(
            name='measured_at',
            sqlalchemy_col=table.measured_at,
            skip_py=avgs_defined_by_inheritance,
        ),
        *sorted_cols,
    ]

    # we need to add a relationship at the end of each view
    py_cols = [i.py_repr for i in cols if i.py_repr]
    py_cols.append(
        textwrap.dedent('''\
        station: Mapped[Station] = relationship(
            lazy=True,
            doc='The station the data was measured at',
        )
        '''),
    )
    inherit_str = f"({', '.join(inherits)})" if inherits else ''
    if inherits and len(inherit_str) + len(table.__name__) + 5 > 88:
        inherit_str = f'({textwrap.indent(f"\n{', '.join(inherits)}", ' ' * 4)},\n)'
        # we need multiple lines
        if inherits and len(inherit_str) + len(table.__name__) + 5 > 88:
            inherit_str = (
                f'({textwrap.indent(f"\n{',\n'.join(inherits)}", ' ' * 4)},\n)'
            )

    creation_sql = textwrap.indent(view_def, ' '*4)
    sql_too_long = any(len(i) > 88 for i in creation_sql.splitlines())
    if sql_too_long:
        noqa = '  # noqa: E501'
    else:
        noqa = ''

    if table_args:
        # do we need multiple lines?
        if any(len(str(i)) > 66 for i in table_args) or len(table_args) >= 2:
            # do we need to break it to separate lines?
            new_parts = []
            for a in table_args:
                if len(str(a)) > 80:
                    parts = re.split(r',?\s|\(|\)', str(a))
                    new = f'{parts[0]}(\n{textwrap.indent(',\n'.join(parts[1:]), ' ' * 4)})'  # noqa: E501
                    new_parts.append(new)
                else:
                    new_parts.append(str(a))
            table_args_str = textwrap.indent(f"\n{',\n'.join(new_parts)},\n", ' ' * 4)
        else:
            table_args_str = f'{table_args[0]},'

        table_args_str = textwrap.indent(
            f'\n__table_args__ = ({table_args_str})', ' ' * 4,
        )
    else:
        table_args_str = ''

    cagg = ''
    # XXX: this is needed when we have a timescale continous aggregate, which is
    # currently not supported due to various limitations
    # mainly: https://github.com/timescale/timescaledb/issues/1324
    # cagg = textwrap.indent('\nis_continuous_aggregate = True',  ' ' * 4)

    # finally combine everything and generate the full class
    repr_attrs_base = [f"f'{i.full_name}={{self.{i.full_name}!r}}, '" for i in cols]
    repr_attrs = []
    for i in repr_attrs_base:
        if len(i) > 76:
            repr_attrs.append(f'{i}  # noqa: E501')
        else:
            repr_attrs.append(i)

    class_def = CLASS_TEMPLATE.format(
        view_name=f'{table.__name__}{target_agg.title()}',
        docstring=f'\"""{docstring}\n    \"""' if docstring else '',
        table_name=view_name,
        inherits=inherit_str,
        creation_sql=creation_sql,
        attributes=textwrap.indent(text='\n'.join(py_cols), prefix=' ' * 4),
        repr_attrs=textwrap.indent(text='\n'.join(repr_attrs), prefix=' ' * 12),
        noqa=noqa,
        table_args=table_args_str,
        is_cagg=cagg,
    )
    return class_def


class Namespace(argparse.Namespace):
    filename: str
    only_show_diff: bool


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    parser.add_argument('--only-show-diff', action='store_true')
    args = parser.parse_args(argv, namespace=Namespace())

    docstring = '''\
    This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.'''.lstrip()
    generated_code = ''
    biomet_hourly = generate_sqlalchemy_class(
        table=BiometData,
        docstring=docstring,
        table_args=(
            Index(
                'ix_biomet_data_hourly_station_id_measured_at',
                'station_id',
                'measured_at',
                unique=True,
            ),
        ),
        inherits=[
            MaterializedView.__name__,
            _ATM41DataRawBase.__name__,
            _BLGDataRawBase.__name__,
            _TempRHDerivatives.__name__,
            _BiometDerivatives.__name__,
        ],
        avgs_defined_by_inheritance=True,
        target_agg='hourly',
    )
    generated_code += f'{biomet_hourly}\n\n'

    temp_rh_hourly = generate_sqlalchemy_class(
        table=TempRHData,
        docstring=docstring,
        table_args=(
            Index(
                'ix_temp_rh_data_hourly_station_id_measured_at',
                'station_id',
                'measured_at',
                unique=True,
            ),
        ),
        inherits=[
            MaterializedView.__name__,
            _SHT35DataRawBase.__name__,
            _TempRHDerivatives.__name__,
            _CalibrationDerivatives.__name__,
        ],
        avgs_defined_by_inheritance=True,
        target_agg='hourly',
    )
    generated_code += f'{temp_rh_hourly}\n\n'

    biomet_daily = generate_sqlalchemy_class(
        table=BiometData,
        docstring=docstring,
        table_args=(
            Index(
                'ix_biomet_data_daily_station_id_measured_at',
                'station_id',
                'measured_at',
                unique=True,
            ),
        ),
        inherits=[
            MaterializedView.__name__,
            _ATM41DataRawBase.__name__,
            _BLGDataRawBase.__name__,
            _TempRHDerivatives.__name__,
            _BiometDerivatives.__name__,
        ],
        avgs_defined_by_inheritance=True,
        target_agg='daily',
        threshold=0.7,
    )
    generated_code += f'{biomet_daily}\n\n'

    temp_rh_daily = generate_sqlalchemy_class(
        table=TempRHData,
        docstring=docstring,
        table_args=(
            Index(
                'ix_temp_rh_data_daily_station_id_measured_at',
                'station_id',
                'measured_at',
                unique=True,
            ),
        ),
        inherits=[
            MaterializedView.__name__,
            _SHT35DataRawBase.__name__,
            _TempRHDerivatives.__name__,
            _CalibrationDerivatives.__name__,
        ],
        avgs_defined_by_inheritance=True,
        target_agg='daily',
        threshold=0.7,
    )
    generated_code += f'{temp_rh_daily}\n\n'
    with open(args.filename) as f:
        content = f.read()

    PATTERN = re.compile(r'#\s(?:START_GENERATED|END_GENERATED)')
    before, _, after, = re.split(PATTERN, content)
    new = f'{before}# START_GENERATED\n{generated_code.strip()}\n# END_GENERATED{after}'

    if args.only_show_diff:
        diff = difflib.unified_diff(new.splitlines(), content.splitlines())
        diff_rows = list(diff)
        if diff_rows:
            print('\n'.join(diff_rows))
            c = f'python -m {os.path.relpath(__file__).strip('.py').replace('/', '.')}'
            print(
                f'views may not be up to date? Generated differs from current!\n'
                f'run: {c} {args.filename} to fix it',
            )
    else:
        with open(args.filename, 'w') as f:
            f.write(new)

    return content != new


if __name__ == '__main__':
    raise SystemExit(main())
