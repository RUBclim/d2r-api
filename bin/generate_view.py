import re
import textwrap
from typing import Any
from typing import Literal
from typing import NamedTuple

from sqlalchemy import Column
from sqlalchemy.orm import InstrumentedAttribute

from app.models import _ATM41DataRawBase
from app.models import _BiometDerivatives
from app.models import _BLGDataRawBase
from app.models import _CalibrationDerivatives
from app.models import _Data
from app.models import _SHT35DataRawBase
from app.models import _TempRHDerivatives
from app.models import BiometData
from app.models import TempRHData
from app.routers.main import get_aggregator

# definition of template strings for generating SQL code
VIEW_TEMPLATE_DAILY = '''
CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS
SELECT
    (time_bucket('1day', measured_at, 'CET') + '1 hour'::INTERVAL)::DATE as measured_at,
    name,
{columns}
FROM {target_table}
GROUP BY measured_at, name
ORDER BY name, measured_at'''

VIEW_TEMPLATE_HOURLY = '''
CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}(
    measured_at,
    name,
{column_names}
)
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
    SELECT
        time_bucket('1hour', measured_at) AT TIME ZONE 'UTC' + '1 hour',
        name,
{columns}
    FROM {target_table}
    GROUP BY time_bucket('1hour', measured_at), name'''

COL_TEMPLATE = '''\
CASE
    WHEN (count(*) FILTER (
            WHERE {column_name} IS NOT NULL) / {total_vals}
        ) > {threshold} THEN {agg_func}
    ELSE NULL
END AS {column_name}{col_suffix}'''

COL_TEMPLATE_NO_TH = '{agg_func} AS {column_name}{col_suffix}'

# definition of template strings for generating Python code
ATTR_TEMPLATE = '{attr_name}: Mapped[{attr_type}] = mapped_column({col_args})'

REFRESH_PG_TEMPLATE = '''\
@classmethod
async def refresh(cls, db: AsyncSession) -> None:
    await db.execute(text('REFRESH MATERIALIZED VIEW {view_name}'))'''

REFRESH_TS_TEMPLATE = '''\
@classmethod
async def refresh(cls) -> None:
    async with sessionmanager.connect(as_transaction=False) as sess:
        await sess.execute(
            text("CALL refresh_continuous_aggregate('{view_name}', NULL, NULL)"),
        )'''

CLASS_TEMPLATE = """\
class {view_name}{inherits}:
    {docstring}
    __tablename__ = {table_name!r}

{attributes}

{refresh_method}

    creation_sql = text('''\\{creation_sql}
    ''')
"""


AVG_EXCLUDES = {'name', 'measured_at', 'protocol_version'}


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
        agg_func = str(get_aggregator(self.sqlalchemy_col).compile())
        if self.threshold > 0:
            template = COL_TEMPLATE
        else:
            template = COL_TEMPLATE_NO_TH

        return template.format(
            column_name=self.name,
            total_vals=self.total_vals,
            threshold=self.threshold,
            agg_func=agg_func,
            col_suffix=self.suffix,
        )

    @property
    def py_repr(self) -> str:
        """generate a sqlalchemy mapping definition for this column"""
        if self.skip_py:
            return ''
        comment = self.sqlalchemy_col.comment
        nullable = self.sqlalchemy_col.nullable
        args = []
        if nullable is not None:
            args.append(f'nullable={nullable!r}')
        if comment is not None:
            args.append(f'comment={comment!r}')

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
    if target_agg == 'daily':
        view_definition = VIEW_TEMPLATE_DAILY.format(
            view_name=view_name,
            columns=textwrap.indent(
                text=',\n'.join([i.sql_repr for i in columns]),
                prefix=' ' * 4,
            ),
            target_table=table.__tablename__,
        )
    else:
        view_definition = VIEW_TEMPLATE_HOURLY.format(
            view_name=view_name,
            column_names=textwrap.indent(
                ',\n'.join([i.full_name for i in columns]),
                prefix=' ' * 4,
            ),
            columns=textwrap.indent(
                text=',\n'.join([i.sql_repr for i in columns]),
                prefix=' ' * 8,
            ),
            target_table=table.__tablename__,
        )
    return view_definition


def generate_sqlalchemy_class(
        table: type[_Data],
        target_agg: Literal['daily', 'hourly'],
        docstring: str | None = None,
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
        if col.key in AVG_EXCLUDES:
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
        other_aggs = {'category', 'count', 'sum', 'max', 'direction'}
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

    # sort by name but keep the columns name and measured at the first two
    cols = [
        Col(
            name='measured_at',
            sqlalchemy_col=table.measured_at,
            skip_py=avgs_defined_by_inheritance,
        ),
        Col(
            name='name',
            sqlalchemy_col=table.name,
            skip_py=avgs_defined_by_inheritance,
        ),
        *sorted_cols,
    ]
    if target_agg == 'hourly':
        refresh_method = REFRESH_TS_TEMPLATE
    else:
        refresh_method = REFRESH_PG_TEMPLATE

    # we need to add a relationship at the end of each view
    py_cols = [i.py_repr for i in cols if i.py_repr]
    py_cols.append('station: Mapped[Station] = relationship(lazy=True)')

    # finally combine everything and generate the full class
    class_def = CLASS_TEMPLATE.format(
        view_name=f'{table.__name__}{target_agg.title()}',
        docstring=f'\"""{docstring}\n    \"""' if docstring else '',
        table_name=view_name,
        inherits=f"({', '.join(inherits)})" if inherits else '',
        creation_sql=textwrap.indent(view_def, ' '*4),
        attributes=textwrap.indent(
            text='\n'.join(py_cols), prefix=' ' * 4,
        ),
        refresh_method=textwrap.indent(
            refresh_method.format(view_name=view_name),
            prefix=' ' * 4,
        ),
    )
    return class_def


def insert_generated(path: str, generated: str) -> None:
    with open(path) as f:
        content = f.read()

    PATTERN = re.compile(r'#\s(?:START_GENERATED|END_GENERATED)')
    before, _, after, = re.split(PATTERN, content)
    new = f'''\
{before}# START_GENERATED\n{generated}
# END_GENERATED{after}\
'''
    with open(path, 'w') as f:
        f.write(new)


def main() -> int:
    docstring = '''\
    This is not an actual table, but a materialized view. We simply trick sqlalchemy
    into thinking this was a table. Querying a materialized view does not differ from
    querying a proper table.'''.lstrip()
    generated_code = ''
    biomet_hourly = generate_sqlalchemy_class(
        table=BiometData,
        docstring=docstring,
        inherits=[
            _ATM41DataRawBase.__name__,
            _BLGDataRawBase.__name__,
            _TempRHDerivatives.__name__,
            _BiometDerivatives.__name__,
        ],
        avgs_defined_by_inheritance=True,
        target_agg='hourly',
    )
    generated_code += biomet_hourly

    temp_rh_hourly = generate_sqlalchemy_class(
        table=TempRHData,
        docstring=docstring,
        inherits=[
            _SHT35DataRawBase.__name__,
            _TempRHDerivatives.__name__,
            _CalibrationDerivatives.__name__,
        ],
        avgs_defined_by_inheritance=True,
        target_agg='hourly',
    )
    generated_code += temp_rh_hourly
    insert_generated(path='app/models.py', generated=generated_code)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
