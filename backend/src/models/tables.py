from sqlalchemy import (JSON, Column, ForeignKey, Integer, MetaData, String,
                        Table, Text, inspect, text)

schema = MetaData()
users = Table('accounts', schema,
    Column('id', Integer, primary_key=True),
    Column('username', String(40), nullable=False, unique=True),
    Column('password_hash', String(200), nullable=False),
    Column('role', String(20), nullable=False))
cases = Table('business_cases', schema,
    Column('id', Integer, primary_key=True),
    Column('slug', String(64), nullable=False, unique=True),
    Column('title', String(120), nullable=False),
    Column('payload', JSON, nullable=False),
    Column('author', String(40), nullable=False),
    Column('created_at', String(32), nullable=False))
runs = Table('business_runs', schema,
    Column('id', Integer, primary_key=True),
    Column('case_id', ForeignKey('business_cases.id'), nullable=False, index=True),
    Column('status', String(20), nullable=False, index=True),
    Column('owner', String(40), nullable=False),
    Column('attempt_token', String(36)),
    Column('started_at', String(32)),
    Column('finished_at', String(32)),
    Column('created_at', String(32), nullable=False),
    # Months this run touches, derived from the case payload at enqueue time.
    Column('months', JSON),
    # Months that currently keep this run out of the database, for the UI.
    Column('waiting_for', JSON),
    Column('error', Text))
evidence = Table('business_evidence', schema,
    Column('id', Integer, primary_key=True),
    Column('run_id', ForeignKey('business_runs.id'), nullable=False, unique=True),
    Column('report', JSON, nullable=False),
    Column('digest', String(64), nullable=False),
    Column('reviewer', String(40)),
    Column('review_note', String(500)),
    Column('reviewed_at', String(32)))
audit = Table('audit_events', schema,
    Column('id', Integer, primary_key=True),
    Column('actor', String(40), nullable=False),
    Column('action', String(32), nullable=False),
    Column('reference', String(50), nullable=False),
    Column('created_at', String(32), nullable=False))


def ensure_columns(connectable):
    """Add scheduler columns to a database created by an earlier snapshot.

    The project ships only an initial schema, so this keeps an existing demo
    volume usable without a separate migration step.
    """
    present = {column['name'] for column in inspect(connectable).get_columns(runs.name)}
    with connectable.begin() as conn:
        for name in ('months', 'waiting_for'):
            if name not in present:
                conn.execute(text(f'ALTER TABLE {runs.name} ADD COLUMN {name} JSON'))
