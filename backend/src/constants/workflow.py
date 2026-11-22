from enum import StrEnum


class Role(StrEnum):
    OPERATOR = 'operator'
    REVIEWER = 'reviewer'
    VIEWER = 'viewer'


class Status(StrEnum):
    QUEUED = 'queued'
    WAITING = 'waiting'
    RUNNING = 'running'
    COMPLETE = 'complete'
    FAILED = 'failed'
