from typing import Literal


class Statuses:
    add = "ADD"
    run = "RUN"
    success = "SUCCESS"
    error = "ERROR"
    fatal_error = "FATAL_ERROR"
    error_statuses = (error, fatal_error)
    LiteralT = Literal[add, run, success, error, fatal_error]


class FlowETLStep:
    export = "EXPORT"
    transform = "TRANSFORM"
    load = "LOAD"
    LiteralT = Literal[export, transform, load]


class FlowOperator:
    etl = "ETL"
    LiteralT = Literal[etl]
