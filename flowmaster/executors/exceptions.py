from flowmaster.exceptions import FlowmasterExeception


class ExecutorExeception(FlowmasterExeception):
    ...


class SleepException(ExecutorExeception):
    def __init__(self, sleep):
        self.sleep = sleep
        super(SleepException, self).__init__()

    def __str__(self):
        return "Repeat execution after a while"


class PoolOverflowingException(SleepException):
    def __str__(self):
        return "There are no free execution slots in the pool, repeat after a while"


class ExpiredError(ExecutorExeception):
    def __str__(self):
        return "The task timed out"


class SoftTimeLimitError(ExpiredError):
    def __str__(self):
        return "The duration of the task has expired"
