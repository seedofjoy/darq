import datetime
import sys

import arq

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict


class JobCtx(TypedDict):
    redis: arq.ArqRedis
    job_id: str
    job_try: int
    enqueue_time: datetime.datetime
    score: int
