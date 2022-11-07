from datetime import datetime
from datetime import timezone


class CloseToNow:
    def __init__(self, delta=2):
        self.delta: float = delta
        self.now = datetime.utcnow()
        self.match = False
        self.other = None

    def __eq__(self, other):
        self.other = other
        if other.tzinfo:
            self.now = self.now.replace(tzinfo=timezone.utc)
        self.match = (
            -self.delta < (self.now - other).total_seconds() < self.delta)
        return self.match

    def __repr__(self):
        if self.match:
            # if we've got the correct value return it to aid in diffs
            return repr(self.other)

        return (
            f'<CloseToNow(delta={self.delta}, '
            f'now={self.now:%Y-%m-%dT%H:%M:%S})>'
        )


class AnyInt:
    def __init__(self):
        self.v = None

    def __eq__(self, other):
        if type(other) == int:
            self.v = other
            return True

    def __repr__(self):
        if self.v is None:
            return '<AnyInt>'
        else:
            return repr(self.v)
