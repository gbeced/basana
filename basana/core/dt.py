# Basana
#
# Copyright 2022 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import calendar
import datetime
import time

from dateutil import tz


# Capture reference point
_start_utc = datetime.datetime.now(tz=datetime.timezone.utc)
_start_monotonic = time.monotonic()


def is_naive(dt: datetime.datetime) -> bool:
    """Returns True if datetime is naive."""
    return dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None


def utc_now(monotonic: bool = False) -> datetime.datetime:
    """
    Returns the current datetime in UTC timezone.

    :param monotonic: True for monotonic behaviour (ignoring system clock updates).
    """
    if monotonic:
        delta = time.monotonic() - _start_monotonic
        return _start_utc + datetime.timedelta(seconds=delta)
    else:
        return datetime.datetime.now(tz=datetime.timezone.utc)


def local_datetime(*args, **kwargs) -> datetime.datetime:
    return datetime.datetime(*args, **kwargs).replace(tzinfo=tz.tzlocal())


def local_now() -> datetime.datetime:
    """Returns the current datetime in local timezone."""
    return datetime.datetime.now().replace(tzinfo=tz.tzlocal())


def to_utc_timestamp(dt: datetime.datetime) -> int:
    # return (dt - datetime.datetime(1970, 1, 1).replace(tzinfo=datetime.timezone.utc)).total_seconds()
    return calendar.timegm(dt.utctimetuple())
