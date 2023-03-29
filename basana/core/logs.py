# Basana
#
# Copyright 2022-2023 Gabriel Martin Becedillas Ruiz
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

import contextlib
import json
import logging

from . import dt


@contextlib.contextmanager
def backtesting_log_mode(dispatcher):
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)

        record_dt = dispatcher.current_event_dt if dispatcher.current_event_dt else dt.utc_now()
        record.created = dt.to_utc_timestamp(record_dt)
        record.msecs = int(record_dt.microsecond / 1000)
        return record

    logging.setLogRecordFactory(record_factory)
    yield
    logging.setLogRecordFactory(old_factory)


# https://docs.python.org/3/howto/logging-cookbook.html#implementing-structured-logging
class StructuredMessage:
    def __init__(self, message, /, **kwargs):
        self.message = message
        self.kwargs = kwargs

    def __str__(self):
        return "{} {}".format(self.message, json.dumps(self.kwargs, default=str))
