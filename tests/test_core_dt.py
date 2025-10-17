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

from basana.core import dt


def test_utc_now_mono():
    now_mono = dt.utc_now(monotonic=True)
    now = dt.utc_now()

    assert abs((now - now_mono).total_seconds()) < 0.1

    for _ in range(1000):
        now_mono_1 = dt.utc_now(monotonic=True)
        now_mono_2 = dt.utc_now(monotonic=True)
        assert now_mono_2 >= now_mono_1

    assert now_mono_2 > now_mono
