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

class Missing:
    pass


def _get_config_value_impl(config: dict, path: str, default=None):
    ret = default
    current_dict = config
    keys = path.split(".")
    for i, key in enumerate(keys):
        assert key, "Invalid path {}".format(path)
        if i == len(keys) - 1:
            ret = current_dict.get(key, default)
        else:
            current_dict = current_dict.get(key, {})
            assert isinstance(current_dict, dict), f"Element at {key} is not a dictionary"
    return ret


def get_config_value(config: dict, path: str, default=None, overrides: dict = {}):
    missing = Missing()
    ret = _get_config_value_impl(overrides, path, default=missing)
    if ret == missing:
        ret = _get_config_value_impl(config, path, default=default)
    return ret
