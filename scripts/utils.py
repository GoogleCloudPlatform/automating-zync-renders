# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''Utility methods for Automating ZYNC renders pipeline.'''

import re
import yaml

def read(yaml_file):
    '''Read and parse YAML file.'''

    with open(yaml_file, 'r') as stream:
        try:
            yml_data = yaml.load(stream)
        except yaml.YAMLError, exc:
            raise Exception('Unable to read YAML file.', exc)
        # end try

        return yml_data

    # end with
# end def


def get_trailing_number(part):
  '''Strip part number off part Alembic file.'''

  num = re.search(r'\d+$', part)
  return num.group() if num else None
# end def
