#!/bin/bash
#
# Copyright 2010 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

dir=`dirname $0`
demo_dir=`pwd`

test () {
  if [ -z $APPENGINE_LIB ]; then
    echo "APPENGINE_LIB environment variable shoud be defined and should point to appengine sdk folder"
    exit 1
  fi

  export PYTHONPATH="${PYTHONPATH}:\
$APPENGINE_LIB:\
$APPENGINE_LIB/lib/fancy_urllib:\
$APPENGINE_LIB/lib/webob-1.1.1:\
$APPENGINE_LIB/lib/yaml/lib:\
$dir/src:\
$dir/test:\
"
  fetch_dependencies
  echo "Using PYTHONPATH=$PYTHONPATH"
  exit_status=0
  for t in $(find "$dir/test" -name "*test.py"); do
    if python $t
    then
      echo "PASSED"
    else
      echo "FAILED"
      ((exit_status++))
    fi
  done

  echo "----------------------------------------------------------------------"
  if [ $exit_status -ne 0 ];
  then
    echo "FAILED $exit_status tests"
  else
    echo "PASSED all tests"
  fi
  exit $exit_status
}

fetch_dependencies() {
  if [ ! `which pip` ]
  then
    echo "pip not found. pip is required to install dependencies."
    exit 1;
  fi
  # Work arround https://github.com/pypa/pip/issues/1356
  for dep in `cat $dir/src/todelete.txt`
  do
      rm -r $dir/src/$dep $dir/src/$dep*-info 2>/dev/null
  done

  pip install --exists-action=s -r $dir/src/requirements.txt -t $dir/src/ --upgrade || exit 1
  pip install --exists-action=s -r $dir/src/requirements.txt -t $dir/demo/ --upgrade || exit 1
}

case "$1" in
  test)
    test
    ;;
  deps)
    fetch_dependencies
    ;;
  *)
    echo $"Usage: $0 {test|deps}"
    exit 1
esac
