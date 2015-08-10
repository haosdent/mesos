# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

##############################################################
# VS_BUILD_CMD generates a build command for project which have Visual Studio
# Solution File(.sln). For example, when we want to build GLOG through Visual
# Studio in command line, we would run this command. It would define
# ${LIB_NAME_UPPER}_BUILD_CMD with build command finally.
function(VS_BUILD_CMD LIB_NAME_UPPER SOLUTION_FILE PROJECTS)

  # Search ${VS_BUILD_SCRIPT} under ${CMAKE_MODULE_PATH}.
  set(VS_BUILD_SCRIPT_NAME VsBuildCommand.bat)
  find_path(VS_BUILD_SCRIPT_DIR ${VS_BUILD_SCRIPT_NAME} ${CMAKE_MODULE_PATH})
  set(VS_BUILD_SCRIPT  ${VS_BUILD_SCRIPT_DIR}/${VS_BUILD_SCRIPT_NAME})

  set(BUILD_CMD_VAR    ${LIB_NAME_UPPER}_BUILD_CMD)

  # Generate a build command with ${VS_BUILD_SCRIPT}.
  set(${BUILD_CMD_VAR}
    ${VS_BUILD_SCRIPT} ${SOLUTION_FILE} ${PROJECTS}
    PARENT_SCOPE)
endfunction()