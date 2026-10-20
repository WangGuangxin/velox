# Copyright (c) Facebook, Inc. and its affiliates.
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

include_guard(GLOBAL)

# Shared by the Velox build and Gluten's native build. Keep profiling opt-in,
# and let compiler-command changes trigger recompilation between phases.
function(velox_configure_pgo prefix)
  set(${prefix}_PGO_MODE "OFF" CACHE STRING "PGO phase: OFF, GENERATE or USE")
  set_property(CACHE ${prefix}_PGO_MODE PROPERTY STRINGS OFF GENERATE USE)
  set(${prefix}_PGO_PROFILE "" CACHE PATH "GCC profile directory or Clang profile directory/file")
  string(TOUPPER "${${prefix}_PGO_MODE}" mode)
  if(NOT mode MATCHES "^(OFF|GENERATE|USE)$")
    message(FATAL_ERROR "${prefix}_PGO_MODE must be OFF, GENERATE or USE")
  endif()
  if(mode STREQUAL "OFF")
    return()
  endif()
  if("${${prefix}_PGO_PROFILE}" STREQUAL "")
    message(FATAL_ERROR "${prefix}_PGO_PROFILE is required for PGO ${mode}")
  endif()
  get_filename_component(profile "${${prefix}_PGO_PROFILE}" ABSOLUTE BASE_DIR "${CMAKE_BINARY_DIR}")
  if(mode STREQUAL "GENERATE")
    file(MAKE_DIRECTORY "${profile}")
  elseif(NOT EXISTS "${profile}")
    message(FATAL_ERROR "PGO profile does not exist: ${profile}")
  endif()

  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if(NOT IS_DIRECTORY "${profile}")
      message(FATAL_ERROR "GCC PGO requires a profile directory")
    endif()
    if(mode STREQUAL "GENERATE")
      set(flag "-fprofile-generate=${profile}")
      add_compile_options(-fprofile-update=atomic)
    else()
      set(flag "-fprofile-use=${profile}")
      add_compile_options(-fprofile-correction)
    endif()
    add_compile_options("-fprofile-prefix-path=${CMAKE_BINARY_DIR}")
  elseif(CMAKE_CXX_COMPILER_ID MATCHES "^(Clang|AppleClang)$")
    if(mode STREQUAL "GENERATE")
      set(flag "-fprofile-instr-generate=${profile}/%m-%p.profraw")
    else()
      if(IS_DIRECTORY "${profile}")
        message(FATAL_ERROR "Clang PGO USE requires an llvm-profdata merged file")
      endif()
      set(flag "-fprofile-instr-use=${profile}")
    endif()
  else()
    message(FATAL_ERROR "PGO requires GCC or Clang")
  endif()
  add_compile_options("${flag}")
  add_link_options("${flag}")
  message(STATUS "${prefix} PGO ${mode}: ${profile}")
endfunction()
