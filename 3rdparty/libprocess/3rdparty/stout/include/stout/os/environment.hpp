/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __STOUT_OS_ENVIRONMENT_HPP__
#define __STOUT_OS_ENVIRONMENT_HPP__

#include <map>
#include <regex>
#include <string>

#include <stout/option.hpp>

#include <stout/os/raw/environment.hpp>


namespace os {

inline std::map<std::string, std::string> environment(
    Option<std::string> strRegex = None())
{
  char** env = os::raw::environment();

  std::smatch match;
  Option<std::regex> regex = None();
  if (strRegex.isSome()) {
    regex = std::regex(strRegex.get());
  }

  std::map<std::string, std::string> result;

  for (size_t index = 0; env[index] != NULL; index++) {
    std::string entry(env[index]);
    size_t position = entry.find_first_of('=');
    if (position == std::string::npos) {
      continue; // Skip malformed environment entries.
    }

    // gcc 4.8 not implement regex completely. More details could be found in
    // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=53631
    // Skip dismatch environment entries when specify regex filter.
    if (regex.isSome()) {
      std::regex_match(entry, match, regex.get());
      if (match.size() == 0) {
        continue;
      }
    }

    result[entry.substr(0, position)] = entry.substr(position + 1);
  }

  return result;
}

} // namespace os {

#endif // __STOUT_OS_ENVIRONMENT_HPP__
