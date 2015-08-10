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

#ifndef __STOUT_OS_WINDOWS_OPEN_HPP__
#define __STOUT_OS_WINDOWS_OPEN_HPP__

#include <sys/types.h>

#include <string>

#include <stout/try.hpp>
#include <stout/unimplemented.hpp>


namespace os {

inline Try<int> open(const std::string& path, int oflag, mode_t mode = 0)
{
  UNIMPLEMENTED;
}

} // namespace os {

#endif // __STOUT_OS_WINDOWS_OPEN_HPP__
