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
#ifndef __STOUT_OS_WINDOWS_READ_HPP__
#define __STOUT_OS_WINDOWS_READ_HPP__

#include <stdio.h>

#include <string>

#include <stout/result.hpp>
#include <stout/try.hpp>
#include <stout/unimplemented.hpp>


namespace os {

// Reads 'size' bytes from a file from its current offset.
// If EOF is encountered before reading 'size' bytes then the result
// will contain the bytes read and a subsequent read will return None.
inline Result<std::string> read(int fd, size_t size)
{
  UNIMPLEMENTED;
}


// Returns the contents of the file.
inline Try<std::string> read(const std::string& path)
{
  UNIMPLEMENTED;
}

} // namespace os {

#endif // __STOUT_OS_WINDOWS_READ_HPP__
