#pragma once

namespace vdb {
/* system log level
 *
 * Debug: Used for frequent loop logging, may impact performance if it logs
 * system-level details.

 * Verbose: Logs detailed steps of a process, potentially
 * generating a large volume of logs for a single request.

 * Notice: (Default)
 * Logs warnings when errors occur. As the default, minimize use in frequently
 * executed code.

 * Always: Logs essential information that should not be turned
 * off, even when other logs are disabled, for critical issues. */

enum LogLevel {
  kLogDebug = 0,
  kLogVerbose,
  kLogNotice,
  kLogAlways,
  kLogLevelMax
};

#define SYSTEM_LOG(level, ...) serverLog(level, __VA_ARGS__)
#define SYSTEM_LOG_WITH_PATH(level, file, line, ...) \
  do {                                               \
    if (((level) & 0xff) < server.verbosity) break;  \
    _serverLog(level, file, line, __VA_ARGS__);      \
  } while (0)

};  // namespace vdb
