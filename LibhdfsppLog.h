/* Copyright (c) 2005 - 2015 Vertica, an HP company -*- C++ -*- */

#ifndef LIBHDFSPP_LOG_H_
#define LIBHDFSPP_LOG_H_

#include <cstdarg>
#include "Logging.h"

namespace hdfs {

#define LOG_ENABLE true

#define LOG_OPEN(...) do {\
    if (LOG_ENABLE)\
        Logging::logMessage(Logger::OPEN, __VA_ARGS__);\
} while(0)

#define LOG_OPEN_RET(...) do {\
    if (LOG_ENABLE)\
        Logging::logMessage(Logger::OPEN_RET, __VA_ARGS__);\
} while(0)

#define LOG_CLOSE(...) do {\
    if (LOG_ENABLE)\
        Logging::logMessage(Logger::CLOSE, __VA_ARGS__);\
} while(0)

#define LOG_CLOSE_RET(...) do {\
    if (LOG_ENABLE)\
        Logging::logMessage(Logger::CLOSE_RET, __VA_ARGS__);\
} while(0)

#define LOG_READ(...) do {\
    if (LOG_ENABLE)\
        Logging::logMessage(Logger::READ, __VA_ARGS__);\
} while(0)

#define LOG_READ_RET(...) do {\
    if (LOG_ENABLE)\
        Logging::logMessage(Logger::READ_RET, __VA_ARGS__);\
} while(0)

}

#endif
