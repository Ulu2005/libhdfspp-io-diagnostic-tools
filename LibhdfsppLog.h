/* Copyright (c) 2005 - 2011 Vertica, an HP company -*- C++ -*- */

#ifndef LIBHDFSPP_LOG_H_
#define LIBHDFSPP_LOG_H_

#include <cstdarg>
#include "Logger.h"

namespace iotools {

#define LOG_OPEN(...) do {\
    Logger::logMessage(Logger::OPEN, __VA_ARGS__);\
} while(0)

#define LOG_OPEN_RET(...) do {\
    Logger::logMessage(Logger::OPEN_RET, __VA_ARGS__);\
} while(0)

#define LOG_CLOSE(...) do {\
    Logger::logMessage(Logger::CLOSE, __VA_ARGS__);\
} while(0)

#define LOG_CLOSE_RET(...) do {\
    Logger::logMessage(Logger::CLOSE_RET, __VA_ARGS__);\
} while(0)

#define LOG_READ(...) do {\
    Logger::logMessage(Logger::READ, __VA_ARGS__);\
} while(0)

#define LOG_READ_RET(...) do {\
    Logger::logMessage(Logger::READ_RET, __VA_ARGS__);\
} while(0)

}

#endif
