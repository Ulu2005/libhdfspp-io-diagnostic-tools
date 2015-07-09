/* Copyright (c) 2005 - 2015 Vertica, an HP company -*- C++ -*- */

#ifndef LIBHDFSPP_LOG_H_
#define LIBHDFSPP_LOG_H_

#include "Logging.h"

namespace hdfs {

#define LOG_ENABLE true
#define LOG_PATH "PUT_LOG_PATH_HERE"
#define INDEX_PATH "PUT_INDEX_PATH_HERE"

#define LOG_START() do {\
    if (LOG_ENABLE)\
        Logging::startLog(LOG_PATH, INDEX_PATH);\
} while(0)

#define LOG_OPEN() do {\
    if (LOG_ENABLE)\
        Logging::logMessage(Logger::OPEN, fs, path, flags, bufferSize, replication, blockSize);\
} while(0)

#define LOG_OPEN_RET(ret) do {\
    if (LOG_ENABLE)\
        Logging::logMessage(Logger::OPEN_RET, ret);\
} while(0)

#define LOG_CLOSE() do {\
    if (LOG_ENABLE)\
        Logging::logMessage(Logger::CLOSE, fs, file);\
} while(0)

#define LOG_CLOSE_RET(ret) do {\
    if (LOG_ENABLE)\
        Logging::logMessage(Logger::CLOSE_RET, ret);\
} while(0)

#define LOG_READ() do {\
    if (LOG_ENABLE)\
        Logging::logMessage(Logger::READ, fs, file, position, buf, length);\
} while(0)

#define LOG_READ_RET(ret) do {\
    if (LOG_ENABLE)\
        Logging::logMessage(Logger::READ_RET, ret);\
} while(0)

}

#endif
