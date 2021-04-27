#ifndef __FS_UTIL
#define __FS_UTIL

#include <sys/stat.h>
#include <limits.h>

inline void mkdirs(const char* dir)
{
    char   tmp[256];
    char*  p = NULL;
    size_t len;

    len = snprintf(tmp, sizeof(tmp), "%s", dir);
    if (tmp[len - 1] == '/') tmp[len - 1] = 0;
    for (p = tmp + 1; *p; p++)
        if (*p == '/') {
            *p = 0;
            mkdir(tmp, S_IRWXU);
            *p = '/';
        }
    mkdir(tmp, S_IRWXU);
}

#endif
