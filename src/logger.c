/* logger.c — реализация потокобезопасного логгера (см. include/logger.h) */
#include "logger.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <pthread.h>

static FILE          *g_fp      = NULL;   /* лог-файл (NULL = не ведём)     */
static log_level_t    g_min     = LOG_INFO;
static pthread_mutex_t g_mtx    = PTHREAD_MUTEX_INITIALIZER;
static int            g_colors  = -1;     /* авто: есть ли TTY              */

const char *log_level_name(log_level_t lvl)
{
    static const char *names[] = {"TRACE", "DEBUG", "INFO ", "WARN ", "ERROR"};
    if ((unsigned)lvl > 4) return "?????";
    return names[lvl];
}

int log_init(const char *file, log_level_t min_level)
{
    g_min = min_level;
    if (g_colors < 0)
        g_colors = isatty(STDERR_FILENO) ? 1 : 0;
    if (!file) return 0;
    /* каталог для лога создаём по возможности (best-effort) */
    char dir[256];
    snprintf(dir, sizeof dir, "%s", file);
    char *slash = strrchr(dir, '/');
    if (slash) { *slash = '\0'; if (dir[0]) mkdir(dir, 0775); }
    g_fp = fopen(file, "a");
    return g_fp ? 0 : -1;
}

void log_shutdown(void)
{
    pthread_mutex_lock(&g_mtx);
    if (g_fp) { fclose(g_fp); g_fp = NULL; }
    pthread_mutex_unlock(&g_mtx);
}

static const char *color_for(log_level_t lvl)
{
    switch (lvl) {
    case LOG_TRACE: return "\x1b[90m";
    case LOG_DEBUG: return "\x1b[36m";
    case LOG_INFO:  return "\x1b[32m";
    case LOG_WARN:  return "\x1b[33m";
    case LOG_ERROR: return "\x1b[31m";
    }
    return "";
}

void log_write(log_level_t lvl, const char *module, const char *fmt, ...)
{
    if (lvl < g_min) return;

    char msg[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tmv;
    localtime_r(&ts.tv_sec, &tmv);

    char stamp[32];
    strftime(stamp, sizeof stamp, "%Y-%m-%d %H:%M:%S", &tmv);

    pid_t tid = (pid_t)syscall(SYS_gettid);

    pthread_mutex_lock(&g_mtx);

    if (g_fp) {
        fprintf(g_fp, "[%s.%03ld] [%s] [%5d:%-6d] [%s] %s\n",
                stamp, ts.tv_nsec / 1000000, log_level_name(lvl),
                (int)getpid(), (int)tid, module, msg);
        fflush(g_fp);
    }
    if (g_colors)
        fprintf(stderr, "%s[%s] %-5s %s | %s\x1b[0m\n",
                color_for(lvl), stamp, log_level_name(lvl), module, msg);
    else
        fprintf(stderr, "[%s] %-5s %s | %s\n",
                stamp, log_level_name(lvl), module, msg);

    pthread_mutex_unlock(&g_mtx);
}
