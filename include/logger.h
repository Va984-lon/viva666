/* ============================================================================
 * logger.h — модуль логирования (доп. требование ТЗ)
 *
 * Индивидуальность: потокобезопасный логгер с уровнями, цветным выводом в
 * терминал и общим лог-файлом для обеих программ проекта. Формат строки
 * содержит PID+TID, что позволяет по логу восстановить межпоточную
 * интерливинг-картину работы приложения.
 * ==========================================================================*/
#ifndef VEKTRA_LOGGER_H
#define VEKTRA_LOGGER_H

#include <stddef.h>

typedef enum {
    LOG_TRACE = 0,
    LOG_DEBUG = 1,
    LOG_INFO  = 2,
    LOG_WARN  = 3,
    LOG_ERROR = 4
} log_level_t;

/* Инициализация: открывает file (NULL — только stderr), ставит мин. уровень. */
int  log_init(const char *file, log_level_t min_level);
void log_shutdown(void);

void log_write(log_level_t lvl, const char *module, const char *fmt, ...)
#ifdef __GNUC__
     __attribute__((format(printf, 3, 4)))
#endif
     ;

#define LOG_T(module, ...) log_write(LOG_TRACE, module, __VA_ARGS__)
#define LOG_D(module, ...) log_write(LOG_DEBUG, module, __VA_ARGS__)
#define LOG_I(module, ...) log_write(LOG_INFO,  module, __VA_ARGS__)
#define LOG_W(module, ...) log_write(LOG_WARN,  module, __VA_ARGS__)
#define LOG_E(module, ...) log_write(LOG_ERROR, module, __VA_ARGS__)

const char *log_level_name(log_level_t lvl);

#endif /* VEKTRA_LOGGER_H */
