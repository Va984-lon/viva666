/* ============================================================================
 * analytics.h — модуль аналитики продаж (ядро второй программы)
 *
 * Индивидуальность проекта "Vektra":
 *  1. Парсинг файла продаж выполняется ПАРАЛЛЕЛЬНО: файл делится на чанки по
 *     границам строк, каждый чанк обрабатывает воркер пула потоков.
 *  2. Агрегация без глобальной блокировки: у каждого воркера собственный
 *     локальный набор счётчиков (thread-local aggregation), которые потом
 *     сливаются в общие таблицы ("scatter–gather"). Это снижает contention
 *     и является осознанным архитектурным решением, а не случайностью.
 *  3. Тренд считается методом наименьших квадратов (линейная регрессия) по
 *     дневной выручке + сравнение первой и второй половин периода.
 * ==========================================================================*/
#ifndef VEKTRA_ANALYTICS_H
#define VEKTRA_ANALYTICS_H

#include "common.h"
#include <pthread.h>

/* Аккумулятор по одному товару. */
typedef struct {
    int    product_id;
    long   qty;
    double revenue;
} vt_prod_agg_t;

/* Аккумулятор по одному покупателю. */
typedef struct {
    int    customer_id;
    long   purchases;
    double total_sum;
} vt_cust_agg_t;

/* Итоговое состояние анализа (защищено мьютексом записи). */
typedef struct vt_analysis {
    /* справочники */
    vt_htab_t   *products;              /* id -> vt_product_t* (const) */
    vt_htab_t   *customers;             /* id -> vt_customer_t* (const) */

    /* агрегаты (сформированы после параллельного прогона) */
    vt_htab_t   *prod_agg;              /* id -> vt_prod_agg_t  */
    vt_htab_t   *cust_agg;              /* id -> vt_cust_agg_t  */

    double       total_revenue;         /* ф-я 1: текущая сумма всех продаж */
    long         total_sales_count;
    long         skipped_lines;         /* повреждённые/чужие строки */

    /* ф-я 4: тренд */
    double       trend_slope_per_day;   /* руб/день (МНК) */
    double       first_half_sum;
    double       second_half_sum;
    time_t       min_ts, max_ts;

    /* параметры обработки, меняемые на ходу (ф-я 5 ТЗ) */
    int          n_workers;             /* потоков парсера */
    int          top_n;                 /* N в топе/антитопе (ф-я 2) */
    double       threshold_sum;         /* порог покупателя (ф-я 3) */

    pthread_mutex_t lock;               /* защита изменения параметров */
} vt_analysis_t;

/* Создание/уничтожение контекста. Справочники загружаются из CSV. */
vt_analysis_t *vt_analytics_new(const char *products_csv,
                                const char *customers_csv);
void           vt_analytics_free(vt_analysis_t *a);

/* Полный параллельный проход по файлу продаж: перечитать и пересобрать все
 * агрегаты. chunk_bytes — размер чанка; workers<=0 — взять из контекста. */
int  vt_analytics_run_pass(vt_analysis_t *a, const char *sales_file,
                           size_t chunk_bytes, int workers);

/* ---- Отчёты (ф-я 6 ТЗ): сохранение в txt, возвращают 0 при успехе ---- */
int vt_report_total(const vt_analysis_t *a, const char *path);
int vt_report_top_bottom(const vt_analysis_t *a, const char *path);
int vt_report_big_customers(const vt_analysis_t *a, const char *path);
int vt_report_trend(const vt_analysis_t *a, const char *path);
int vt_report_full(const vt_analysis_t *a, const char *path);

#endif /* VEKTRA_ANALYTICS_H */
