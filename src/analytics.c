/* ============================================================================
 * analytics.c — параллельный анализатор файла продаж (вторая программа, ядро)
 *
 * Архитектура одного прогона (run_pass):
 *   1. Определяем размер файла и делим его на чанки по ~chunk_bytes,
 *      сдвигая границы к следующему '\n' (чтобы строки не рвались).
 *   2. Каждый чанк — задача в пул потоков. Воркер читает свой диапазон через
 *      pread() (общий fd, смещение передаётся — блокировки нет) и ведёт
 *      ЛОКАЛЬНЫЕ агрегаты (scatter).
 *   3. После tp_drain() координатор сливает локальные таблицы в общие
 *      (gather) — без мьютексов на горячем пути парсинга.
 * ==========================================================================*/
#include "analytics.h"
#include "threadpool.h"
#include "logger.h"

#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define MODULE "analytics"

/* Максимум дней, за которые строится тренд (буфер дневной выручки). */
#define VT_MAX_DAYS 1024

struct daymerge { double rev[VT_MAX_DAYS]; long base; };

/* ------------------------- загрузка справочников ------------------------ */

static void free_product(void *p) { free(p); }

static vt_htab_t *load_csv(const char *path, int is_products, long *bad_out)
{
    FILE *f = fopen(path, "r");
    if (!f) {
        LOG_E(MODULE, "не удалось открыть справочник '%s': %s",
              path, strerror(errno));
        return NULL;
    }
    vt_htab_t *h = vt_htab_new(is_products ? VT_MAX_PRODUCT : VT_MAX_CUSTOMER);
    char line[VT_MAX_LINE];
    long bad = 0, good = 0;
    while (fgets(line, sizeof line, f)) {
        if (line[0] == '#' || line[0] == '\n' || line[0] == '\r')
            continue;                       /* комментарий/пустая строка */
        if (is_products) {
            vt_product_t p;
            if (vt_parse_product_line(line, &p) == 0) {
                vt_product_t *cp = malloc(sizeof p);
                *cp = p;
                vt_htab_put(h, p.id, cp);
                good++;
            } else bad++;
        } else {
            vt_customer_t c;
            if (vt_parse_customer_line(line, &c) == 0) {
                vt_customer_t *cc = malloc(sizeof c);
                *cc = c;
                vt_htab_put(h, c.id, cc);
                good++;
            } else bad++;
        }
    }
    fclose(f);
    if (bad_out) *bad_out = bad;
    LOG_I(MODULE, "справочник %s: записей=%ld, повреждённых строк=%ld",
          path, good, bad);
    return h;
}

/* ------------------------------ контекст ---------------------------------- */

vt_analysis_t *vt_analytics_new(const char *products_csv,
                                const char *customers_csv)
{
    vt_analysis_t *a = calloc(1, sizeof *a);
    if (!a) return NULL;
    a->products  = load_csv(products_csv, 1, NULL);
    a->customers = load_csv(customers_csv, 0, NULL);
    if (!a->products || !a->customers) {
        vt_analytics_free(a);
        return NULL;
    }
    a->prod_agg     = vt_htab_new(VT_MAX_PRODUCT);
    a->cust_agg     = vt_htab_new(VT_MAX_CUSTOMER);
    pthread_mutex_init(&a->lock, NULL);

    /* значения по умолчанию (ф-я 5 ТЗ — потом меняются из CLI-интерфейса) */
    a->n_workers     = 4;
    a->top_n         = VT_TOP_N_DEFAULT;
    a->threshold_sum = 5000.0;
    LOG_I(MODULE, "контекст создан: товаров=%zu покупателей=%zu",
          vt_htab_count(a->products), vt_htab_count(a->customers));
    return a;
}

void vt_analytics_free(vt_analysis_t *a)
{
    if (!a) return;
    vt_htab_free(a->products, free_product);
    vt_htab_free(a->customers, free_product);
    vt_htab_free(a->prod_agg, free);
    vt_htab_free(a->cust_agg, free);
    pthread_mutex_destroy(&a->lock);
    free(a);
}

/* ---------------------- scatter–gather параллельный проход --------------- */

typedef struct {
    vt_analysis_t *a;
    int            fd;
    off_t          start, end;     /* диапазон байт чанка               */
    /* локальный аккумулятор воркера */
    vt_htab_t     *lprod;
    vt_htab_t     *lcust;
    double         lrevenue;
    long           lcount;
    long           lskipped;
    time_t         lmin_ts, lmax_ts;
    /* для тренда: выручка по дням (ключ = день от начала эпохи данных) */
    vt_htab_t     *lday;           /* day_idx -> double*               */
} chunk_task_t;

static double *day_cell(vt_htab_t *h, long day)
{
    double *d = vt_htab_get(h, (int)day);
    if (!d) { d = calloc(1, sizeof *d); vt_htab_put(h, (int)day, d); }
    return d;
}

static void chunk_worker(void *arg)
{
    chunk_task_t *t = arg;
    size_t len = (size_t)(t->end - t->start);
    char *buf = malloc(len + 2);
    if (!buf) { LOG_E(MODULE, "OOM в воркере чанка (%zu байт)", len); return; }

    ssize_t rd = pread(t->fd, buf, len, t->start);
    if (rd < 0) {
        LOG_E(MODULE, "pread(%lld..%lld): %s", (long long)t->start,
              (long long)t->end, strerror(errno));
        free(buf);
        return;
    }
    buf[rd] = '\n';                      /* страж конца диапазона        */
    buf[rd + 1] = '\0';

    char *p = buf, *last = buf + rd;
    while (p < last) {
        char *nl = memchr(p, '\n', (size_t)(last - p));
        if (!nl) nl = last;
        *nl = '\0';
        vt_sale_t s;
        if (p[0] != '\0' && vt_parse_sale_line(p, &s) == 0) {
            vt_product_t  *pr = vt_htab_get(t->a->products,  s.product_id);
            vt_customer_t *cu = vt_htab_get(t->a->customers, s.customer_id);
            if (pr && cu) {
                double sum = pr->unit_price * (100 - cu->discount_pct) / 100.0;

                vt_prod_agg_t *pa = vt_htab_get(t->lprod, pr->id);
                if (!pa) {
                    pa = calloc(1, sizeof *pa);
                    pa->product_id = pr->id;
                    vt_htab_put(t->lprod, pr->id, pa);
                }
                pa->qty++; pa->revenue += sum;

                vt_cust_agg_t *ca = vt_htab_get(t->lcust, cu->id);
                if (!ca) {
                    ca = calloc(1, sizeof *ca);
                    ca->customer_id = cu->id;
                    vt_htab_put(t->lcust, cu->id, ca);
                }
                ca->purchases++; ca->total_sum += sum;

                t->lrevenue += sum;
                t->lcount++;
                if (s.ts < t->lmin_ts || t->lcount == 1) t->lmin_ts = s.ts;
                if (s.ts > t->lmax_ts) t->lmax_ts = s.ts;

                long day = (long)(s.ts / 86400);
                *day_cell(t->lday, day) += sum;
            } else {
                t->lskipped++;           /* id нет в справочнике */
            }
        } else if (p[0] != '\0') {
            t->lskipped++;               /* битая/чужая строка */
        }
        p = nl + 1;
    }
    free(buf);
    LOG_D(MODULE, "чанк [%lld;%lld) обработан: sales=%ld revenue=%.2f skip=%ld",
          (long long)t->start, (long long)t->end, t->lcount, t->lrevenue,
          t->lskipped);
}

/* коллбэк слияния товаров */
typedef struct { vt_analysis_t *a; } merge_ctx_t;

static void merge_prod(int key, void *val, void *user)
{
    merge_ctx_t *m = user;
    vt_prod_agg_t *src = val;
    vt_prod_agg_t *dst = vt_htab_get(m->a->prod_agg, key);
    if (!dst) {
        dst = calloc(1, sizeof *dst);
        dst->product_id = key;
        vt_htab_put(m->a->prod_agg, key, dst);
    }
    dst->qty += src->qty;
    dst->revenue += src->revenue;
}

static void merge_cust(int key, void *val, void *user)
{
    merge_ctx_t *m = user;
    vt_cust_agg_t *src = val;
    vt_cust_agg_t *dst = vt_htab_get(m->a->cust_agg, key);
    if (!dst) {
        dst = calloc(1, sizeof *dst);
        dst->customer_id = key;
        vt_htab_put(m->a->cust_agg, key, dst);
    }
    dst->purchases += src->purchases;
    dst->total_sum += src->total_sum;
}

static void merge_day(int key, void *val, void *user)
{
    struct daymerge *dm = user;
    long rel = (long)key - dm->base;         /* день относительно min_ts */
    if (rel >= 0 && rel < VT_MAX_DAYS)
        dm->rev[rel] += *(const double *)val;
}

int vt_analytics_run_pass(vt_analysis_t *a, const char *sales_file,
                          size_t chunk_bytes, int workers)
{
    if (workers <= 0) workers = a->n_workers;
    if (chunk_bytes < 4096) chunk_bytes = 1u << 20;   /* по умолчанию 1 MiB */

    int fd = open(sales_file, O_RDONLY);
    if (fd < 0) {
        LOG_E(MODULE, "файл продаж '%s': %s", sales_file, strerror(errno));
        return -1;
    }
    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size == 0) {
        LOG_W(MODULE, "файл продаж пуст или недоступен (%s)", sales_file);
        close(fd);
        return st.st_size == 0 ? 0 : -1;
    }

    /* сброс старых агрегатов — каждый прогон считается заново */
    vt_htab_free(a->prod_agg, free); a->prod_agg = vt_htab_new(VT_MAX_PRODUCT);
    vt_htab_free(a->cust_agg, free); a->cust_agg = vt_htab_new(VT_MAX_CUSTOMER);
    a->total_revenue     = 0;
    a->total_sales_count = 0;
    a->skipped_lines     = 0;
    a->trend_slope_per_day = 0;
    a->first_half_sum    = 0;
    a->second_half_sum   = 0;
    a->min_ts = a->max_ts = 0;

    /* режем файл на чанки по границам строк */
    off_t pos = 0, size = st.st_size;
    int nchunks = 0;
    chunk_task_t *tasks = calloc((size_t)(size / (off_t)chunk_bytes + 2),
                                 sizeof *tasks);
    if (!tasks) { close(fd); return -1; }

    while (pos < size) {
        off_t end = pos + (off_t)chunk_bytes;
        if (end >= size) end = size;
        else {
            /* сдвигаем границу к началу следующей строки */
            char c;
            while (end < size && pread(fd, &c, 1, end) == 1) {
                end++;
                if (c == '\n') break;
            }
        }
        chunk_task_t *t = &tasks[nchunks++];
        t->a = a; t->fd = fd; t->start = pos; t->end = end;
        t->lprod = vt_htab_new(64);
        t->lcust = vt_htab_new(64);
        t->lday  = vt_htab_new(64);
        t->lmin_ts = t->lmax_ts = 0;
        pos = end;
    }
    LOG_I(MODULE, "прогон: файл=%.1f MiB, чанков=%d, потоков-воркеров=%d",
          size / 1048576.0, nchunks, workers);

    tp_pool_t *pool = tp_create((size_t)workers);
    for (int i = 0; i < nchunks; i++)
        tp_submit(pool, chunk_worker, &tasks[i]);
    tp_drain(pool);
    tp_destroy(pool);

    /* gather: сливаем локальные результаты в общие таблицы.
     * Сначала — быстрый проход ради глобального min_ts (он нужен как база
     * для нормализации дней в тренде). */
    time_t gmin = 0, gmax = 0;
    for (int i = 0; i < nchunks; i++) {
        chunk_task_t *t = &tasks[i];
        if (t->lcount > 0) {
            if (!gmin || t->lmin_ts < gmin) gmin = t->lmin_ts;
            if (t->lmax_ts > gmax) gmax = t->lmax_ts;
        }
    }
    long base_day = gmin ? (long)(gmin / 86400) : 0;

    struct daymerge dm;
    memset(dm.rev, 0, sizeof dm.rev);
    dm.base = base_day;

    merge_ctx_t mctx = { a };
    for (int i = 0; i < nchunks; i++) {
        chunk_task_t *t = &tasks[i];
        a->total_revenue    += t->lrevenue;
        a->total_sales_count+= t->lcount;
        a->skipped_lines    += t->lskipped;
        vt_htab_iter(t->lprod, merge_prod, &mctx);
        vt_htab_iter(t->lcust, merge_cust, &mctx);
        vt_htab_iter(t->lday, merge_day, &dm);
        vt_htab_free(t->lprod, free);
        vt_htab_free(t->lcust, free);
        vt_htab_free(t->lday, free);
    }
    free(tasks);
    close(fd);

    a->min_ts = gmin; a->max_ts = gmax;

    /* ---- ф-я 4: тренд по дневной выручке (МНК + половины периода) ---- */
    double sx = 0, sy = 0, sxy = 0, sxx = 0; int ndays = 0;
    double first_half = 0, second_half = 0;
    int half = (int)((gmax - gmin) / 86400 / 2) + 1;
    for (int d = 0; d < VT_MAX_DAYS; d++) {
        if (dm.rev[d] == 0.0) continue;
        ndays++;
        double x = d;
        sx += x; sy += dm.rev[d]; sxy += x * dm.rev[d]; sxx += x * x;
        if (d < half) first_half += dm.rev[d];
        else          second_half += dm.rev[d];
    }
    if (ndays >= 2) {
        double den = ndays * sxx - sx * sx;
        a->trend_slope_per_day = den != 0 ? (ndays * sxy - sx * sy) / den : 0;
    }
    a->first_half_sum  = first_half;
    a->second_half_sum = second_half;

    LOG_I(MODULE, "прогон завершён: продаж=%ld сумма=%.2f руб,skip=%ld,"
          " тренд=%.2f руб/день", a->total_sales_count, a->total_revenue,
          a->skipped_lines, a->trend_slope_per_day);
    return 0;
}

/* ------------------------------- отчёты ---------------------------------- */

static void report_header(FILE *f, const char *title, const vt_analysis_t *a)
{
    time_t now = time(NULL);
    char ts[64];
    struct tm tmv;
    localtime_r(&now, &tmv);
    strftime(ts, sizeof ts, "%Y-%m-%d %H:%M:%S", &tmv);
    fprintf(f, "==============================================================\n");
    fprintf(f, " ВЕКTRA SALES ANALYTICS\n");
    fprintf(f, " Отчёт: %s\n", title);
    fprintf(f, " Сформирован: %s\n", ts);
    fprintf(f, " Параметр порога: %.2f руб | Топ-N: %d | Потоков: %d\n",
            a->threshold_sum, a->top_n, a->n_workers);
    fprintf(f, "==============================================================\n\n");
}

/* 1. Текущая сумма всех продаж */
int vt_report_total(const vt_analysis_t *a, const char *path)
{
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    report_header(f, "Текущая сумма всех продаж (функция 1)", a);
    fprintf(f, "Всего продаж:        %ld шт.\n", a->total_sales_count);
    fprintf(f, "Общая сумма:         %.2f руб.\n", a->total_revenue);
    fprintf(f, "Средний чек:         %.2f руб.\n",
            a->total_sales_count ? a->total_revenue / a->total_sales_count : 0);
    fprintf(f, "Строк пропущено:     %ld (битые/неизвестные id)\n",
            a->skipped_lines);
    fclose(f);
    LOG_I(MODULE, "отчёт сохранён: %s", path);
    return 0;
}

/* 2. Топ-N популярных и непопулярных товаров */
typedef struct { int id; long qty; double revenue; const char *name; } row_t;

static int cmp_rev_desc(const void *x, const void *y)
{
    double a = ((const row_t *)x)->revenue, b = ((const row_t *)y)->revenue;
    return (b > a) - (b < a);
}
static int cmp_rev_asc(const void *x, const void *y)
{
    double a = ((const row_t *)x)->revenue, b = ((const row_t *)y)->revenue;
    return (a > b) - (a < b);
}

int vt_report_top_bottom(const vt_analysis_t *a, const char *path)
{
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    report_header(f, "Топ популярных и непопулярных товаров (функция 2)", a);

    size_t n = vt_htab_count(a->prod_agg);
    row_t *rows = calloc(n + 1, sizeof *rows);
    if (!rows) { fclose(f); return -1; }

    /* снимок ключей -> выборка агрегатов (без знания внутренних деталей) */
    int *keys = calloc(n + 1, sizeof *keys);
    size_t idx = vt_htab_keys(a->prod_agg, keys, n);
    for (size_t i = 0; i < idx; i++) {
        vt_prod_agg_t *pa = vt_htab_get(a->prod_agg, keys[i]);
        vt_product_t  *pr = vt_htab_get(a->products, keys[i]);
        rows[i].id      = keys[i];
        rows[i].qty     = pa ? pa->qty : 0;
        rows[i].revenue = pa ? pa->revenue : 0;
        rows[i].name    = pr ? pr->name : "?";
    }
    free(keys);

    size_t topn = (size_t)a->top_n < idx ? (size_t)a->top_n : idx;
    qsort(rows, idx, sizeof *rows, cmp_rev_desc);
    fprintf(f, "--- ТОП-%zu САМЫХ ПОПУЛЯРНЫХ ТОВАРОВ (по выручке) ---\n", topn);
    for (size_t i = 0; i < topn; i++)
        fprintf(f, " %2zu. T#%-4d %-28s qty=%-5ld выручка=%12.2f руб\n",
                i + 1, rows[i].id, rows[i].name, rows[i].qty, rows[i].revenue);

    qsort(rows, idx, sizeof *rows, cmp_rev_asc);
    fprintf(f, "\n--- ТОП-%zu САМЫХ НЕПОПУЛЯРНЫХ ТОВАРОВ (по выручке) ---\n", topn);
    for (size_t i = 0; i < topn; i++)
        fprintf(f, " %2zu. T#%-4d %-28s qty=%-5ld выручка=%12.2f руб\n",
                i + 1, rows[i].id, rows[i].name, rows[i].qty, rows[i].revenue);
    free(rows);
    fclose(f);
    LOG_I(MODULE, "отчёт сохранён: %s", path);
    return 0;
}

/* 3. Покупатели с суммой покупок выше порога */
int vt_report_big_customers(const vt_analysis_t *a, const char *path)
{
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    report_header(f, "Покупатели сверх порога (функция 3)", a);
    fprintf(f, "Порог: %.2f руб.\n\n", a->threshold_sum);
    size_t n = vt_htab_count(a->cust_agg);
    int *keys = calloc(n + 1, sizeof *keys);
    size_t nk = vt_htab_keys(a->cust_agg, keys, n);
    int found = 0;
    for (size_t i = 0; i < nk; i++) {
        vt_cust_agg_t *ca = vt_htab_get(a->cust_agg, keys[i]);
        if (!ca || ca->total_sum <= a->threshold_sum) continue;
        vt_customer_t *cu = vt_htab_get(a->customers, keys[i]);
        fprintf(f, " C#%-5d %-28s покупок=%-4ld сумма=%12.2f руб%s%s\n",
                keys[i], cu ? cu->fio : "?", ca->purchases, ca->total_sum,
                cu ? ", регион: " : "", cu ? cu->region : "");
        found++;
    }
    free(keys);
    if (!found) fprintf(f, "(нет покупателей, превысивших порог)\n");
    fprintf(f, "\nИтого таких покупателей: %d\n", found);
    fclose(f);
    LOG_I(MODULE, "отчёт сохранён: %s (найдено %d)", path, found);
    return 0;
}

/* 4. Тренд продаж */
int vt_report_trend(const vt_analysis_t *a, const char *path)
{
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    report_header(f, "Тенденции продаж (функция 4)", a);
    struct tm t1, t2;
    localtime_r(&a->min_ts, &t1);
    localtime_r(&a->max_ts, &t2);
    char s1[32], s2[32];
    strftime(s1, sizeof s1, "%Y-%m-%d", &t1);
    strftime(s2, sizeof s2, "%Y-%m-%d", &t2);
    fprintf(f, "Период данных:      %s .. %s\n", s1, s2);
    fprintf(f, "Наклон МНК:         %+.2f руб/день\n", a->trend_slope_per_day);
    fprintf(f, "Выручка 1-й половины: %12.2f руб\n", a->first_half_sum);
    fprintf(f, "Выручка 2-й половины: %12.2f руб\n", a->second_half_sum);
    const char *verdict = "СТАБИЛЬНО";
    if (a->second_half_sum > a->first_half_sum * 1.05) verdict = "РОСТ";
    else if (a->second_half_sum < a->first_half_sum * 0.95) verdict = "СПАД";
    fprintf(f, "Заключение:         продажи %s\n", verdict);
    fclose(f);
    LOG_I(MODULE, "отчёт сохранён: %s (%s)", path, verdict);
    return 0;
}

/* 6 (ТЗ): полный сводный отчёт = все разделы в один txt.
 * Функции отчётов параметризованы путём к файлу, поэтому для сводки мы
 * сначала генерируем партиальные отчёты во временные файлы, а затем
 * вклеиваем их содержимое — переиспользование без дублирования логики. */
int vt_report_full(const vt_analysis_t *a, const char *path)
{
    int rc = 0;
    /* кладём партиалы рядом с целевым сводным отчётом */
    char dir[512] = ".";
    const char *slash = strrchr(path, '/');
    if (slash) {
        size_t dlen = (size_t)(slash - path);
        if (dlen < sizeof dir) { memcpy(dir, path, dlen); dir[dlen] = '\0'; }
    }
    char part[600];
    struct { const char *name; int (*fn)(const vt_analysis_t *, const char *); }
        parts[] = {
            {"total.txt",        vt_report_total},
            {"top_bottom.txt",   vt_report_top_bottom},
            {"big_customers.txt",vt_report_big_customers},
            {"trend.txt",        vt_report_trend},
        };
    FILE *out = fopen(path, "w");
    if (!out) return -1;
    report_header(out, "СВОДНЫЙ ОТЧЁТ (все функции ТЗ)", a);
    for (size_t i = 0; i < sizeof parts / sizeof *parts; i++) {
        snprintf(part, sizeof part, "%s/.%s.tmp", dir, parts[i].name);
        if (parts[i].fn(a, part) != 0) { rc = -1; continue; }
        FILE *pf = fopen(part, "r");
        if (!pf) { rc = -1; continue; }
        char line[VT_MAX_LINE];
        int hdr = 0;
        while (fgets(line, sizeof line, pf)) {
            /* вырезаем шапку партиального отчёта — в сводке она своя */
            if (hdr < 6) { hdr++; continue; }
            fputs(line, out);
        }
        fclose(pf);
        remove(part);
        fputc('\n', out);
    }
    fclose(out);
    LOG_I(MODULE, "сводный отчёт сохранён: %s (rc=%d)", path, rc);
    return rc;
}
