/* ============================================================================
 * sale_sim.c — ПЕРВАЯ ПРОГРАММА: многопоточный симулятор продаж "Vektra-SIM"
 *
 * Индивидуальность («кассовая очередь»): каждый товар обслуживает свой
 * поток-«кассир». Поток имитирует продажи своего товара через случайные
 * интервалы (экспоненциальное распределение — модель пуассоновского потока
 * событий, классика теории массового обслуживания). Это более реалистично,
 * чем равномерные sleep(), и является осознанным отличием от типовых решений.
 *
 * Точная запись в общий файл обеспечивается POSIX-мьютексом вокруг fwrite;
 * уникальные ID продаж выдаются атомарным счётчиком без блокировок.
 *
 * Режим --fast генерирует историю за прошлые N дней мгновенно (для теста
 * тренда), обычный режим — реальное время «как в магазине».
 * ==========================================================================*/
#include "common.h"
#include "logger.h"

#include <errno.h>
#include <math.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define MODULE "sale_sim"

static FILE             *g_sales_fp   = NULL;
static pthread_mutex_t   g_write_mtx  = PTHREAD_MUTEX_INITIALIZER;
static volatile sig_atomic_t g_stop   = 0;          /* флаг остановки */
static double            g_intensity  = 1.0;        /* множитель частоты */
/* Виртуальные часы для режима --fast: общая монотонная метка времени,
 * защищённая мьютексом (продажа обязана иметь ts >= предыдущей). */
static time_t            g_vnow       = 0;
static int               g_fast_mode  = 0;
static pthread_mutex_t   g_vnow_mtx   = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    int          product_id;
    const char  *product_name;
    double       lambda;        /* среднее число продаж в час */
    unsigned     seed;          /* свой PRNG — без гонки на rand() */
    long         sales_done;
} cashier_t;

/* Свой ГПСЧ xorshift64* — дёшев и не требует блокировок (per-thread state). */
static uint64_t xs_next(uint64_t *s)
{
    *s ^= *s >> 12; *s ^= *s << 25; *s ^= *s >> 27;
    return *s * 2685821657845525933ULL;
}
static double xs_uniform(uint64_t *s)
{
    return (double)(xs_next(s) >> 11) / 9007199254740992.0; /* [0,1) */
}

/* Экспоненциально распределённый интервал до следующей продажи:
 * T = -ln(U)/lambda — модель пуассоновского потока. */
static double exp_interval(double lambda, uint64_t *rng)
{
    double u = xs_uniform(rng);
    if (u < 1e-12) u = 1e-12;
    return -log(u) / lambda;
}

static void write_sale(const vt_sale_t *s)
{
    char line[VT_MAX_LINE];
    size_t n = vt_format_sale_line(line, sizeof line, s);
    if (!n) return;
    pthread_mutex_lock(&g_write_mtx);
    fwrite(line, 1, n, g_sales_fp);
    fflush(g_sales_fp);                /* данные видны анализатору сразу */
    pthread_mutex_unlock(&g_write_mtx);
}

static int pick_customer(uint64_t *rng, int max_cust)
{
    return 1 + (int)(xs_uniform(rng) * (max_cust - 1));
}

/* Поток-«кассир»: бесконечная лента продаж своего товара. */
static void *cashier_thread(void *arg)
{
    cashier_t *c = arg;
    uint64_t rng = c->seed ? c->seed
                           : (uint64_t)time(NULL) ^ ((uint64_t)c->product_id << 32);

    LOG_I(MODULE, "касса T#%d открыта ('%s', lambda=%.2f/ч)",
          c->product_id, c->product_name, c->lambda);

    while (!g_stop) {
        double wait_h = exp_interval(c->lambda * g_intensity, &rng);
        double wait_s = wait_h * 3600.0;
        time_t ts;
        if (g_fast_mode) {
            /* не спим: сдвигаем общие виртуальные часы под мьютексом,
             * чтобы метки времени продаж были монотонны */
            pthread_mutex_lock(&g_vnow_mtx);
            g_vnow += (time_t)(wait_s >= 1.0 ? wait_s : 1.0);
            ts = g_vnow;
            pthread_mutex_unlock(&g_vnow_mtx);
        } else {
            struct timespec nap = {
                (time_t)wait_s,
                (long)((wait_s - (time_t)wait_s) * 1e9)
            };
            int rc;
            while ((rc = nanosleep(&nap, &nap)) == -1 && errno == EINTR)
                if (g_stop) break;       /* прерывание сигналом остановки */
            if (g_stop) break;
            ts = time(NULL);
        }
        if (g_stop) break;

        vt_sale_t s;
        s.sale_id     = vt_next_sale_id();
        s.ts          = ts;
        s.customer_id = pick_customer(&rng, 51);   /* id покупателей 1..50 */
        s.product_id  = c->product_id;
        write_sale(&s);
        c->sales_done++;

        if (c->sales_done % 50 == 0)
            LOG_D(MODULE, "касса T#%d: продано %ld шт.",
                  c->product_id, c->sales_done);
    }
    LOG_I(MODULE, "касса T#%d закрыта (продано %ld)",
          c->product_id, c->sales_done);
    return NULL;
}

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

static void usage(const char *argv0)
{
    fprintf(stderr,
        "Использование: %s [-f файл_продаж] [-t секунд] [-i интенсивность]\n"
        "               [--fast days=N] [-l логфайл] [-qQuiet]\n"
        "  -f PATH   файл продаж (по умолчанию data/sales.log)\n"
        "  -t SEC    работать SEC секунд и завершиться\n"
        "  -i FLOAT  множитель интенсивности продаж (быстрее/медленнее)\n"
        "  --fast N  генерировать историю за N прошедших дней без пауз\n"
        "  -l PATH   файл журнала (по умолчанию logs/sim.log)\n", argv0);
}

/* Поток-таймер: через N секунд поднимает флаг graceful shutdown.
 * Вынесен в отдельный поток, чтобы main не блокировался и мог переждать
 * join'ом все кассы. */
struct timer_arg { int sec; };

static void *timer_thread(void *arg)
{
    struct timer_arg *t = arg;
    for (int s = 0; s < t->sec && !g_stop; s++) sleep(1);
    LOG_I(MODULE, "таймер (%d с) истёк — останавливаем кассы", t->sec);
    g_stop = 1;
    free(t);
    return NULL;
}

int main(int argc, char **argv)
{
    const char *sales_path = "data/sales.log";
    const char *log_path   = "logs/sim.log";
    int run_seconds = 0;
    int fast_days = 0;
    log_level_t lvl = LOG_INFO;

    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "-f") && i + 1 < argc) sales_path = argv[++i];
        else if (!strcmp(argv[i], "-l") && i + 1 < argc) log_path = argv[++i];
        else if (!strcmp(argv[i], "-t") && i + 1 < argc) run_seconds = atoi(argv[++i]);
        else if (!strcmp(argv[i], "-i") && i + 1 < argc) g_intensity = atof(argv[++i]);
        else if (!strncmp(argv[i], "--fast", 6)) {
            const char *v = strchr(argv[i], '=');
            fast_days = v ? atoi(v + 1) : 7;
        }
        else if (!strcmp(argv[i], "-q")) lvl = LOG_WARN;
        else if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "--help")) {
            usage(argv[0]); return 0;
        }
        else { fprintf(stderr, "неизвестный аргумент: %s\n", argv[i]);
               usage(argv[0]); return 2; }
    }
    if (g_intensity <= 0) g_intensity = 1.0;

    log_init(log_path, lvl);
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    /* каталог данных создаём best-effort */
    mkdir("data", 0775);

    g_sales_fp = fopen(sales_path, "a");
    if (!g_sales_fp) {
        LOG_E(MODULE, "не открыть '%s': %s", sales_path, strerror(errno));
        return 1;
    }

    /* Каталог товаров: один «кассир» на товар. Берём из products.csv,
     * если он рядом, — иначе используем встроенный набор (автономность). */
    static cashier_t cashiers[64];
    static char      names_store[64][VT_MAX_NAME];   /* владеем строками имён */
    int n = 0;
    {
        /* popularity[id] — множитель частоты покупок товара (задан в
         * справочнике или выведен из цены); так симулятор генерирует
         * реалистичный разброс популярности для теста функций 2 и 4 */
        FILE *pc = fopen("data/products.csv", "r");
        if (pc) {
            char line[VT_MAX_LINE];
            while (fgets(line, sizeof line, pc) && n < 64) {
                if (line[0] == '#' || line[0] == '\n') continue;
                vt_product_t p;
                if (vt_parse_product_line(line, &p) == 0) {
                    cashiers[n].product_id = p.id;
                    snprintf(names_store[n], VT_MAX_NAME, "%s", p.name);
                    cashiers[n].product_name = names_store[n];
                    /* дешёвые продукты покупают чаще: lambda обратно цене;
                     * stock>0 даёт дополнительный множитель оборота */
                    double turn = p.stock > 0 ? (1.0 + p.stock / 500.0) : 1.0;
                    cashiers[n].lambda =
                        (0.3 + 40.0 / (1.0 + p.unit_price / 100.0)) * turn;
                    cashiers[n].seed = (unsigned)(p.id * 7919 + 13);
                    cashiers[n].sales_done = 0;
                    n++;
                }
            }
            fclose(pc);
            LOG_I(MODULE, "каталог загружен из data/products.csv: %d товаров", n);
        }
    }
    if (n == 0) {
        static const char *names[] = {"Хлеб","Молоко","Ноутбук","Флешка",
                                      "Футболка","Книга","Лампа","Гантели"};
        double lambdas[] = {6, 5, 0.4, 2, 1.5, 1.2, 1, 0.8};
        for (int i = 0; i < 8; i++) {
            cashiers[i].product_id = i + 1;
            cashiers[i].product_name = names[i];
            cashiers[i].lambda = lambdas[i];
            cashiers[i].seed = (unsigned)(i * 7919 + 13);
            cashiers[i].sales_done = 0;
        }
        n = 8;
        LOG_W(MODULE, "products.csv не найден — встроенный каталог (%d т.)", n);
    }

    if (fast_days > 0) {
        /* стартуем виртуальные часы N дней назад */
        pthread_mutex_lock(&g_vnow_mtx);
        g_vnow = time(NULL) - (time_t)fast_days * 86400;
        pthread_mutex_unlock(&g_vnow_mtx);
        g_fast_mode = 1;
        LOG_I(MODULE, "режим --fast: генерируем историю за %d дн.", fast_days);
    }

    pthread_t th[64];
    for (int i = 0; i < n; i++)
        pthread_create(&th[i], NULL, cashier_thread, &cashiers[i]);

    pthread_t timer = 0;
    int have_timer = 0;
    if (run_seconds > 0) {
        struct timer_arg *ta = malloc(sizeof *ta);
        ta->sec = run_seconds;
        pthread_create(&timer, NULL, timer_thread, ta);
        have_timer = 1;
    }
    /* main крутится до флага остановки (таймер или Ctrl-C) */
    while (!g_stop) sleep(1);
    g_stop = 1;
    if (have_timer) pthread_join(timer, NULL);

    for (int i = 0; i < n; i++)
        pthread_join(th[i], NULL);

    long total = 0;
    for (int i = 0; i < n; i++) total += cashiers[i].sales_done;
    LOG_I(MODULE, "симуляция завершена: всего продаж %ld в %s",
          total, sales_path);
    fclose(g_sales_fp);
    log_shutdown();
    return 0;
}
