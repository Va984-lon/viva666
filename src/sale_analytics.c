/* ============================================================================
 * sale_analytics.c — ВТОРАЯ ПРОГРАММА: интерактивный многопоточный сервер
 *                   аналитики продаж "Vektra-ANALYTICS"
 *
 * Индивидуальность: это не «однократный скрипт», а REPL-сервер (как в
 * реальных BI-системах): анализатор живёт, периодически перезапускает
 * параллельные прогоны и позволяет пользователю МЕНЯТЬ ПАРАМЕТРЫ НА ХОДУ
 * (требование 5 ТЗ) командами из консоли, не прерывая работу потоков.
 *
 * Распределение по потокам:
 *   - main-поток          — REPL (команды пользователя) + диспетчер;
 *   - фоновый поток-«автопилот» — периодические прогоны анализа по таймеру;
 *   - пул N воркеров      — параллельный парсинг/агрегация чанков файла
 *                          продаж (число N меняется командой `workers`).
 * ==========================================================================*/
#include "analytics.h"
#include "logger.h"

#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define MODULE "cli"

static vt_analysis_t *g_ctx        = NULL;
static const char    *g_sales_file = "data/sales.log";
static volatile sig_atomic_t g_quit = 0;
static pthread_mutex_t g_print_mtx = PTHREAD_MUTEX_INITIALIZER;

static void on_sigint(int sig) { (void)sig; g_quit = 1; }

/* ---------------------- печать сводки в консоль -------------------------- */
static void print_summary(void)
{
    pthread_mutex_lock(&g_print_mtx);
    printf("\n---------------- СВОДКА ------------------------------\n");
    printf(" Потоков парсера : %d | Топ-N: %d | Порог: %.0f руб\n",
           g_ctx->n_workers, g_ctx->top_n, g_ctx->threshold_sum);
    printf(" Всего продаж    : %ld\n", g_ctx->total_sales_count);
    printf(" Текущая сумма   : %.2f руб.\n", g_ctx->total_revenue);
    if (g_ctx->total_sales_count)
        printf(" Средний чек     : %.2f руб.\n",
               g_ctx->total_revenue / g_ctx->total_sales_count);
    printf(" Пропущено строк : %ld\n", g_ctx->skipped_lines);
    printf(" Тренд           : %+.2f руб/день\n", g_ctx->trend_slope_per_day);
    printf("------------------------------------------------------\n");
    fflush(stdout);
    pthread_mutex_unlock(&g_print_mtx);
}

static void cmd_run(size_t chunk_kb)
{
    size_t chunk = chunk_kb ? chunk_kb * 1024 : 0; /* 0 => default 1 MiB */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    int rc = vt_analytics_run_pass(g_ctx, g_sales_file, chunk, 0);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    if (rc == 0) {
        double ms = (t1.tv_sec - t0.tv_sec) * 1000.0
                  + (t1.tv_nsec - t0.tv_nsec) / 1e6;
        LOG_I(MODULE, "анализ выполнен за %.1f мс (%d потоками)",
              ms, g_ctx->n_workers);
        print_summary();
    } else {
        LOG_E(MODULE, "прогон анализа завершился с ошибкой rc=%d", rc);
    }
}

static void save_all_reports(void)
{
    mkdir("reports", 0775);
    vt_report_total(g_ctx,      "reports/total.txt");
    vt_report_top_bottom(g_ctx, "reports/top_bottom.txt");
    vt_report_big_customers(g_ctx,"reports/big_customers.txt");
    vt_report_trend(g_ctx,      "reports/trend.txt");
    vt_report_full(g_ctx,       "reports/full_report.txt");
    printf(" Отчёты сохранены в каталог reports/\n");
}

static void help(void)
{
    printf(
    "Команды:\n"
    "  run [чанк_КБ]         — параллельный прогон анализа файла продаж\n"
    "  total                 — отчёт 1: текущая сумма всех продаж\n"
    "  top                   — отчёт 2: топ-N популярных и непопулярных\n"
    "  big                   — отчёт 3: покупатели сверх порога\n"
    "  trend                 — отчёт 4: тенденции продаж\n"
    "  report all            — отчёт 6: все txt-отчёты в reports/\n"
    "  workers N             — изменить число потоков-воркеров (функция 5)\n"
    "  threshold SUM         — изменить порог покупателей\n"
    "  topn N                — изменить N для топа/антитопa\n"
    "  auto SEC [чанк_КБ]    — включить автопилот: прогон каждые SEC секунд\n"
    "  off                   — выключить автопилот\n"
    "  status                — сводка последних результатов\n"
    "  help                  — эта справка\n"
    "  quit                  — выход\n");
}

/* ------------------ фоновый поток «автопилот» (ф-я 5 ТЗ) ----------------- */
struct autopilot_arg { int sec; size_t chunk_kb; };
static volatile int g_auto_on = 0;

static void *autopilot_thread(void *arg)
{
    struct autopilot_arg *ap = arg;
    LOG_I(MODULE, "автопилот запущен: интервал %d с", ap->sec);
    while (g_auto_on && !g_quit) {
        for (int s = 0; s < ap->sec && g_auto_on && !g_quit; s++) sleep(1);
        if (!g_auto_on || g_quit) break;
        cmd_run(ap->chunk_kb);
    }
    free(ap);
    LOG_I(MODULE, "автопилот остановлен");
    return NULL;
}

/* чтение строки без зависания на длинных токенах */
static int read_line(char *buf, size_t cap)
{
    if (!fgets(buf, (int)cap, stdin)) return 0;   /* EOF */
    buf[strcspn(buf, "\r\n")] = '\0';
    return 1;
}

int main(int argc, char **argv)
{
    const char *products_csv  = "data/products.csv";
    const char *customers_csv = "data/customers.csv";
    const char *log_path      = "logs/analytics.log";

    for (int i = 1; i < argc; i++) {
        if     (!strcmp(argv[i], "-s") && i+1 < argc) g_sales_file  = argv[++i];
        else if(!strcmp(argv[i], "-p") && i+1 < argc) products_csv  = argv[++i];
        else if(!strcmp(argv[i], "-c") && i+1 < argc) customers_csv = argv[++i];
        else if(!strcmp(argv[i], "-l") && i+1 < argc) log_path      = argv[++i];
        else if(!strcmp(argv[i], "-w") && i+1 < argc) { /* стартовое число потоков */
            /* зададим после создания контекста */
        }
    }

    log_init(log_path, LOG_DEBUG);
    signal(SIGINT, on_sigint);

    g_ctx = vt_analytics_new(products_csv, customers_csv);
    if (!g_ctx) {
        fprintf(stderr, "Не удалось инициализировать аналитику (см. лог).\n");
        return 1;
    }
    for (int i = 1; i < argc; i++)
        if (!strcmp(argv[i], "-w") && i + 1 < argc)
            g_ctx->n_workers = atoi(argv[i + 1]);

    printf("VEKTRA ANALYTICS v1.0 — интерактивный многопоточный анализатор\n");
    printf("Файл продаж: %s | товаров: %zu | покупателей: %zu\n",
           g_sales_file, vt_htab_count(g_ctx->products),
           vt_htab_count(g_ctx->customers));
    printf("Введите 'help' для списка команд.\n");

    char line[256];
    while (!g_quit) {
        printf("vektra> ");
        fflush(stdout);
        if (!read_line(line, sizeof line)) break;
        char cmd[64];
        long arg1 = 0;
        int nargs = sscanf(line, "%63s %ld", cmd, &arg1);
        if (nargs <= 0) continue;

        if (!strcmp(cmd, "quit") || !strcmp(cmd, "exit")) break;
        else if (!strcmp(cmd, "help")) help();
        else if (!strcmp(cmd, "run")) cmd_run((size_t)arg1);
        else if (!strcmp(cmd, "status")) print_summary();
        else if (!strcmp(cmd, "total"))
            printf(" Текущая сумма: %.2f руб. за %ld продаж\n",
                   g_ctx->total_revenue, g_ctx->total_sales_count);
        else if (!strcmp(cmd, "top"))
            vt_report_top_bottom(g_ctx, "reports/top_bottom.txt"),
            printf(" Сохранено: reports/top_bottom.txt\n");
        else if (!strcmp(cmd, "big"))
            vt_report_big_customers(g_ctx, "reports/big_customers.txt"),
            printf(" Сохранено: reports/big_customers.txt\n");
        else if (!strcmp(cmd, "trend"))
            vt_report_trend(g_ctx, "reports/trend.txt"),
            printf(" Сохранено: reports/trend.txt\n");
        else if (!strncmp(cmd, "report", 6)) save_all_reports();
        else if (!strcmp(cmd, "workers")) {
            pthread_mutex_lock(&g_ctx->lock);
            g_ctx->n_workers = (int)(arg1 > 0 ? arg1 : 4);
            pthread_mutex_unlock(&g_ctx->lock);
            printf(" Число потоков-воркеров: %d (применится со следующего run)\n",
                   g_ctx->n_workers);
            LOG_I(MODULE, "пользователь изменил n_workers=%d", g_ctx->n_workers);
        }
        else if (!strcmp(cmd, "threshold")) {
            pthread_mutex_lock(&g_ctx->lock);
            g_ctx->threshold_sum = (double)arg1;
            pthread_mutex_unlock(&g_ctx->lock);
            printf(" Порог: %.2f руб.\n", g_ctx->threshold_sum);
            LOG_I(MODULE, "пользователь изменил threshold=%.2f",
                  g_ctx->threshold_sum);
        }
        else if (!strcmp(cmd, "topn")) {
            pthread_mutex_lock(&g_ctx->lock);
            g_ctx->top_n = (int)(arg1 >= 1 ? arg1 : VT_TOP_N_DEFAULT);
            pthread_mutex_unlock(&g_ctx->lock);
            printf(" Топ-N: %d\n", g_ctx->top_n);
        }
        else if (!strcmp(cmd, "auto")) {
            if (!g_auto_on) {
                g_auto_on = 1;
                struct autopilot_arg *ap = malloc(sizeof *ap);
                ap->sec = arg1 > 0 ? (int)arg1 : 10;
                ap->chunk_kb = 0;
                pthread_t th;
                pthread_create(&th, NULL, autopilot_thread, ap);
                pthread_detach(th);
                printf(" Автопилот включён (каждые %d с).\n", ap->sec);
            }
        }
        else if (!strcmp(cmd, "off")) {
            g_auto_on = 0;
            printf(" Автопилот выключен.\n");
        }
        else printf(" Неизвестная команда '%s'. 'help' — список команд.\n", cmd);
    }

    g_auto_on = 0;
    g_quit = 1;
    vt_analytics_free(g_ctx);
    LOG_I(MODULE, "сервер завершил работу");
    log_shutdown();
    return 0;
}
