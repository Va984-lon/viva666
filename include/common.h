/* ============================================================================
 * common.h — общий слой предметной области проекта "Vektra"
 *
 * Индивидуальная черта практики: единое ядро данных (формат CSV + константы)
 * используется обеими программами — симулятором продаж (sale_sim) и
 * аналитическим сервером (sale_analytics). Модели данных описаны один раз,
 * что исключает расхождение форматов между "производителем" и "потребителем".
 * ==========================================================================*/
#ifndef VEKTRA_COMMON_H
#define VEKTRA_COMMON_H

#include <stddef.h>
#include <time.h>

/* ---------------------------------------------------------------------------
 * Версия формата данных. В каждую строку файла продаж пишется префикс
 * "VT1|". Это наш собственный "магический" префикс записи: он позволяет
 * анализатору отличать корректные записи от повреждённых/чужих строк и
 * эволюционировать формат без потери совместимости.
 * -------------------------------------------------------------------------*/
#define VT_RECORD_PREFIX    "VT1|"
#define VT_MAX_LINE         256
#define VT_MAX_NAME         48
#define VT_MAX_CUSTOMER     2048
#define VT_MAX_PRODUCT      512
#define VT_TOP_N_DEFAULT    5     /* сколько товаров в топе/антитопе по ТЗ */

/* Коды товарных категорий (словарь фиксирован, чтобы симулятор и
 * анализатор говорили на одном языке). */
typedef enum {
    CAT_GROCERY = 1,   /* продукты        */
    CAT_ELECTRO = 2,   /* электроника     */
    CAT_CLOTHES = 3,   /* одежда          */
    CAT_BOOKS   = 4,   /* книги           */
    CAT_HOME    = 5,   /* товары для дома */
    CAT_SPORT   = 6,   /* спорт           */
    CAT_COUNT   = 7
} vt_category_t;

const char *vt_category_name(int cat);

/* Запись о продаже: ID продажи, unix-время, код покупателя, код товара. */
typedef struct {
    long  sale_id;
    time_t ts;
    int   customer_id;
    int   product_id;
} vt_sale_t;

/* Покупатель (из customers.csv): id, fio, region, discount_pct */
typedef struct {
    int  id;
    char fio[VT_MAX_NAME];
    char region[24];
    int  discount_pct;
} vt_customer_t;

/* Товар (из products.csv): id, name, category, unit_price, stock */
typedef struct {
    int   id;
    char  name[VT_MAX_NAME];
    int   category;
    double unit_price;
    int   stock;
} vt_product_t;

/* Парсинг строки вида "VT1|<sale_id>|<ts>|<cust>|<prod>".
 * Возвращает 0 при успехе, -1 при ошибке формата. */
int  vt_parse_sale_line(const char *line, vt_sale_t *out);

/* Формирование строки продажи в буфер (для симулятора). */
size_t vt_format_sale_line(char *buf, size_t cap, const vt_sale_t *s);

/* Разбор строки customers.csv / products.csv. */
int  vt_parse_customer_line(const char *line, vt_customer_t *out);
int  vt_parse_product_line(const char *line, vt_product_t *out);

/* Простая хеш-таблица открытой адресации int -> void*.
 * Используется и как справочник товаров/покупателей, и как аккумулятор
 * агрегатов в многопоточных воркерах. */
typedef struct vt_htab vt_htab_t;
vt_htab_t *vt_htab_new(size_t capacity);
void       vt_htab_free(vt_htab_t *h, void (*val_dtor)(void *));
int        vt_htab_put(vt_htab_t *h, int key, void *val);   /* перезаписывает */
void      *vt_htab_get(const vt_htab_t *h, int key);        /* NULL если нет  */
size_t     vt_htab_count(const vt_htab_t *h);
/* Итерация: fn(key, val, user). */
typedef void (*vt_htab_iter_fn)(int key, void *val, void *user);
void       vt_htab_iter(vt_htab_t *h, vt_htab_iter_fn fn, void *user);

/* Снимок ключей таблицы (для безопасного обхода без знания внутренних
 * деталей реализации). Возвращает число записанных ключей. */
size_t     vt_htab_keys(const vt_htab_t *h, int *dst, size_t dst_cap);

/* Монотонный счётчик ID продаж для симулятора: общий атомарный счётчик,
 * распределённый между потоками-«кассирами» (CAS-loop на stdatomic). */
long       vt_next_sale_id(void);
void       vt_reset_sale_id(long start_from);

#endif /* VEKTRA_COMMON_H */
