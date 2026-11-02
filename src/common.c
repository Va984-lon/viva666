/* common.c — формат данных, парсеры строк и хеш-таблица проекта "Vektra".
 * См. комментарии в include/common.h (модуль написан специально компактно:
 * это общий слой, переиспользуемый обеими программами). */
#include "common.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

const char *vt_category_name(int cat)
{
    switch (cat) {
    case CAT_GROCERY: return "Продукты";
    case CAT_ELECTRO: return "Электроника";
    case CAT_CLOTHES: return "Одежда";
    case CAT_BOOKS:   return "Книги";
    case CAT_HOME:    return "Товары для дома";
    case CAT_SPORT:   return "Спорт";
    default:          return "Без категории";
    }
}

/* ---------------------------- Парсеры строк ------------------------------ */

int vt_parse_sale_line(const char *line, vt_sale_t *out)
{
    /* Ожидаем VT1|sale_id|ts|customer_id|product_id */
    if (strncmp(line, VT_RECORD_PREFIX, strlen(VT_RECORD_PREFIX)) != 0)
        return -1;
    const char *p = line + strlen(VT_RECORD_PREFIX);
    char *end;
    long sale_id = strtol(p, &end, 10);
    if (*end != '|' || end == p) return -1;
    p = end + 1;
    long ts = strtol(p, &end, 10);
    if (*end != '|' || end == p) return -1;
    p = end + 1;
    long cust = strtol(p, &end, 10);
    if (*end != '|' || end == p) return -1;
    p = end + 1;
    long prod = strtol(p, &end, 10);
    if (end == p) return -1;
    out->sale_id     = sale_id;
    out->ts          = (time_t)ts;
    out->customer_id = (int)cust;
    out->product_id  = (int)prod;
    return 0;
}

size_t vt_format_sale_line(char *buf, size_t cap, const vt_sale_t *s)
{
    int n = snprintf(buf, cap, "%s%ld|%lld|%d|%d\n",
                     VT_RECORD_PREFIX, s->sale_id,
                     (long long)s->ts, s->customer_id, s->product_id);
    return (n > 0 && (size_t)n < cap) ? (size_t)n : 0;
}

/* Разбор CSV-поля до запятой/конца строки. */
static int csv_field(const char **pp, char *dst, size_t cap)
{
    const char *p = *pp;
    size_t i = 0;
    while (*p && *p != ',' && *p != '\n' && *p != '\r') {
        if (i + 1 < cap) dst[i++] = *p;
        p++;
    }
    dst[i] = '\0';
    if (**pp == *p && *p == ',') p++;      /* съедаем разделитель */
    *pp = p;
    return (int)i;
}

int vt_parse_customer_line(const char *line, vt_customer_t *out)
{
    memset(out, 0, sizeof(*out));
    char f[64];
    const char *p = line;
    if (csv_field(&p, f, sizeof f) <= 0) return -1;
    out->id = atoi(f);
    csv_field(&p, out->fio, sizeof out->fio);
    csv_field(&p, out->region, sizeof out->region);
    csv_field(&p, f, sizeof f);
    out->discount_pct = atoi(f);
    return out->id > 0 ? 0 : -1;
}

int vt_parse_product_line(const char *line, vt_product_t *out)
{
    memset(out, 0, sizeof(*out));
    char f[64];
    const char *p = line;
    if (csv_field(&p, f, sizeof f) <= 0) return -1;
    out->id = atoi(f);
    csv_field(&p, out->name, sizeof out->name);
    csv_field(&p, f, sizeof f);
    out->category = atoi(f);
    csv_field(&p, f, sizeof f);
    out->unit_price = strtod(f, NULL);
    csv_field(&p, f, sizeof f);
    out->stock = atoi(f);
    return out->id > 0 ? 0 : -1;
}

/* ------------------------------ Хеш-таблица ------------------------------ */
/* Открытая адресация, линейное пробирование. Значения хранятся как void*. */

struct hcell {
    int   key;
    char  used;
    void *val;
};

struct vt_htab {
    struct hcell *cells;
    size_t cap;
    size_t fill;
};

static size_t next_pow2(size_t n)
{
    size_t c = 16;
    while (c < n) c <<= 1;
    return c;
}

vt_htab_t *vt_htab_new(size_t capacity)
{
    vt_htab_t *h = calloc(1, sizeof *h);
    if (!h) return NULL;
    h->cap = next_pow2(capacity ? capacity : 64);
    h->cells = calloc(h->cap, sizeof *h->cells);
    if (!h->cells) { free(h); return NULL; }
    return h;
}

void vt_htab_free(vt_htab_t *h, void (*val_dtor)(void *))
{
    if (!h) return;
    if (val_dtor)
        for (size_t i = 0; i < h->cap; i++)
            if (h->cells[i].used) val_dtor(h->cells[i].val);
    free(h->cells);
    free(h);
}

static size_t hprobe(const struct hcell *cells, size_t cap, int key)
{
    size_t mask = cap - 1;
    /* смешивание: умножение на простой константу + сдвиг (Knuth-style) */
    unsigned h = (unsigned)(key * 2654435761u);
    size_t idx = h & mask;
    while (cells[idx].used && cells[idx].key != key)
        idx = (idx + 1) & mask;
    return idx;
}

int vt_htab_put(vt_htab_t *h, int key, void *val)
{
    /* рост при заполнении > 70% */
    if (h->fill * 10 >= h->cap * 7) {
        size_t newcap = h->cap << 1;
        struct hcell *nc = calloc(newcap, sizeof *nc);
        if (!nc) return -1;
        for (size_t i = 0; i < h->cap; i++) {
            if (!h->cells[i].used) continue;
            size_t j = hprobe(nc, newcap, h->cells[i].key);
            nc[j] = h->cells[i];
        }
        free(h->cells);
        h->cells = nc;
        h->cap = newcap;
    }
    size_t idx = hprobe(h->cells, h->cap, key);
    if (!h->cells[idx].used) {
        h->cells[idx].used = 1;
        h->cells[idx].key = key;
        h->fill++;
    }
    h->cells[idx].val = val;
    return 0;
}

void *vt_htab_get(const vt_htab_t *h, int key)
{
    size_t idx = hprobe(h->cells, h->cap, key);
    return h->cells[idx].used ? h->cells[idx].val : NULL;
}

size_t vt_htab_count(const vt_htab_t *h) { return h->fill; }

void vt_htab_iter(vt_htab_t *h, vt_htab_iter_fn fn, void *user)
{
    for (size_t i = 0; i < h->cap; i++)
        if (h->cells[i].used)
            fn(h->cells[i].key, h->cells[i].val, user);
}

size_t vt_htab_keys(const vt_htab_t *h, int *dst, size_t dst_cap)
{
    size_t n = 0;
    for (size_t i = 0; i < h->cap && n < dst_cap; i++)
        if (h->cells[i].used)
            dst[n++] = h->cells[i].key;
    return n;
}

/* ---------------- Атомарный счётчик ID продаж (для симулятора) ----------- */
/* Индивидуальность: ID выдаются без мьютекса — лёгкий CAS-цикл. Потоки
 * «кассиров» никогда не получают одинаковый sale_id даже при гонке. */
static atomic_long g_sale_id_counter = ATOMIC_VAR_INIT(1);

long vt_next_sale_id(void)
{
    return atomic_fetch_add_explicit(&g_sale_id_counter, 1, memory_order_relaxed);
}

void vt_reset_sale_id(long start_from)
{
    atomic_store(&g_sale_id_counter, start_from > 0 ? start_from : 1);
}
