/* threadpool.c — пул потоков с очередью задач (см. include/threadpool.h) */
#include "threadpool.h"

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

typedef struct tnode {
    tp_task_fn fn;
    void      *arg;
    struct tnode *next;
} tnode_t;

struct tp_pool {
    pthread_t       *threads;
    size_t           nthreads;

    pthread_mutex_t  mtx;
    pthread_cond_t   cv_work;   /* рабочие ждут задач            */
    pthread_cond_t   cv_idle;   /* заказчик ждёт окончания очереди */

    tnode_t         *qhead, *qtail;
    size_t           pending;   /* задач в очереди + выполняется  */
    int              shutdown;
};

static void push_task(tp_pool_t *p, tp_task_fn fn, void *arg)
{
    tnode_t *n = malloc(sizeof *n);
    if (!n) return;                    /* тихая потеря при OOM не критична */
    n->fn = fn; n->arg = arg; n->next = NULL;
    pthread_mutex_lock(&p->mtx);
    if (p->qtail) p->qtail->next = n; else p->qhead = n;
    p->qtail = n;
    p->pending++;
    pthread_cond_signal(&p->cv_work);
    pthread_mutex_unlock(&p->mtx);
}

static void *worker_main(void *arg)
{
    tp_pool_t *p = arg;
    for (;;) {
        pthread_mutex_lock(&p->mtx);
        while (!p->qhead && !p->shutdown)
            pthread_cond_wait(&p->cv_work, &p->mtx);
        if (p->shutdown && !p->qhead) {
            pthread_mutex_unlock(&p->mtx);
            return NULL;
        }
        tnode_t *n = p->qhead;
        p->qhead = n->next;
        if (!p->qhead) p->qtail = NULL;
        pthread_mutex_unlock(&p->mtx);

        n->fn(n->arg);                 /* выполнение вне мьютекса!          */

        free(n);
        pthread_mutex_lock(&p->mtx);
        if (--p->pending == 0)
            pthread_cond_broadcast(&p->cv_idle);
        pthread_mutex_unlock(&p->mtx);
    }
}

tp_pool_t *tp_create(size_t nworkers)
{
    if (nworkers == 0) nworkers = 1;
    tp_pool_t *p = calloc(1, sizeof *p);
    if (!p) return NULL;
    p->nthreads = nworkers;
    p->threads = calloc(nworkers, sizeof(pthread_t));
    pthread_mutex_init(&p->mtx, NULL);
    pthread_cond_init(&p->cv_work, NULL);
    pthread_cond_init(&p->cv_idle, NULL);
    for (size_t i = 0; i < nworkers; i++) {
        if (pthread_create(&p->threads[i], NULL, worker_main, p) != 0) {
            p->nthreads = i;           /* что создали — то и остановим */
            break;
        }
    }
    return p;
}

int tp_submit(tp_pool_t *p, tp_task_fn fn, void *arg)
{
    if (!p || !fn) return -EINVAL;
    push_task(p, fn, arg);
    return 0;
}

void tp_drain(tp_pool_t *p)
{
    if (!p) return;
    pthread_mutex_lock(&p->mtx);
    while (p->pending > 0)
        pthread_cond_wait(&p->cv_idle, &p->mtx);
    pthread_mutex_unlock(&p->mtx);
}

void tp_destroy(tp_pool_t *p)
{
    if (!p) return;
    tp_drain(p);
    pthread_mutex_lock(&p->mtx);
    p->shutdown = 1;
    pthread_cond_broadcast(&p->cv_work);
    pthread_mutex_unlock(&p->mtx);
    for (size_t i = 0; i < p->nthreads; i++)
        pthread_join(p->threads[i], NULL);
    pthread_mutex_destroy(&p->mtx);
    pthread_cond_destroy(&p->cv_work);
    pthread_cond_destroy(&p->cv_idle);
    free(p->threads);
    free(p);
}
