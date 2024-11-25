#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>

#define NUM_BUCKETS 5     // Buckets in hash table
#define NUM_KEYS 100000   // Number of keys inserted per thread
int num_threads = 1;      // Number of threads (configurable)
int keys[NUM_KEYS];

typedef struct _bucket_entry {
    int key;
    int val;
    struct _bucket_entry *next;
} bucket_entry;

bucket_entry *table[NUM_BUCKETS];
pthread_spinlock_t bucket_locks[NUM_BUCKETS];  // Spinlocks for each bucket

void panic(const char *msg) {
    fprintf(stderr, "%s\n", msg);
    exit(EXIT_FAILURE);
}

double now() {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return tv.tv_sec + tv.tv_usec / 1e6;
}

void insert(int key, int val) {
    int i = key % NUM_BUCKETS;
    pthread_spin_lock(&bucket_locks[i]);  // Lock the bucket

    bucket_entry *e = (bucket_entry *)malloc(sizeof(bucket_entry));
    if (!e) panic("Memory allocation failed!");
    e->next = table[i];
    e->key = key;
    e->val = val;
    table[i] = e;

    pthread_spin_unlock(&bucket_locks[i]);  // Unlock the bucket
}

bucket_entry *retrieve(int key) {
    int i = key % NUM_BUCKETS;
    pthread_spin_lock(&bucket_locks[i]);  // Lock the bucket

    bucket_entry *b;
    for (b = table[i]; b != NULL; b = b->next) {
        if (b->key == key) {
            pthread_spin_unlock(&bucket_locks[i]);  // Unlock before returning
            return b;
        }
    }

    pthread_spin_unlock(&bucket_locks[i]);  // Unlock the bucket
    return NULL;
}

void *put_phase(void *arg) {
    long tid = (long)arg;
    for (int key = tid; key < NUM_KEYS; key += num_threads) {
        insert(keys[key], tid);
    }
    pthread_exit(NULL);
}

void *get_phase(void *arg) {
    long tid = (long)arg;
    long lost = 0;

    for (int key = tid; key < NUM_KEYS; key += num_threads) {
        if (!retrieve(keys[key])) lost++;
    }
    printf("[thread %ld] %ld keys lost!\n", tid, lost);

    pthread_exit((void *)lost);
}

int main(int argc, char **argv) {
    if (argc != 2) {
        panic("Usage: ./parallel_hashtable <num_threads>");
    }
    num_threads = atoi(argv[1]);
    if (num_threads <= 0) {
        panic("Invalid number of threads!");
    }

    // Initialize bucket spinlocks
    for (int i = 0; i < NUM_BUCKETS; i++) {
        pthread_spin_init(&bucket_locks[i], PTHREAD_PROCESS_PRIVATE);
    }

    srandom(time(NULL));
    for (int i = 0; i < NUM_KEYS; i++) {
        keys[i] = random();
    }

    pthread_t *threads = (pthread_t *)malloc(sizeof(pthread_t) * num_threads);
    if (!threads) panic("Out of memory allocating thread handles");

    // Insert keys in parallel
    double start = now();
    for (long i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, put_phase, (void *)i);
    }
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    double end = now();
    printf("[main] Inserted %d keys in %f seconds\n", NUM_KEYS, end - start);

    // Reset thread array
    memset(threads, 0, sizeof(pthread_t) * num_threads);

    // Retrieve keys in parallel
    start = now();
    long total_lost = 0;
    long *lost_keys = (long *)malloc(sizeof(long) * num_threads);
    for (long i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, get_phase, (void *)i);
    }
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], (void **)&lost_keys[i]);
        total_lost += lost_keys[i];
    }
    end = now();

    printf("[main] Retrieved %ld/%d keys in %f seconds\n", NUM_KEYS - total_lost, NUM_KEYS, end - start);

    // Destroy bucket spinlocks
    for (int i = 0; i < NUM_BUCKETS; i++) {
        pthread_spin_destroy(&bucket_locks[i]);
    }

    free(threads);
    free(lost_keys);

    return 0;
}
