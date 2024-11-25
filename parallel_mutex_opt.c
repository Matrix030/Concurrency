/************ parallel_mutex_opt.c ************/ 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
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
pthread_mutex_t bu_mu[NUM_BUCKETS];   // Mutex for managing writes in each bucket
pthread_mutex_t re_mu[NUM_BUCKETS];   // Mutex for managing concurrent readers
int ac_re[NUM_BUCKETS] = {0};       // Counter for active readers in each bucket

void panic(char *msg) {
    printf("%s\n", msg);
    exit(1);
}

double now() {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Inserts a key-value pair into the table
void insert(int key, int val) {
    int i = key % NUM_BUCKETS;
    bucket_entry *e = (bucket_entry *)malloc(sizeof(bucket_entry));
    if (!e) panic("No memory to allocate bucket!");

    pthread_mutex_lock(&bu_mu[i]); // Lock bucket for exclusive access
    e->next = table[i];
    e->key = key;
    e->val = val;
    table[i] = e;
    pthread_mutex_unlock(&bu_mu[i]); // Release lock
}

// Retrieves an entry from the hash table by key
bucket_entry *retrieve(int key) {
    int i = key % NUM_BUCKETS;

    // Enter reader section
    pthread_mutex_lock(&re_mu[i]);
    ac_re[i]++;
    if (ac_re[i] == 1) {
        pthread_mutex_lock(&bu_mu[i]); // First reader locks the bucket
    }
    pthread_mutex_unlock(&re_mu[i]);

    // Search for the key in the bucket
    bucket_entry *b = table[i];
    while (b) {
        if (b->key == key) {
            // Exit reader section
            pthread_mutex_lock(&re_mu[i]);
            ac_re[i]--;
            if (ac_re[i] == 0) {
                pthread_mutex_unlock(&bu_mu[i]); // Last reader unlocks the bucket
            }
            pthread_mutex_unlock(&re_mu[i]);
            return b;
        }
        b = b->next;
    }

    // Exit reader section if key not found
    pthread_mutex_lock(&re_mu[i]);
    ac_re[i]--;
    if (ac_re[i] == 0) {
        pthread_mutex_unlock(&bu_mu[i]);
    }
    pthread_mutex_unlock(&re_mu[i]);

    return NULL;
}

void *put_phase(void *arg) {
    long tid = (long)arg;
    int key = 0;

    for (key = tid; key < NUM_KEYS; key += num_threads) {
        insert(keys[key], tid);
    }

    pthread_exit(NULL);
}

void *get_phase(void *arg) {
    long tid = (long)arg;
    int key = 0;
    long lost = 0;

    for (key = tid; key < NUM_KEYS; key += num_threads) {
        if (retrieve(keys[key]) == NULL) lost++;
    }
    printf("[thread %ld] %ld keys lost!\n", tid, lost);

    pthread_exit((void *)lost);
}

int main(int argc, char **argv) {
    long i;
    pthread_t *threads;
    double start, end;

    if (argc != 2) {
        panic("usage: ./parallel_mutex_opt <num_threads>");
    }
    if ((num_threads = atoi(argv[1])) <= 0) {
        panic("must enter a valid number of threads to run");
    }

    for (i = 0; i < NUM_BUCKETS; i++) {
        pthread_mutex_init(&bu_mu[i], NULL);
        pthread_mutex_init(&re_mu[i], NULL);
        ac_re[i] = 0;
    }

    srandom(time(NULL));
    for (i = 0; i < NUM_KEYS; i++) {
        keys[i] = random();
    }

    threads = (pthread_t *)malloc(sizeof(pthread_t) * num_threads);
    if (!threads) {
        panic("out of memory allocating thread handles");
    }

    // Insert keys in parallel
    start = now();
    for (i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, put_phase, (void *)i);
    }

    // Barrier
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    end = now();
    printf("[main] Inserted %d keys in %f seconds\n", NUM_KEYS, end - start);

    // Reset the thread array
    memset(threads, 0, sizeof(pthread_t) * num_threads);

    // Retrieve keys in parallel
    start = now();
    long total_lost = 0;
    long *lost_keys = (long *)malloc(sizeof(long) * num_threads);
    for (i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, get_phase, (void *)i);
    }

    // Collect count of lost keys
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], (void **)&lost_keys[i]);
        total_lost += lost_keys[i];
    }
    end = now();

    printf("[main] Retrieved %ld/%d keys in %f seconds\n", NUM_KEYS - total_lost, NUM_KEYS, end - start);

    for (i = 0; i < NUM_BUCKETS; i++) {
        pthread_mutex_destroy(&bu_mu[i]);
        pthread_mutex_destroy(&re_mu[i]);
    }

    free(threads);
    free(lost_keys);

    return 0;
}
