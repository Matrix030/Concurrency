/************ parallel_spin.c ************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>

#define NUM_BUCKETS 5     // Buckets in hash table
#define NUM_KEYS 100000   // Number of keys inserted per thread
int num_threads = 1;      // Configurable thread count
int keys[NUM_KEYS];

typedef struct _bucket_entry {
    int key;
    int val;
    struct _bucket_entry *next;
} bucket_entry;

bucket_entry *table[NUM_BUCKETS];
pthread_spinlock_t bu_lks[NUM_BUCKETS];    // Spinlock for each bucket
pthread_spinlock_t re_lks[NUM_BUCKETS];    // Spinlock for reader access
int ac_re[NUM_BUCKETS] = {0};           // Reader count for each bucket

void panic(char *msg) {
  printf("%s\n", msg);
  exit(1);
}

double now() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

void insert(int key, int val) {
    int i = key % NUM_BUCKETS;
    bucket_entry *e = (bucket_entry *)malloc(sizeof(bucket_entry));
    if (!e) panic("No memory to allocate bucket!");

    pthread_spin_lock(&bu_lks[i]);  // Acquire bucket lock
    e->next = table[i];
    e->key = key;
    e->val = val;
    table[i] = e;
    pthread_spin_unlock(&bu_lks[i]); // Release bucket lock
}

// Retrieves an entry from the hash table by key
// Returns NULL if the key isn't found in the table
bucket_entry *retrieve(int key) {
    int i = key % NUM_BUCKETS;

    // Reader entry section
    pthread_spin_lock(&re_lks[i]);
    ac_re[i]++;
    if (ac_re[i] == 1) {
        pthread_spin_lock(&bu_lks[i]);  // First reader locks bucket
    }
    pthread_spin_unlock(&re_lks[i]);

    bucket_entry *b;
    for (b = table[i]; b!= NULL; b = b->next) {
        if (b->key == key) {
            // Reader exit section
            pthread_spin_lock(&re_lks[i]);
            ac_re[i]--;
            if (ac_re[i] == 0) {
                pthread_spin_unlock(&bu_lks[i]);  // Last reader unlocks bucket
            }
            pthread_spin_unlock(&re_lks[i]);
            return b;
        }
    }

    // Reader exit section
    pthread_spin_lock(&re_lks[i]);
    ac_re[i]--;
    if (ac_re[i] == 0) {
        pthread_spin_unlock(&bu_lks[i]);  // Last reader unlocks bucket
    }
    pthread_spin_unlock(&re_lks[i]);

    return NULL;
}

void * put_phase(void *arg) {
  long tid = (long) arg;
  int key = 0;

  // If there are k threads, thread i inserts
  //      (i, i), (i+k, i), (i+k*2)
  for (key = tid ; key < NUM_KEYS; key += num_threads) {
    insert(keys[key], tid);
  }

  pthread_exit(NULL);
}

void *get_phase(void *arg) {
    long tid = (long)arg;
    int key = 0;
    long lost = 0;

    for (key = tid ; key < NUM_KEYS; key += num_threads) {
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
        panic("Usage: ./parallel_spin <num_threads>");
    }

    if ((num_threads = atoi(argv[1])) <= 0) {
        panic("must enter a valid number of threads to run");
    }

    for (int i = 0; i < NUM_BUCKETS; i++) {
        pthread_spin_init(&bu_lks[i], 0);
        pthread_spin_init(&re_lks[i], 0);
        ac_re[i] = 0;
    }

    srandom(time(NULL));
    for (int i = 0; i < NUM_KEYS; i++) {
        keys[i] = random();
    }

    threads = (pthread_t *) malloc(sizeof(pthread_t)*num_threads);
    if (!threads) {
        panic("Out of memory allocating thread handles");
    }

    // Insert keys in parallel
    start = now();
    for (long i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, put_phase, (void *)i);
    }

    //Barrier
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    end = now();
    
    printf("[main] Inserted %d keys in %f seconds\n", NUM_KEYS, end - start);

    // Reset thread array
    memset(threads, 0, sizeof(pthread_t) * num_threads);

    // Retrieve keys in parallel
    start = now();
  for (i = 0; i < num_threads; i++) {
    pthread_create(&threads[i], NULL, get_phase, (void *)i);
  }

  // Collect count of lost keys
  long total_lost = 0;
  long *lost_keys = (long *) malloc(sizeof(long) * num_threads);
  for (i = 0; i < num_threads; i++) {
    pthread_join(threads[i], (void **)&lost_keys[i]);
    total_lost += lost_keys[i];
  }
  end = now();

    printf("[main] Retrieved %ld/%d keys in %f seconds\n", NUM_KEYS - total_lost, NUM_KEYS, end - start);

    for (int i = 0; i < NUM_BUCKETS; i++) {
        pthread_spin_destroy(&bu_lks[i]);
        pthread_spin_destroy(&re_lks[i]);
    }

    free(threads);
    free(lost_keys);

    return 0;
}
