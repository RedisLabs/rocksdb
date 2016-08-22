#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "rocksdb/c.h"

#include <unistd.h>  // sysconf() - get CPU count

#define  DBPath "/var/opt/redislabs/flash/badger"
#define keyspace 50000000
#define BULK 1 //100 //bulk write
#define read_threads 64

rocksdb_t *db;


typedef struct tasks {
    int start;
    int count;
}task;

void *ThreadEntryPoint(void *arg) {
    task *t= (task*)arg;
    int c;
    char* err = NULL;
    for (c=0; c<t->count; c++) {
        // Get value
        int i = t->start+c;
        rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
        size_t len;
        char key[32];
        sprintf(key, "memtier-%d", i);
        char *returned_value = rocksdb_get(db, readoptions, key, strlen(key), &len, &err);
        if(err) printf("error reading %s", err);
        assert(!err);
        free(returned_value);
        rocksdb_readoptions_destroy(readoptions);
        if(i%1000000==0) {
            printf("*"); fflush(stdout);
        }
    }
}

#include <pthread.h>
void spawnThreads( ) {
    int   i;
    pthread_t threads[read_threads];
    pthread_attr_t threads_attr;
    pthread_attr_init(&threads_attr);

    int k=0;
    for(i=0; i<read_threads; i++) {
        task* t = malloc(sizeof(task));
        t->start = k;
        t->count = keyspace/read_threads;
        k+=t->count;
        pthread_create(&threads[i],&threads_attr,ThreadEntryPoint,t);
    }
    for(i=0; i<read_threads; i++) {
        pthread_join(threads[i], NULL);
    }
}

int main(int argc, char **argv) {
    int i;
    char *err = NULL;
    rocksdb_options_t *options = rocksdb_options_create();



    /* default settings*/
    int rocks_memtable_memory_budget = 128*1024*1024;
    int rocks_level0_slowdown_writes_trigger = 12;
    float rocks_soft_rate_limit = 1.5;
    int rocks_info_log_level = 3; // ERROR. (default is INFO) - DEBUG=0,INFO,WARN,ERROR,FATAL,HEADER
    int rocks_compaction_readahead_size = 2<<20; /* 2mb */
    int rocks_allow_os_buffer = 0;
    int rocks_num_flushing_threads = 1; /* threads that serve only flashes (not for compaction) */
    int rocks_max_background_flushes = 1;
    int rocks_max_background_compactions = 64;
    rocksdb_options_increase_parallelism(options, 64);
#if 0    
    /* rocksdb_options_increase_parallelism(options, num_threads); - we now use state->env instead of this call */
    rocksdb_env_set_background_threads(state->env, num_threads); /* these threads serve both compactions and flashes */
    rocksdb_env_set_high_priority_background_threads(state->env, rocks_num_flushing_threads); /* these threads serve only flushes */
    rocksdb_options_set_env(options, state->env);
    rocksdb_options_set_max_background_flushes(options, rocks_max_background_flushes);
    rocksdb_options_set_max_background_compactions(options, rocks_max_background_compactions);
#endif
#define NUM_OF_LEVELS  7 /* this is the default number of level */
    rocksdb_options_set_num_levels(options, NUM_OF_LEVELS);
    rocksdb_options_optimize_level_style_compaction(options, rocks_memtable_memory_budget);
    rocksdb_options_set_max_open_files(options, -1);
    rocksdb_options_set_allow_os_buffer(options, rocks_allow_os_buffer );
    rocksdb_options_set_optimize_filters_for_hits(options, 1);
    rocksdb_options_compaction_readahead_size(options, rocks_compaction_readahead_size);

    rocksdb_options_set_db_log_dir(options, DBPath);
    rocksdb_options_set_info_log_level(options, rocks_info_log_level);
    rocksdb_options_set_stats_dump_period_sec(options, 10);
    // max log size is 1MB
    rocksdb_options_set_max_log_file_size(options, 100*1024);  // 100KB per file
    rocksdb_options_set_log_file_time_to_roll(options, 60*60*24); // 24 hours
    rocksdb_options_set_keep_log_file_num(options, 10);  // 10 files

    rocksdb_options_set_level0_slowdown_writes_trigger(options, rocks_level0_slowdown_writes_trigger);
    rocksdb_options_set_soft_rate_limit(options, rocks_soft_rate_limit);

    system("rm -rf "DBPath);

    rocksdb_options_set_create_if_missing(options, 1);
    rocksdb_options_set_error_if_exists(options, 1);

    /* disable compression */
    int compresionByLevel[NUM_OF_LEVELS];
    for (i=0; i<NUM_OF_LEVELS; i++)
        compresionByLevel[i]=rocksdb_no_compression;
    rocksdb_options_set_compression_per_level(options, compresionByLevel, NUM_OF_LEVELS);
    rocksdb_options_set_compression(options, rocksdb_no_compression);

    rocksdb_get_options_from_string(options, "base_background_compactions=64", options, &err);
    if(err) printf("error setting options %s", err);
    assert(!err);

    rocksdb_get_options_from_string(options, "soft_pending_compaction_bytes_limit=0;hard_pending_compaction_bytes_limit=0;", options, &err);
    if(err) printf("error setting options %s", err);
    assert(!err);

    // open DB
    db = rocksdb_open(options, DBPath, &err);
    if(err) printf("error opening %s", err);
    assert(!err);


    const char value[1024] = "value";
    int start = time(NULL);
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_writeoptions_disable_WAL(writeoptions, 1 );

    printf("writing\n");
    for (i=0; i<keyspace; i++) {
        // Put key-value
        char key[32];
        if(BULK==1) {
            sprintf(key, "memtier-%d", i);
            rocksdb_put(db, writeoptions, key, sizeof(key), value, sizeof(value),
                        &err);
            if(err) printf("error writing %s", err);
            assert(!err);
        }
        else {
            rocksdb_writebatch_t* b = rocksdb_writebatch_create();
            int j = 0;
            for (j=0; j<BULK; j++) {
                sprintf(key, "memtier-%d", i+j);
                rocksdb_writebatch_put(b, key, sizeof(key), value, sizeof(value));
            }
            rocksdb_write(db, writeoptions, b, &err);
            rocksdb_writebatch_destroy(b);
            i+=BULK-1;
        }
        if(i%1000000==0) {
            printf("*"); fflush(stdout);
        }
    }
    printf("\n"); fflush(stdout);
    char* stats = rocksdb_property_value(db, "rocksdb.levelstats");
    printf("%s",stats);
    free(stats);
    stats = rocksdb_property_value(db, "rocksdb.stats");
    printf("%s",stats);
    free(stats);

    rocksdb_writeoptions_destroy(writeoptions);

    printf("\nwrite time: %d\r\n", (int)(time(NULL)-start));



    start = time(NULL);
    printf("read iterator\n");
    rocksdb_iterator_t* iterator;
    rocksdb_readoptions_t *ro = rocksdb_readoptions_create();
    rocksdb_readoptions_set_readahead_size(ro, 2<<20);
    iterator = rocksdb_create_iterator(db, ro);
    rocksdb_readoptions_destroy(ro);
    rocksdb_iter_seek_to_first(iterator);
    i=0;
    while (rocksdb_iter_valid(iterator)){
        // Get value
        void* out_key; size_t out_keylen; void *out_val; size_t out_vallen;
        out_key = (void*)rocksdb_iter_key(iterator, &out_keylen);
        out_val = (void*)rocksdb_iter_value(iterator, &out_vallen);
        if(i%1000000==0) {
            printf("*"); fflush(stdout);
        }
        i++;
        rocksdb_iter_next(iterator);
    }
    rocksdb_iter_destroy(iterator);
    printf("\n"); fflush(stdout);

    stats = rocksdb_property_value(db, "rocksdb.levelstats");
    printf("%s",stats);
    free(stats);
    stats = rocksdb_property_value(db, "rocksdb.stats");
    printf("%s",stats);
    free(stats);

    printf("\nread iterator time: %d\r\n", (int)(time(NULL)-start));



    start = time(NULL);
    printf("reading\n");


    if(read_threads>1) {
        spawnThreads();
    }
    else   {
        for (i=0; i<keyspace; i++) {
            // Get value
            rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
            rocksdb_readoptions_set_readahead_size(readoptions, 2<<20);
            size_t len;
            char key[32];
            sprintf(key, "memtier-%d", i);
            char *returned_value =
                    rocksdb_get(db, readoptions, key, strlen(key), &len, &err);
            if(err) printf("error reading %s", err);
            assert(!err);
            free(returned_value);
            rocksdb_readoptions_destroy(readoptions);
            if(i%1000000==0) {
                printf("*"); fflush(stdout);
            }
        }

    }
    printf("\n"); fflush(stdout);

    stats = rocksdb_property_value(db, "rocksdb.levelstats");
    printf("%s",stats);
    free(stats);
    stats = rocksdb_property_value(db, "rocksdb.stats");
    printf("%s",stats);
    free(stats);


    printf("\nread time: %d\r\n", (int)(time(NULL)-start));

    rocksdb_options_destroy(options);
    rocksdb_close(db);


    return 0;
}
