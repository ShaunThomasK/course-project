CC      ?= cc
CFLAGS  ?= -O2 -g
CFLAGS  += -std=gnu11 -Wall -Wextra -Wno-unused-parameter
LDLIBS  ?=

COMMON    := src/common/common.c src/common/proto.c src/common/discovery.c src/common/sha256.c
SCHEDULER := src/scheduler/scheduler.c src/scheduler/jobs.c src/scheduler/pass.c src/scheduler/cron.c
WORKER    := src/worker/worker.c src/worker/jobrun.c src/worker/cache.c
CLIENT    := src/client/client.c
HEADERS   := $(wildcard src/*/*.h)

TEST_SRCS := $(filter-out tests/programs/bigprog.c,$(wildcard tests/programs/*.c))
TEST_BINS := $(patsubst tests/programs/%.c,tests/programs/bin/%,$(TEST_SRCS))

.PHONY: all clean tests

all: scheduler worker client

scheduler: $(SCHEDULER) $(COMMON) $(HEADERS)
	$(CC) $(CFLAGS) -o $@ $(SCHEDULER) $(COMMON) $(LDLIBS)

worker: $(WORKER) $(COMMON) $(HEADERS)
	$(CC) $(CFLAGS) -o $@ $(WORKER) $(COMMON) $(LDLIBS)

client: $(CLIENT) $(COMMON) $(HEADERS)
	$(CC) $(CFLAGS) -o $@ $(CLIENT) $(COMMON) $(LDLIBS)

BIG_BINS  := tests/programs/bin/big_a tests/programs/bin/big_b tests/programs/bin/big_c

tests: $(TEST_BINS) $(BIG_BINS)

tests/programs/bin/big_%: tests/programs/bigprog.c
	@mkdir -p tests/programs/bin
	$(CC) -O0 -DVARIANT='"$*"' -o $@ $<

tests/programs/bin/%: tests/programs/%.c
	@mkdir -p tests/programs/bin
	$(CC) -O0 -g -pthread -o $@ $<

clean:
	rm -f scheduler worker client
	rm -rf tests/programs/bin
