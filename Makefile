# ============================================================================
# Vektra Sales Platform — сборка двух многопоточных программ (Си, POSIX)
#   bin/sale_sim       — симулятор продаж  (поток-«кассир» на каждый товар)
#   bin/sale_analytics — анализатор продаж  (пул потоков + автопилот + REPL)
# ============================================================================
CC       ?= gcc
CFLAGS   ?= -std=c11 -O2 -Wall -Wextra -Wpedantic
CPPFLAGS += -Iinclude -D_GNU_SOURCE -D_POSIX_C_SOURCE=200809L
LDFLAGS  += -pthread
LDLIBS   += -lm

BUILD    := build
BIN      := bin

COMMON_SRC := src/common.c src/logger.c
OBJ        := $(COMMON_SRC:%.c=$(BUILD)/%.o)

.PHONY: all clean run-sim run-analyze test dirs

all: dirs $(BIN)/sale_sim $(BIN)/sale_analytics

dirs:
	@mkdir -p $(BUILD)/src $(BIN) data logs reports

$(BUILD)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

$(BIN)/sale_sim: $(BUILD)/src/sale_sim.o $(OBJ)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS) $(LDLIBS)

$(BIN)/sale_analytics: $(BUILD)/src/sale_analytics.o $(BUILD)/src/analytics.o \
                       $(BUILD)/src/threadpool.o $(OBJ)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS) $(LDLIBS)

# ---- удобные цели ----------------------------------------------------------
# сгенерировать историю продаж за 7 дней мгновенно (--fast)
run-sim: $(BIN)/sale_sim
	./$(BIN)/sale_sim --fast=7 -f data/sales.log

# неинтерактивный прогон анализа (команды подаются на stdin)
run-analyze: $(BIN)/sale_analytics
	printf 'run\nreport all\nquit\n' | ./$(BIN)/sale_analytics

# полный самопроверочный тест: чистые данные -> генерация -> анализ -> отчёты
test: all
	./tests/smoke.sh

clean:
	rm -rf $(BUILD) $(BIN)

distclean: clean
	rm -f data/sales.log logs/*.log reports/*.txt
