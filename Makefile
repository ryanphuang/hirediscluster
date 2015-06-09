TARGET := example
OBJS := example.o
DEPS := example.d

all: $(TARGET)

$(TARGET): $(OBJS)
	cd hiredis
	g++ $^ -o $@ -Lhiredis -static -lhiredis

-include $(DEPS)

%.o :%.cc
	g++ -MMD -c -g -O0 -I hiredis $< -o $@

clean:
	rm -f *.o *.d $(TARGET)

.PHONY: all clean
