CC=g++
CFLAGS=-g -Wall
OBJS=server.o TableManager.o CSDScheduler.o buffer_manager.o
TARGET=a.out
 
$(TARGET): $(OBJS) 
	$(CC)	-o	$@	$(OBJS) -lpthread -lboost_thread

server.o: TableManager.h keti_type.h server.cc buffer_manager.cc
TableManager.o: TableManager.h TableManager.cc
CSDScheduler.o: buffer_manager.h CSDScheduler.cc
buffer_manager.o: buffer_manager.h buffer_manager.cc

clean:
	rm -f *.o