.SUFFIXES: .c .o

CC=gcc

SRCDIR=src/
INC=include/
LIBS=lib/

# main source file
TARGET_SRC:=$(SRCDIR)main.c
TARGET_OBJ:=$(SRCDIR)main.o

# Include more files if you write another source file.
SRCS_FOR_LIB:=$(SRCDIR)bpt.c $(SRCDIR)file.c $(SRCDIR)tests.c
OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.c=.o)

CFLAGS+= -g -fPIC -I $(INC)

TARGET=main
	
all: $(TARGET)
$(TARGET): $(TARGET_OBJ)
	make static_library
	$(CC) $(CFLAGS) -o $@ $^ -L $(LIBS) -lbpt

clean:
	rm $(TARGET) $(TARGET).exe $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*.a db

library:
	gcc -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

static_library: $(OBJS_FOR_LIB)
	ar cr $(LIBS)libbpt.a $(OBJS_FOR_LIB)