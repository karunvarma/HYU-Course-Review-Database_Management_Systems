.SUFFIXES: .cc .o

CC=g++

SRCDIR=src/
INC=include/
LIBS=lib/

# main source file
TARGET_SRC:=$(SRCDIR)main.cc
TARGET_OBJ:=$(SRCDIR)main.o

# Include more files if you write another source file.
SRCS_FOR_LIB:=$(SRCDIR)index.cc $(SRCDIR)file.cc $(SRCDIR)tests.cc $(SRCDIR)buffer.cc $(SRCDIR)transaction.cc
OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.cc=.o)

CXXFLAGS+= -g -fPIC -I $(INC)

TARGET=main
	
all: $(TARGET)
$(TARGET): $(TARGET_OBJ)
	make static_library
	$(CC) $(CXXFLAGS) -o $@ $^ -L $(LIBS) -lbpt

clean:
	rm -f $(TARGET) $(TARGET).exe $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*.a db_table*

library:
	gcc -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

static_library: $(OBJS_FOR_LIB)
	ar cr $(LIBS)libbpt.a $(OBJS_FOR_LIB)