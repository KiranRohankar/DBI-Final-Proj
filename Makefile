CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

yhsql: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Statistics.o Function.o QueryPlan.o Optimizer.o y.tab.o lex.yy.o main.o
	$(CC) -o yhsql Record.o Comparison.o ComparisonEngine.o Schema.o Function.o File.o DBFile.o Pipe.o BigQ.o RelOp.o Statistics.o QueryPlan.o Optimizer.o y.tab.o lex.yy.o main.o -ll -pthread
	
main.o: main.cc
	$(CC) -g -c main.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
Optimizer.o: Optimizer.cc
	$(CC) -g -c Optimizer.cc

QueryPlan.o:QueryPlan.cc
	$(CC) -g -c QueryPlan.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc
	
Function.o: Function.cc
	$(CC) -g -c Function.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc
	
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc
	
BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc
	
RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
