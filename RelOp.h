#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <pthread.h>
//#include <string>
#include <sstream>
#include <tr1/unordered_map>

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 
	static void *selectFile(void *);
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	DBFile *inFile;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	void DoSelectFile();
	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {
	static void *selectPipe(void *);
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	Pipe *inPipe, *outPipe;
	CNF *selOp;
	Record *literal;
	void DoSelectPipe();
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};


class Project : public RelationalOp { 
	static void *project(void *);
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	Pipe *inPipe, *outPipe;
	int *keepMe;
	int numAttsInput, numAttsOutput;
	void DoProject();
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) ;
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
};

class Join : public RelationalOp { 
	static void *join(void *);
	void DoJoin();
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	Pipe *inPipeL, *inPipeR, *outPipe;
	CNF *selOp;
	Record *literal;

	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
};
class DuplicateRemoval : public RelationalOp {
	static void *duplicateRemoval(void *);
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	Pipe *inPipe, *outPipe;
	Schema *mySchema;
	void DoDuplicateRemoval();
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) ;
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
};
class Sum : public RelationalOp {
	static void *sum(void *);
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	Pipe *inPipe, *outPipe;
	Function *computeMe;
	void DoSum();
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) ;
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
};
class GroupBy : public RelationalOp {
	static void *groupBy(void *);
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	Pipe *inPipe, *outPipe;
	OrderMaker *groupAtts;
	Function *compteMe;
	void DoGroupBy();
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) ;
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
};
class WriteOut : public RelationalOp {
	static void *writeOut(void *);
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	Pipe *inPipe;
	FILE *outFile;
	Schema *mySchema;

	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) ;
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
};
#endif
