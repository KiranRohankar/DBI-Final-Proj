
#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"

typedef enum {heap, sorted, tree} fType; // {0, 1, 2}

// stub DBFile header..replace it with your own DBFile.h 
class GenericDBFile {
	friend class DBFile;
protected:
	struct RecordPointer {
		off_t whichPage;   // the next page to read in file
		off_t whichRecord; // the next record in page

		RecordPointer() { whichPage = 0; whichRecord = 0;}
		RecordPointer(int p, int r) { whichPage = p; whichRecord = r;}
		~RecordPointer() { }
	};
	File * file;
	RecordPointer *curRec;  // current record in file
	char *fileName; //file name "*.bin" by which to store the extra info of DBFile
	bool opend; //whether the current file is open or not.
public:
	virtual int Create (char *fpath, void *startup) =0;
	virtual int Open (char *fpath) = 0;
	virtual int Close () = 0;
	virtual void Load (Schema &myschema, char *loadpath) = 0;
	virtual void MoveFirst () = 0;
	virtual void Add (Record &addme) = 0;
	virtual int GetNext (Record &fetchme) = 0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
	GenericDBFile();
	virtual ~GenericDBFile();
};

class Heap: virtual public GenericDBFile {
private:
	Page *readerBuffer;  //buffer the current page in read
	Page *writerBuffer;  //buffer the last page to write
	bool synchronized;

public:
	Heap();
	~Heap();
	int Create (char *fpath, void *startup);
	int Open (char *fpath);
	int Close ();
	void Load (Schema &myschema, char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};

struct SortInfo{
	OrderMaker *myorder;
	int runLength;
};

enum Mode {READ, WRITE};

class Sorted: public GenericDBFile {
private:
	SortInfo *sortInfo;
	Mode mode;
	BigQ *m_bigQ;
	Pipe *m_inPipe, *m_outPipe;

	//for read
	Page *readerBuffer;

	//for create and open
//	int CreateExtra (void *startup);
//	int OpenExtra ();

	//for write
	Page *pageBufferForMerge;
	int pagePtrForMerge;
	bool GetNextFromBegin(Record *fetchme);
	void MergeOutpipeWithFile();
	int CompareWithOrder( Record *,  Record *,  OrderMaker *);

	//for getnext with cnf
	OrderMaker *queryOrder;

	//make the readerbuffer start with the first equal record according to queryOrder
	//-1 if not have the first equal record.
	Record *LoadMatchPage(Record &literal);
	bool queryChange;
	//return the page that may have the first equal record, -1 if not found
	int BinarySearch(int low, int high, OrderMaker *queryOM, Record &literal);
	OrderMaker *GetMatchedOrderFromCNF(CNF &cnf, OrderMaker &);
public:
	Sorted();
	~Sorted();
	int Create (char *fpath, void *startup);
	int Open (char *fpath);
	int Close ();
	void Load (Schema &myschema, char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};

class DBFile {
private:
	GenericDBFile *myInternalVar;
public:
	DBFile (); 
	~DBFile();

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
