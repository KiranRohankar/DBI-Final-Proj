#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <vector>
#include <sstream>
#include <sys/time.h>
#include <string.h>
//#include <queue>

using namespace std;



//class CompareRecord {
//private:
//	OrderMaker *orderMaker;
//	ComparisonEngine cmp;
//public:
//	CompareRecord(OrderMaker *order);
//	~CompareRecord();
//	bool operator()(Record *r1, Record *r2) {
//		if(cmp.Compare(r1, r2, orderMaker) != 1)
//			return true;
//		else return false;
//	}
//};
//typedef priority_queue<Record, vector<Record>, CompareRecord> PQ;
//PQ pq;

class BigQ {
	 struct Util{
		Pipe *in;
		Pipe *out;
		OrderMaker *sortorder;
		int runlen;
		Util(Pipe *i, Pipe *o, OrderMaker *s, int l){
			in = i;
			out = o;
			sortorder = s;
			runlen = l;
		}
		~Util();
	};

private:
	Util *util;
	Page *runPages;
	char *fileName;
	char *phaseIIfilename;
	File *tmpFile;
	int runNum;

	struct CompareRecords
	{
		OrderMaker *pSortOrder;
		CompareRecords(OrderMaker *pOM): pSortOrder(pOM) {}

		bool operator()(Record* const& r1, Record* const& r2)
		{
			Record* r11 = const_cast<Record*>(r1);
			Record* r22 = const_cast<Record*>(r2);

			ComparisonEngine ce;
			//sort in a descending order, 'cause we fetch it reversely
			if (ce.Compare(r11, r22, pSortOrder) ==1)
				return true;
			else
				return false;
		}
	};

private:
	static void *workerHelper(void *);
	void *worker();
	void mergeOut();
//	int sort(int rcdNum);
	void appendRunToFile(vector<Record*>* aRun, bool);
//	//auxiliary methods for sort
//	void quickSort(Record *record, int *result, int start,
//			int end, OrderMaker *sortorder);
//	int partition(Record *record, int *result, int start,
//			int end, OrderMaker *sortorder);
	bool lessThan(Record &, Record &, OrderMaker &);
public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

};

#endif
