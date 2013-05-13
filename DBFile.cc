#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

#include <iostream>
#include <stdio.h>
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>

#define NAME_LEN 100
#define PIPE_SIZE 100
using namespace std;

//Initialize the write buffer as the last page
//if there is at least one page, always get the last page as the writer buffer   
#define INIT_WRITERBUFFER   \
               ({ if(file->GetLength()>=2)     \
                    file->GetPage(writerBuffer, file->GetLength()-2); \
                })

//substitute the write buffer to the last page
#define FLUSH_WRITERBUFFER \
	({ int pos = !file->GetLength()? 0 : file->GetLength()-2; \
		file->AddPage(writerBuffer, pos);   \
	  writerBuffer->EmptyItOut();           \
	  synchronized = true;                  \
	  })

//Sorted::MACRO
#define WRITETOREADCHANGE \
		({ if(mode == WRITE) { \
				mode = READ; \
				this->readerBuffer->EmptyItOut(); \
				MergeOutpipeWithFile(); \
				MoveFirst(); \
			} \
		})

#define READTOWRITECHANGE \
	({ if(mode == READ)  \
			mode = WRITE; \
		if(!m_inPipe) m_inPipe = new Pipe(PIPE_SIZE); \
		if(!m_outPipe) m_outPipe = new Pipe(PIPE_SIZE); \
	   if (!m_bigQ) \
		   m_bigQ = new BigQ(*this->m_inPipe, *this->m_outPipe,  \
				   *(sortInfo->myorder), sortInfo->runLength);\
	})

#define WRITETOPAGEFORMERGE(rcd) \
	({ if(!tmpPage->Append(rcd)) { \
			int pos = tmpFile->GetLength()==0? 0:tmpFile->GetLength()-1;  \
			tmpFile->AddPage(tmpPage, pos);  \
			tmpPage->EmptyItOut();   \
			tmpPage->Append(rcd);  \
		} syn = false;})

DBFile::DBFile () {
//	FILE_LOG(logDEBUG) << "DBFile Constructor";
	myInternalVar = NULL;
}

DBFile::~DBFile() {
	if(!myInternalVar)
		delete myInternalVar;
}

Heap::Heap() {
//	FILE_LOG(logDEBUG) << "Heap DBFile Constructor";
	readerBuffer = new Page();
	writerBuffer = new Page();
	synchronized = true;
}

Heap::~Heap() {
	delete readerBuffer;
	delete writerBuffer;
}

Sorted::Sorted() {
//	FILE_LOG(logDEBUG) << "Sorted DBFile Constructor";
	sortInfo = new SortInfo();
	m_inPipe = NULL;
	m_outPipe = NULL;
	readerBuffer = new Page();
	pageBufferForMerge = NULL;
	m_bigQ = NULL;
	mode = READ;
	queryChange = true;

	//for getnext CNF
	queryOrder = NULL;
}

Sorted::~Sorted() {
	delete readerBuffer;
	delete m_inPipe;
	delete m_outPipe;
	delete sortInfo;
}

GenericDBFile::GenericDBFile() {
//	FILE_LOG(logDEBUG) << "Generic DBFile Constructor";
	file = new File();
	curRec = new RecordPointer();
	fileName = new char[NAME_LEN];
	opend = false;
}

GenericDBFile::~GenericDBFile() {
	delete [] fileName;
	delete file;
	delete curRec;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
//	FILE_LOG(logDEBUG) << "DBFile::Create " <<f_path;
	if(f_type == heap) {
		myInternalVar = new Heap();
	} else if(f_type == sorted) {
		myInternalVar = new Sorted();
	}
	if(myInternalVar == NULL) {
		cerr <<"Yahui: create DBFile error!"<<endl;
		return 0;
	}
	if( myInternalVar->Create(f_path, startup) ) {
		strcpy(myInternalVar->fileName, f_path);
		myInternalVar->opend = true;
		return 1;
	}
	return 0;
}

int DBFile::Open (char *f_path) {
	//this function assumes that the DBFile exists and has previously been created and closed
    //in case that open another file while there is one being written now.
//	FILE_LOG(logDEBUG) << "DBFile::Open " <<f_path;

    char *headerName = new char[NAME_LEN];
    sprintf(headerName, "%s.header", f_path);
	FILE *fp = fopen(headerName, "r");
	if(!fp) {
		cout <<"Yahui: "<<f_path<<" DBFile does not exist"<<endl;
		exit(-1);
	}

    fType ftype;
	fscanf(fp, "%d", &ftype);
//	cout <<"DBFile type: "<<ftype<<endl;
	if(ftype == heap) {
		myInternalVar = new Heap();
	} else if(ftype == sorted) {
		myInternalVar = new Sorted();
		//initialize some stuff of sorted DBFile
	} else {
		cerr <<"Unsupported DBFile type!"<<endl;
		return 0;
	}
	fclose(fp);
	if(myInternalVar == NULL) {
		cerr <<"Yahui: create DBFile error!"<<endl;
		return 0;
	}

	strcpy(myInternalVar->fileName, f_path);
	if(myInternalVar->Open(f_path)) {
//	cout <<"DBFile path: "<<f_path<<endl;
		myInternalVar->opend = true;
		return 1;
	}
	return 0;
}

int DBFile::Close (){
//	FILE_LOG(logDEBUG) << "DBFile::Close";
	if(!myInternalVar->opend) return 0;
	return myInternalVar->Close();
}

void DBFile::Load (Schema &myschema, char *loadpath) {
//	FILE_LOG(logDEBUG) << "DBFile::Load";
	myInternalVar->Load(myschema, loadpath);
}

void DBFile::MoveFirst () {
//	FILE_LOG(logDEBUG) << "DBFile::MoveFirst";
	myInternalVar->MoveFirst();
}
void DBFile::Add (Record &addme) {
//	FILE_LOG(logDEBUG) << "DBFile::Add";
	myInternalVar->Add(addme);
}
int DBFile::GetNext (Record &fetchme){
//	FILE_LOG(logDEBUG) << "DBFile::GetNext(Record &)";
	return myInternalVar->GetNext(fetchme);
}
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
//	FILE_LOG(logDEBUG) << "DBFile::GetNext(Record &, CNF &, Record &)";
	return myInternalVar->GetNext(fetchme, cnf, literal);
}

int Heap::Create (char *fpath, void *startup) {
	file->Open(0, fpath);
	return 1;
}
int Heap::Open (char *fpath) {
	file->Open(1, fpath);
	return 1;
}

void Heap::Load (Schema &f_schema, char *loadpath) {
//	FILE_LOG(logDEBUG1) << "Heap::Load";
//	cout <<"Heap:: Load"<<endl;
	FILE *fp = fopen(loadpath, "r");
	if(!fp) {
		cout <<"Yahui: Can not open load path! "<<endl;
		return ;
	}
	Record rec;
	while(rec.SuckNextRecord(&f_schema, fp) == 1) {
		//put rec into file
		//rec.Print(&f_schema);
		Add(rec);
	}
//	FLUSH_WRITERBUFFER;
    fclose(fp);
//	cout <<"Yahui: Load "<<loadpath <<" OK!"<<endl;
}

void Heap::MoveFirst () {
//	FILE_LOG(logDEBUG1) << "Heap::MoveFirst";
	if(!file)
		cerr <<"DBFile is not open"<<endl;;
	readerBuffer->EmptyItOut();
	if(!synchronized)
		FLUSH_WRITERBUFFER;

    curRec->whichPage = 0;
	curRec->whichRecord = 0;
	if(curRec->whichPage<=file->GetLength()-2) {
		readerBuffer->EmptyItOut();
		file->GetPage(readerBuffer, curRec->whichPage);
		curRec->whichPage++;
	}
	return ;
}

int Heap::Close () {
//	FILE_LOG(logDEBUG1) << "Heap::Close";
	if(!file)
		return 0;
	readerBuffer->EmptyItOut();
    if(!synchronized)
        FLUSH_WRITERBUFFER;
	file->Close();
	char *fileHeaderName = new char[NAME_LEN];
	sprintf(fileHeaderName, "%s.header", fileName);
	FILE *f = fopen(fileHeaderName, "w");
	if(!f) {
		cerr << "Open file header name error!"<<endl;
		return 0;
	}
	fprintf(f, "%d\n", 0);
	fclose(f);
	delete [] fileHeaderName;
//	cout <<"Yahui: close DBFile OK!"<<endl;
	return 1;
}

void Heap::Add (Record &rec) {
//	FILE_LOG(logDEBUG1) << "Heap::Add";
    static bool initialization = false;
    if(!initialization) {
        INIT_WRITERBUFFER;
        initialization = true;
    }
	if(writerBuffer->Append(&rec)) {
		synchronized = false;
	} else {
		//writerBuffer full
		//write it back to override the last page
		 FLUSH_WRITERBUFFER;
		 //cout <<"Yahui: flush writer buffer!"<<endl;
		 writerBuffer->Append(&rec);
		 synchronized = false;
		 //add a new page to the end of file
		 file->AddPage(writerBuffer, file->GetLength()-1);
	}
//	cout <<"Yahui: Add record suc!"<<endl;
}

int Heap::GetNext (Record &fetchme) {
//	FILE_LOG(logDEBUG1) << "Heap::GetNext(Record &)";

	if(readerBuffer->GetFirst(&fetchme)) {
		//fetch record suc, moving the pointer ahead
 		curRec->whichRecord++;
		return 1;
	} else {
		//no record in the reader buffer page. Load next page if there is one.
		if(curRec->whichPage <= file->GetLength()-2) { //there is more page
			if(!synchronized && curRec->whichPage == file->GetLength()-2) {
				FLUSH_WRITERBUFFER;
			}

			//update reader buffer
//			readerBuffer->EmptyItOut();
			file->GetPage(readerBuffer, curRec->whichPage);
			curRec->whichPage++;
			if(readerBuffer->GetFirst(&fetchme)) {
			//fetch the record in the next page
//				cout <<"read form new page "<<endl;
				curRec->whichRecord = 1;
				return 1;
			} else {
				cout <<"Yahui: No records in this page, should be an error"<<endl;
				return 0;
			}
		}
		else { // no more page.
//				cout <<"Yahui: No more records in file" <<endl;
			return 0;
		}
	}

}

int Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
//	FILE_LOG(logDEBUG1) << "Heap::GetNext(Record &, CNF &, Record &)";
	ComparisonEngine comp;
	while(GetNext(fetchme)){
		if(comp.Compare(&fetchme, &literal, &cnf)) {
			return 1;
		}
	}
	return 0;
}

int Sorted::Create (char *fpath, void *startup) {
//	FILE_LOG(logDEBUG1) << "Sorted Create";
	sortInfo = (SortInfo *) startup;
	file->Open(0, fpath);
//	cout <<"sortInfo len: "<<sortInfo->runLength<<endl;
//	cout <<"sortinfo order: "<<sortInfo->myorder->ToString()<<endl;
	return 1;
}
int Sorted::Open (char *fpath) {
//	FILE_LOG(logDEBUG1) << "Sorted Open";

	//retrieve sort info from file header
	char *fileHeaderName = new char[NAME_LEN];
	sprintf(fileHeaderName, "%s.header", fileName);
	FILE *f = fopen(fileHeaderName,"r");
	int runlen;
	OrderMaker *o = new OrderMaker;
	fscanf(f, "%d", &runlen); //since the first line is the filetype
	fscanf(f, "%d", &runlen);
	sortInfo->runLength = runlen;
	int attNum;
	fscanf(f, "%d", &attNum);
	o->numAtts = attNum;
	for(int i = 0;i<attNum;i++) {
		int att;
		int type;
		if(feof(f)) {
			cerr << "Retrieve ordermaker from file error"<<endl;
			return 0;
		}
		fscanf(f, "%d %d", &att, &type);
		o->whichAtts[i] = att;
		if(0 == type) {
			o->whichTypes[i] = Int;
		} else if(1==type) {
			o->whichTypes[i] = Double;
		} else
			o->whichTypes[i] = String;
	}
	sortInfo->myorder = o;
//	cout <<"sort len is: "<<sortInfo->runLength<<endl;
//	cout <<"sort order is: "<<sortInfo->myorder->ToString()<<endl;
	//open dbfile
	file->Open(1, fpath);
//	cout <<"Open: "<<this->fileName<<" Length is: "<<file->GetLength()<<endl;
	fclose(f);
	delete[] fileHeaderName;

	return 1;
}

void Sorted::Load (Schema &myschema, char *loadpath){
//	FILE_LOG(logDEBUG1) << "Sorted::Load";
	FILE *fp = fopen(loadpath, "r");
	if(!fp) {
		cout <<"Yahui: Can not open load path! "<<endl;
		return ;
	}
	Record rec;
	while(rec.SuckNextRecord(&myschema, fp) == 1) {
		Add(rec);
	}
	fclose(fp);
//	cout <<"Yahui: Load "<<loadpath <<" OK!"<<endl;
	MergeOutpipeWithFile();
}

void Sorted::Add (Record &addme) {
//	FILE_LOG(logDEBUG1) << "Sorted::Add";
//	cout <<"insert ok"<<endl;
	READTOWRITECHANGE;
	queryChange = true;
	m_inPipe->Insert(&addme);
}

int Sorted::CompareWithOrder( Record *r1,  Record *r2,
		 OrderMaker *o) {
	ComparisonEngine cmp;
	return cmp.Compare(r1, r2, o);
}

bool Sorted::GetNextFromBegin(Record *fetchme ) {
//	FILE_LOG(logDEBUG2) << "Sorted:GetNextFromBegin";
	while(!this->pageBufferForMerge->GetFirst(fetchme)) {
		if(pagePtrForMerge >= file->GetLength()-1)
			return false;
		else {
			file->GetPage(pageBufferForMerge, pagePtrForMerge);
			pagePtrForMerge++;
		}
	}
	return true;
}

void Sorted::MergeOutpipeWithFile() {
//	FILE_LOG(logDEBUG2) << "Sorted:MergeOutpipeWithFile";
	if(!m_bigQ) return;
//	cout <<"shut down input pipe!"<<endl;
	m_inPipe->ShutDown();

	Page *tmpPage = new Page();
	Record *tmpRcd1 = new Record;
	Record *tmpRcd2 = new Record;
	File *tmpFile = new File;
	bool syn = true;
//	cout <<"creating tmp file"<<endl;
	tmpFile->Open(0, "mergefile.tmp");
	if(!pageBufferForMerge)
		pageBufferForMerge = new Page();
	pageBufferForMerge->EmptyItOut();
	pagePtrForMerge = 0;
	bool pipeEmpty = 0;
	bool fileEmpty = 0;
//	cout <<"getting from pipe"<<endl;
	if(!this->m_outPipe->Remove(tmpRcd1)) {
		pipeEmpty = 1;
//		cout <<"can not got from outpipe"<<endl;
	}
	if(!GetNextFromBegin(tmpRcd2)) {
		fileEmpty = 1;
//		cout <<"can not got from file"<<endl;
	}
	while(!pipeEmpty && !fileEmpty) {
//		if(!tmpRcd1) {
//			tmpRcd1 = new Record;
//			pipeEmpty = !m_outPipe->Remove(tmpRcd1);
//			cout <<"pipeEmpty: "<<pipeEmpty<<endl;
//		}
//		if(!tmpRcd2) {
//			tmpRcd2 = new Record;
//			fileEmpty = !GetNextFromBegin(tmpRcd2);
//		}
		if(!pipeEmpty && !fileEmpty) {
			if(this->CompareWithOrder(tmpRcd1, tmpRcd2, sortInfo->myorder) <= 0) {
//				cout <<"add from pipe!"<<endl;
				WRITETOPAGEFORMERGE(tmpRcd1);
//				tmpFile->Add(*tmpRcd1);
//				delete tmpRcd1;
//				tmpRcd1 = NULL;
				if(!this->m_outPipe->Remove(tmpRcd1)) {
					pipeEmpty =1;
				}
			}
			else {
//				cout <<"add from file!"<<endl;
		    	WRITETOPAGEFORMERGE(tmpRcd2);
//				tmpFile->Add(*tmpRcd2);
//				delete tmpRcd2;
//				tmpRcd2 = NULL;
				if(!GetNextFromBegin(tmpRcd2))
					fileEmpty = 1;
			}
		}
	}
	if(pipeEmpty && fileEmpty) { //impossible here
		cerr << "pipe and file are both empty!"<<endl;
		return ;
	}
	if(!pipeEmpty){
		//file empty, add the rest records in pipe to tmpFile
//		cout <<"pipe not empty"<<endl;
		do {
			WRITETOPAGEFORMERGE(tmpRcd1);
//			tmpFile->Add(*tmpRcd1);
//			cout <<"add from pipe rest!"<<endl;
		} while(m_outPipe->Remove(tmpRcd1));
	}

	if(!fileEmpty) {
		//pipe empty, add the rest records in file to tmpFile
//		cout <<"file not empty"<<endl;
		do {
			WRITETOPAGEFORMERGE(tmpRcd2);
//			tmpFile->Add(*tmpRcd1);
		} while(GetNextFromBegin(tmpRcd2));
	}

	//add the buffer to file if needed
	if(!syn) {
		int last = !tmpFile->GetLength() ? 0 : tmpFile->GetLength()-1;
		tmpFile->AddPage(tmpPage, last);
	}

    //clean up
	delete tmpPage;
	delete tmpRcd1;
	delete tmpRcd2;

	delete m_inPipe;
	m_inPipe = NULL;
	delete m_outPipe;
	m_outPipe = NULL;

	file->Close();
	tmpFile->Close();
	if(remove(fileName)) {
		cerr <<"remove file error!"<<fileName<<endl;
		return;
	}
//	file = tmpFile;
	if(rename("mergefile.tmp", fileName)) {
		cerr <<"rename file error!"<<endl;
		return;
	}
	readerBuffer->EmptyItOut();
	file->Open(1, this->fileName);
}

void Sorted::MoveFirst () {
//	FILE_LOG(logDEBUG1) << "Sorted::MoveFirst";
	WRITETOREADCHANGE;
	queryChange = true;
	curRec->whichPage = 0;
	curRec->whichRecord = 0;
	// fill the reader buffer for read
	if(curRec->whichPage<=file->GetLength()-2) {
		readerBuffer->EmptyItOut();
		file->GetPage(readerBuffer, curRec->whichPage);
		curRec->whichPage++;
	}
	return ;
}

int Sorted::GetNext (Record &fetchme) {
//	FILE_LOG(logDEBUG1) << "Sorted::GetNext(Record &)";
	WRITETOREADCHANGE;
//	cout <<"curpage: "<< curRec->whichPage <<" filelength: "<<file->GetLength()<<endl;
	while(!readerBuffer->GetFirst(&fetchme)) {
		if(curRec->whichPage >= file->GetLength()-1) {
			cout <<"To the end of file"<<endl;
			return 0;
		}
		else {
//			cout <<"getting new page from file"<<endl;
			file->GetPage(readerBuffer, curRec->whichPage);
			curRec->whichPage++;
			curRec->whichRecord =1;
		}
	}
	curRec->whichRecord++;
	return 1;
}

OrderMaker *Sorted::GetMatchedOrderFromCNF(CNF &cnf, OrderMaker &sortOrder) {
	OrderMaker *matchOrder = new OrderMaker();
	for(int i = 0; i<sortOrder.numAtts; i++) {
		cout <<"now checking att: "<<sortOrder.whichAtts[i]<<endl;
		bool match = false;
		for(int j = 0; j<cnf.numAnds; j++) {
			if(!match) {
				for(int k=0; k<cnf.orLens[j]; k++) {
//					cout <<"now try matching cnf att: "<<cnf.orList[j][k].whichAtt1<< " Op2: "<<cnf.orList[j][k].whichAtt2<<endl;
					if(cnf.orList[j][k].op == Equals) {
						if(cnf.orList[j][k].operand1 != Literal) {
							if((sortOrder.whichAtts[i] == cnf.orList[j][k].whichAtt1)
									&& (sortOrder.whichTypes[i] == cnf.orList[j][k].attType)){
								matchOrder->whichAtts[matchOrder->numAtts] = sortOrder.whichAtts[i];
								matchOrder->whichTypes[matchOrder->numAtts++] = sortOrder.whichTypes[i];
								match = true;
								break;
							}
						} else if(cnf.orList[j][k].operand2 != Literal) {
							if((sortOrder.whichAtts[i] == cnf.orList[j][k].whichAtt2)
									&& (sortOrder.whichTypes[i] == cnf.orList[j][k].attType)){
								matchOrder->whichAtts[matchOrder->numAtts] = sortOrder.whichAtts[i];
								matchOrder->whichTypes[matchOrder->numAtts++] = sortOrder.whichTypes[i];
								match = true;
								break;
							}
						}
					}
				}
			}
		}
		if(!match) break;
	}
	if(matchOrder->numAtts == 0)
	{
		cout <<"No query OrderMaker can be constructed!"<<endl;
		delete matchOrder;
		return NULL;
	}
	return matchOrder;
}

Record *Sorted::LoadMatchPage(Record &literal) {
	//return the first record which equals to literal based on queryorder;
	//make readerbuffer's first record pointer to its next one.
	//remember the current pointer status
//	RecordPointer oldPointer(curRec->whichPage, curRec->whichRecord);
	if(queryChange) {
		int low = curRec->whichPage-1;
		int high = file->GetLength()-2;
		int matchPage = BinarySearch(low, high, queryOrder, literal);
		cout <<"matchpage: "<<matchPage<<endl;
		if(matchPage == -1) {
			//not found
			return NULL;
		}
		if(matchPage != curRec->whichPage-1) {
			readerBuffer->EmptyItOut();
			file->GetPage(readerBuffer, matchPage);
			curRec->whichPage = matchPage+1;
		}
		queryChange = false;
	}

	//find the potential page, make reader buffer pointer to the first record
	// that equal to query order
	Record *returnRcd = new Record;
	while(readerBuffer->GetFirst(returnRcd)) {
		if(CompareWithOrder(returnRcd, &literal, queryOrder) == 0) {
			//find the first one
			return returnRcd;
		}
	}
	if(curRec->whichPage >= file->GetLength()-2) {
		return NULL;
	} else {
		//since the first record may exist on the next page
		curRec->whichPage++;
		file->GetPage(readerBuffer, curRec->whichPage);
		while(readerBuffer->GetFirst(returnRcd)) {
			if(CompareWithOrder(returnRcd, &literal, queryOrder) == 0) {
				//find the first one
				return returnRcd;
			}
		}
	}
	return NULL;

}

int Sorted::BinarySearch(int low, int high, OrderMaker *queryOM, Record &literal) {
	if(high < low) return -1;
	if(high == low) return low;
	//high > low
	cout <<"binary search, low: "<<low<<" high: "<<high<<endl;
	Page *tmpPage = new Page;
	Record *tmpRcd = new Record;
	int mid = (int) (high+low)/2;
	file->GetPage(tmpPage, mid);
	tmpPage->GetFirst(tmpRcd);
	int res = CompareWithOrder(tmpRcd, &literal, queryOM);
	delete tmpPage;
	delete tmpRcd;
	if( -1 == res) {
		if(low==mid)
			return mid;
		return BinarySearch(mid, high, queryOM, literal);
	}
	else if(0 == res) {
		return BinarySearch(low, mid-1, queryOM, literal);
	}
	else
		return BinarySearch(low, mid-1, queryOM, literal);
}

int Sorted::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
//	FILE_LOG(logDEBUG1) << "Sorted::GetNext(Record &, CNF &, Record &)";
	WRITETOREADCHANGE;
//	if(queryChange) {
//		cout <<"Construct query order!"<<endl;
//		queryOrder = this->GetMatchedOrderFromCNF(cnf, *(this->sortInfo->myorder));
//	}
	if(queryChange && queryOrder) {
		cout <<"Construct query order!"<<endl;
		queryOrder = this->GetMatchedOrderFromCNF(cnf, *(this->sortInfo->myorder));

		cout <<"query order: "<<queryOrder->ToString()<<endl;
		Record *firstEqual = this->LoadMatchPage(literal);
		if(!firstEqual) return 0;

		fetchme.Consume(firstEqual);
		ComparisonEngine comp;
		if(comp.Compare(&fetchme, &literal, &cnf)) {
			//find the right record
			return 1;
		}
		//if the first equal one not apply, then check the rest
		while(GetNext(fetchme)) {
			if(CompareWithOrder(&fetchme, &literal, queryOrder)!=0) {
				//not match to query order
				return 0;
			} else {
				if(comp.Compare(&fetchme, &literal, &cnf)) {
					//find the right record
					return 1;
				}
			}
		}
	} else {
		//no query order constructed
		ComparisonEngine comp;
		while(GetNext(fetchme)){
			if(comp.Compare(&fetchme, &literal, &cnf)) {
				return 1;
			}
		}
		return 0;
	}
	return 0;
}

int Sorted::Close () {
//	FILE_LOG(logDEBUG1) << "Sorted::Close";
	WRITETOREADCHANGE;
	if(!file)
		return 0;
	file->Close();
	char *fileHeaderName = new char[NAME_LEN];
	sprintf(fileHeaderName, "%s.header", fileName);
	FILE *f = fopen(fileHeaderName, "w");
	if(!f) {
		cerr << "Open file header name error!"<<endl;
		return 0;
	}
	fprintf(f, "%d\n", 1);
	fprintf(f, "%d\n", sortInfo->runLength);
	fprintf(f, "%s", sortInfo->myorder->ToString().c_str());
//	cout <<sortInfo->myorder->ToString();
	fclose(f);
	delete[] fileHeaderName;
	return 1;
}

