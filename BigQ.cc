#include "BigQ.h"
#include <math.h>
#include <algorithm>

bool BigQ::lessThan(Record &r1, Record &r2, OrderMaker & sortorder) {
	ComparisonEngine cmp;

	if(cmp.Compare(&r1, &r2, &sortorder) <= 0)
		return true;
	else 
		return false;
}

void BigQ::appendRunToFile(vector<Record*> * aRun, bool isLast) {
	//aRun has already been sorted, put the records into file
	static int totalRcdNum = 0;
//	cout <<"appending "<<aRun->size()<<" Records"<<endl;
	int pn = 0;
	Page *tmpPage = new Page();
	Record *lastRcd = new Record();
	int j = 0;
	int size = aRun->size();
	for(int i=0; i<size; i++){
		lastRcd->Copy(aRun->back());
//		lastRcd = aRun->back();
//		cout <<"get last"<<endl;
		if(!tmpPage->Append(lastRcd)){
//			cout <<"one page full"<<endl;
			int pos = !tmpFile->GetLength()? 0 : tmpFile->GetLength()-1 ;
			tmpFile->AddPage(tmpPage,pos);
//			cout <<"write page at "<<pos<<endl;
			tmpPage->EmptyItOut();
			j++;
			if(j==util->runlen) {
//				aRun->push_back(lastRcd);
				break;
			}
			else
				tmpPage->Append(lastRcd);
		}
		aRun->pop_back();
		totalRcdNum ++;
	}
	if(j!= util->runlen) {
		int pos = !tmpFile->GetLength()? 0 : tmpFile->GetLength()-1 ;
		tmpFile->AddPage(tmpPage,pos);
//		cout <<"write page at "<<pos<<endl;
		tmpPage->EmptyItOut();
		if(j!= util->runlen-1 && !isLast) {
			pos++;
			for(;j!=util->runlen-1;j++) {
				//if in some case the the pagenumber is less than runlen,
				//we add some empty pages there
//				cout <<"write page null at "<<pos<<endl;
				tmpFile->AddPage(tmpPage,pos);
				pos++;
			}
		}
	}
	delete tmpPage;
//	cout <<"append "<<totalRcdNum<<" records in total!"<<endl;
}

void BigQ::mergeOut() {
	//now we have runNum sorted runs in file tmpFile
	//implementing multi-way merge algorithm to merge these runs

//	cout << "runnum  " << runNum <<endl;
//	cout <<"file II name: " <<this->phaseIIfilename<<endl;
	if(runNum<1) {
		cout <<"Yahui: There are "<<runNum<<" runs in the file, maybe wrong"<<endl;
		return;
	}

	int m = util->runlen;
	if(m<2) m=2;
	int *pageIndex = new int[m]; //index of the header at each run
	Record *recordBuffer = new Record[m]; // m-1 header record of m-1 header pages
	Page *pageBuffer = new Page[m]; // m-1 header pages of m-1 runs
	Record *tmpRecord = new Record;
	Page *tmpPage = new Page;
	File *file = new File;
	file->Open(0, phaseIIfilename);
	if(!pageIndex || !recordBuffer || !pageBuffer || !tmpRecord || !tmpPage) {
		cerr <<"allocation failed!"<<endl;
	}

	int len = util->runlen+1; //initial runlen
	int num = runNum; //initial runNum

	bool Ifile = true;
	File *f1 = new File(); //file to write into
	File *f2 = new File();  //file to read from
	bool lastMerge = false;
	if(num<=m) lastMerge = true;

	bool firstMerge = true;
	while(1) {
		if(Ifile) {
			f1->Open(0, phaseIIfilename);
			f2->Open(1, fileName);
//			cout <<"now reading I"<<endl;
		}
		else {
			f1->Open(0, fileName);
			f2->Open(1, phaseIIfilename);
//			cout <<"now reading II"<<endl;
		}
		Ifile = !Ifile;

		int total = 0;
//		cout <<"In this merge, len: " <<len<<";run num: "<<num<<endl;
		int groupNum = (num%m <= 0)? (int)(num/m):(int)(num/m)+1;
//		cout <<"group number: "<<groupNum<<endl;
		for(int i=0;i< groupNum; i++) {
			//for each group of m runs
//			cout <<"Now merge group "<<i<<endl;
			int groupLast =0;
			int doneCount = 0;
			off_t groupStart = i*m*len;

			//initialize the pageBuffer and recordBuffer
//			cout <<"initialize page and record buffer"<<endl;
			for(int j=0; j < m; j++) {
				int pos;
				if(firstMerge)
					pos = i*m*(len-1) + j*(len-1);
				else
					pos = groupStart + j * len;
//				cout <<"pos: "<<pos<<endl;
				if(pos >= f2->GetLength()-1) {
					pageIndex[j] = -1;
					doneCount++;
				} else {
					pageIndex[j] = 0;
					f2->GetPage(&pageBuffer[j],pos);
					pageBuffer[j].GetFirst(&recordBuffer[j]);
				}
			}

			//merge the m pageBuffer and write them to f1;
//			cout <<"merge the m page buffer and already done:"<<doneCount<<endl;
			volatile bool initial;
			while(doneCount < m) {
				//find the smallest record among recordBuffer
				initial = false;
				int select = -1;
//				cout <<"finding the smallest"<<endl;
				for(int k= 0; k<m; k++){
					if(pageIndex[k] != -1) {
						if(!initial) {
							select = k;
							initial = true;
						}
						else if(lessThan(recordBuffer[k], recordBuffer[select], *(util->sortorder))) {
							select=k;
						}
					}
				}

				//write the smallest one to tmpPage
//				cout <<"write the smallest"<<endl;
				if(-1 == select) {
					cerr <<"Mergeout: unable to select the smallest"<<endl;
					util->out->ShutDown();
					return;
				}
				if(lastMerge) {
//					cout <<"inserting into out pipe"<<endl;
					util->out->Insert(&recordBuffer[select]);
				}
				else {
					if(!tmpPage->Append(&recordBuffer[select])) {
						//tmpPage full, write it to f1
						off_t last = groupStart + groupLast;
	//					off_t last = f1->GetLength()==0? 0: f1->GetLength()-1;
//						cout <<"write tmpPage on "<<last<<endl;
						f1->AddPage(tmpPage,last);
						tmpPage->EmptyItOut();
						tmpPage->Append(&recordBuffer[select]);
						groupLast++;
					}
				}

				total ++;
				//replace record[select] with its next one on pagebuffer
//				cout <<"replacing"<<endl;
				while(!pageBuffer[select].GetFirst(&recordBuffer[select])) {
					pageIndex[select]++;
					off_t nextPage;
					int fixedLen = len;
					if(firstMerge) {
						fixedLen -= 1;
						nextPage = i*m*(len-1) + select*(len-1) + pageIndex[select];
					}
					else nextPage = groupStart + select*len + pageIndex[select];
					if((pageIndex[select]>=fixedLen) || (nextPage>=f2->GetLength()-1)){
						pageIndex[select]=-1;
						doneCount++;
						break;
					} else {
//						cout <<"read from page "<<nextPage<<endl;
						f2->GetPage(&pageBuffer[select],nextPage);
//						pageBuffer[select].GetFirst(&recordBuffer[select]);
					}
				}
			}
			if(!lastMerge) {
				off_t last = groupStart + groupLast;
//				off_t last = f1->GetLength()==0? 0: f1->GetLength()-1;
				f1->AddPage(tmpPage,last);
				tmpPage->EmptyItOut();
//				cout <<"append write tmpPage on "<<last<<endl;
			} else
				util->out->ShutDown();
		}


//		cout <<"recersive"<<endl;
		if(lastMerge) {
			break;  //quit while(1)
		}
		len = len*m;
		num = groupNum;
		if(num<=m) {
//			cout <<"last true"<<endl;
			lastMerge = true;
		}
//		cout <<"now the len: "<<len<<";num: "<<num<<endl;
		f1->Close();
		f2->Close();
		firstMerge = false;
//		cout <<"In this merge, total records: "<<total<<endl;
	}
//	cout <<"phase II done!"<<endl;
	delete [] pageBuffer;
	delete tmpRecord;
	delete [] pageIndex;
	delete [] recordBuffer;
	remove(phaseIIfilename);
	return;
}

void *BigQ::workerHelper(void *arg) {
	BigQ * bq = (BigQ *) arg;
	cout <<"BigQ on: ";
	bq->util->sortorder->Print();
	bq->worker();
	return NULL;
}

void *BigQ::worker() {
//	cout <<"Now BigQ is working!"<<endl;
	int runlen = util->runlen;
	int rcdNum=0;
	Record *tmpRecord = new Record();
	Page *tmpPage = new Page();
	vector<Record*> aRunVector; // CAUTION, YOUR STORE THEIR ADDRESS
	int i=0;
	while(util->in->Remove(tmpRecord)) {
//		cout <<"remove from input pipe!"<<endl;
		Record *r = new Record(); //THUS YOU HAVE TO CREATE A NEW RECORD WITH NEW ADDRESS
		r->Copy(tmpRecord);
		if(tmpPage->Append(tmpRecord)) {
			rcdNum++;
			aRunVector.push_back(r);
		}
		else {
			i++;
			if(i==runlen) {
//				cout <<"sorting..."<<endl;
				sort(aRunVector.begin(), aRunVector.end(),
						CompareRecords(util->sortorder));
//				cout <<"sort done!"<<endl;
				appendRunToFile(&aRunVector, false);
				runNum ++;
				i=0;
			}
			//add current tmpRecord, otherwise will lose it.
			tmpPage->EmptyItOut();
			tmpPage->Append(tmpRecord);
			aRunVector.push_back(r);
			rcdNum++;
		}
	}
	if(rcdNum == 0)
		return NULL;
	//sort the last runpage which may not be full
	runNum++;
	i++; //page number
//	cout <<"last run has pages: " << i << endl;
	sort(aRunVector.begin(), aRunVector.end(),
							CompareRecords(util->sortorder));
	appendRunToFile(&aRunVector, true);
	if(aRunVector.size() != 0) { //last run has rcd left
		appendRunToFile(&aRunVector,true);
		runNum++;
//		cout <<"last run has " <<i<<endl;
	}
	
//	cout <<"Yahui: tmpfile length " << tmpFile->GetLength()-1
//			<< "and run num: "<<runNum<<endl;
	tmpFile->Close();
//	cout <<"phase I done!"<<endl;

	mergeOut();

//	cout <<"Yahui: worker thread done!"<<endl;
	delete [] runPages;
	delete tmpFile;
	remove(fileName);
	return NULL;
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages

    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	util = new Util(&in, &out, &sortorder, runlen);
	runPages = new Page[runlen];
	tmpFile = new File;

	struct timeval tv;
	gettimeofday(&tv, NULL);
	stringstream ss;
	ss <<"phaseI";
	ss << tv.tv_sec;
	ss << ".";
	ss << tv.tv_usec;
	fileName = new char[ss.str().size()]+1;
	fileName[ss.str().size()]=0;
	memcpy(fileName,ss.str().c_str(),ss.str().size());
//	cout <<"phaseI file name: " <<fileName<<endl;
	tmpFile->Open(0, fileName);

	gettimeofday(&tv,NULL);
	stringstream s;
	s <<"phaseII";
	s <<tv.tv_sec;
	s <<".";
	s <<tv.tv_usec;
	phaseIIfilename = new char[s.str().size()]+1;
	phaseIIfilename[s.str().size()]=0;
	memcpy(phaseIIfilename,s.str().c_str(),s.str().size());
//	cout <<"phaseII file name: " <<phaseIIfilename<<endl;

	runNum = 0;

	pthread_t workerThread;
	pthread_create (&workerThread, NULL, workerHelper, (void *)this);
//	pthread_join(workerThread, NULL);//can not be joined here, or the main thread will forever wait
}

BigQ::~BigQ () {
	tmpFile->Close();
}
