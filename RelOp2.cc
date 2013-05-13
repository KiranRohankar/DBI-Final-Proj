#include "RelOp.h"
#include <fstream>

#define PIPE_SIZE 100

#define CLEANUPVECTOR(v) \
	({ for(vector<Record *>::iterator it = v.begin(); it!=v.end(); it++) { \
		if(!*it) { delete *it; } }\
		v.clear();\
})

//-----------------SelectFile------------------
void *SelectFile::selectFile(void *arg){
	cout <<"Select file."<<endl;
	SelectFile *sf = (SelectFile *) arg;
	Record *tmpRecord = new Record;
	while(sf->inFile->GetNext(*tmpRecord, *(sf->selOp), *(sf->literal))) {
		sf->outPipe->Insert(tmpRecord);
	}
	delete tmpRecord;
	sf->outPipe->ShutDown();
	return NULL;
}
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create(&thread, NULL, selectFile, this);
}

void SelectFile::WaitUntilDone () {
	 pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
	this->nPages = runlen;
}

//-----------------SelectPipe------------------
void *SelectPipe::selectPipe(void *arg) {
	cout <<"Select Pipe."<<endl;
	SelectPipe *sp = (SelectPipe *) arg;
	Record *tmpRecord = new Record();
	while(sp->inPipe->Remove(tmpRecord)) {
		ComparisonEngine cmp;
		if(cmp.Compare(tmpRecord, sp->literal, sp->selOp)) {
			sp->outPipe->Insert(tmpRecord);
		}
	}
	delete tmpRecord;
	sp->outPipe->ShutDown();
	return NULL;
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create(&thread, NULL, selectPipe, this);
}

void SelectPipe::WaitUntilDone () {
	 pthread_join (thread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
	this->nPages = runlen;
}

//-----------------Project-----------------------
void *Project::project(void *arg) {
	cout <<"Project."<<endl;
	Project *pj = (Project *) arg;
	Record *tmpRcd = new Record;
	while(pj->inPipe->Remove(tmpRcd)) {
		tmpRcd->Project(pj->keepMe, pj->numAttsOutput, pj->numAttsInput);
		pj->outPipe->Insert(tmpRcd);
	}
	pj->outPipe->ShutDown();
	delete tmpRcd;
	return NULL;
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe,
		int numAttsInput, int numAttsOutput) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;
	pthread_create(&thread, NULL, project, this);
}

void Project::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void Project::Use_n_Pages (int n) {
	this->nPages = n;
}


//-----------------Join--------------------------
void *Join::join(void *arg) {
	cout <<"Join."<<endl;
	Join *mj = (Join *) arg;
	OrderMaker orderL;
	OrderMaker orderR;
	mj->selOp->GetSortOrders(orderL, orderR);
//	cout <<"order: L: "<<orderL.ToString()<<endl;
//	cout <<"order: R: "<<orderR.ToString()<<endl;

			//----------sort-merge Join-----------
	if(orderL.numAtts && orderR.numAtts && orderL.numAtts == orderR.numAtts) {
		//means we can do a sort-merge join
		int runlen = 50;
	//		cout <<"runlen: "<<runlen<<endl;
		Pipe pipeL(PIPE_SIZE), pipeR(PIPE_SIZE);
		BigQ *bigQL = new BigQ(*(mj->inPipeL), pipeL, orderL, runlen);
		BigQ *bigQR = new BigQ(*(mj->inPipeR), pipeR, orderR, runlen);

		//next get tuples out in order from pipeL and pipeR, and put the equal tuples into two vectors
		//then cross-multiply them
		vector<Record *> vectorL;
		vector<Record *> vectorR;
		Record *rcdL = new Record();
		Record *rcdR = new Record();
		ComparisonEngine cmp;

		if(pipeL.Remove(rcdL) && pipeR.Remove(rcdR)) {
			//figure out the attributes of LHS record and RHS record
//			cout <<"Get the first ones from two pipe"<<endl;
			int leftAttr = ((int *) rcdL->bits)[1] / sizeof(int) -1;
			int rightAttr = ((int *) rcdR->bits)[1] / sizeof(int) -1;
			int totalAttr = leftAttr + rightAttr;
			int attrToKeep[totalAttr];
			for(int i = 0; i< leftAttr; i++)
				attrToKeep[i] = i;
			for(int i = 0; i< rightAttr; i++)
				attrToKeep[i+leftAttr] = i;
//			cout <<"left attr: "<<leftAttr <<"; right Attr: "<<rightAttr<<"; attr to keep:[";
//			for(int i=0; i<totalAttr; i++)
//				cout << attrToKeep[i]<<",";
//			cout <<"]"<<endl;
			int joinNum;

			bool leftOK=true, rightOK=true; //means that rcdL and rcdR are both ok
	int num  =0;
			while(leftOK && rightOK) {
//				cout <<"another !"<<endl;
				leftOK=false; rightOK=false;
				int cmpRst = cmp.Compare(rcdL, &orderL, rcdR, &orderR);
				switch(cmpRst) {
				case 0: // L == R
				{
	num ++;
					Record *rcd1 = new Record(); rcd1->Consume(rcdL);
					Record *rcd2 = new Record(); rcd2->Consume(rcdR);
					vectorL.push_back(rcd1);
					vectorR.push_back(rcd2);
					//cout <<"vector size now: " <<vectorL.size()<<" R: "<<vectorR.size()<<endl;
					//get rcds from pipeL that equal to rcdL
					while(pipeL.Remove(rcdL)) {
						if(0 == cmp.Compare(rcdL, rcd1, &orderL)) { // equal
	//							cout <<"get more equal from left"<<endl;
							Record *cLMe = new Record();
							cLMe->Consume(rcdL);
							vectorL.push_back(cLMe);
	//							cout <<"left equal"<<endl;
						} else {
							leftOK = true;
	//							rcdL->Consume(rcd1);
							break;
						}
					}
					//get rcds from PipeR that equal to rcdR
					while(pipeR.Remove(rcdR)) {
						if(0 == cmp.Compare(rcdR, rcd2, &orderR)) { // equal
	//							cout <<"get more equal from right"<<endl;
							Record *cRMe = new Record();
							cRMe->Consume(rcdR);
							vectorR.push_back(cRMe);
						} else {
							rightOK = true;
	//							rcdR->Consume(rcd2);
							break;
						}
					}
					//now we have the two vectors that can do cross product
//					cout <<"vectorL: "<<vectorL.size()<<"! vectorR: " <<vectorR.size()<<endl;
					Record *lr = new Record, *rr=new Record, *jr = new Record;
	//					cout <<"rcdL:" <<rcdL<<" rcdR: " <<rcdR<<endl;
					for(vector<Record *>::iterator itL = vectorL.begin(); itL!=vectorL.end(); itL++) {
						lr->Consume(*itL);
//						cout <<"Left: " <<lr<<endl;
						for(vector<Record *>::iterator itR = vectorR.begin(); itR!=vectorR.end(); itR++) {
							//join and output
//							cout <<"    right: " <<*itR<<endl;
							if( 1 == cmp.Compare(lr, *itR, mj->literal, mj->selOp)) {
	//								cout <<"l * R"<<endl;
								joinNum++;
	//								rr = new Record();
								rr->Copy(*itR);
	//								jr = new Record();
//								cout <<"producing the join record!"<<endl;
								jr->MergeRecords(lr, rr, leftAttr, rightAttr, attrToKeep, leftAttr+rightAttr, leftAttr);
//								cout <<"join one record"<<endl;
								mj->outPipe->Insert(jr);
							}
						}
						delete lr;
					}
					//empty the two vectors
	//					cout <<"Empty the two vectors"<<endl;
					CLEANUPVECTOR(vectorL);
					CLEANUPVECTOR(vectorR);

//					cout <<"leftOK?"<<leftOK<<" rightOK?"<<rightOK<<endl;
//					cout <<"The "<<num<<" time join"<<endl;
					break;
				}
				case 1: // L > R
//					cout <<"L > R!"<<endl;
					leftOK = true;
//					delete rcdR;
					if(pipeR.Remove(rcdR))
						rightOK = true;
					break;
				case -1: // L < R
//					cout <<"L < R!"<<endl;
					rightOK = true;
//					delete rcdL;
					if(pipeL.Remove(rcdL))
						leftOK = true;
					break;
				}
			}
//			cout <<"sort join: " <<joinNum<<" join times: "<<num<<endl;
	}
} else { //----------Block Nested Loop Join-----------------
		cout <<"block nested loops join"<<endl;
		//assume the size of left relation is less than right relation
		int n_pages = 10;
		// take n_pages-1 pages from right, and 1 page from left
		Record *rcdL = new Record;
		Record *rcdR = new Record;
		Page pageR;
		DBFile dbFileL;
			fType ft = heap;
			dbFileL.Create((char*)"tmpL", ft, NULL);
			dbFileL.MoveFirst();

		int leftAttr, rightAttr, totalAttr, *attrToKeep;

		if(mj->inPipeL->Remove(rcdL) && mj->inPipeR->Remove(rcdR)) {
					//figure out the attributes of LHS record and RHS record
			leftAttr = ((int *) rcdL->bits)[1] / sizeof(int) -1;
			rightAttr = ((int *) rcdR->bits)[1] / sizeof(int) -1;
			totalAttr = leftAttr + rightAttr;
			attrToKeep = new int[totalAttr];
			for(int i = 0; i< leftAttr; i++)
				attrToKeep[i] = i;
			for(int i = 0; i< rightAttr; i++)
				attrToKeep[i+leftAttr] = i;

			//first transfer records from inPipeL to dbFileL;
//			int ttt = 0;
			do {
				dbFileL.Add(*rcdL);
//				ttt ++;
			}while(mj->inPipeL->Remove(rcdL));

//			cout <<"getting left records into file, length: "<<ttt<<endl;

			vector<Record *> vectorR;
			ComparisonEngine cmp;

			bool rMore = true;
			int joinNum =0;
			while(rMore) {
				Record *first = new Record();
				first->Copy(rcdR);
				pageR.Append(rcdR);
				vectorR.push_back(first);
				int rPages = 0;

				rMore = false;
				while(mj->inPipeR->Remove(rcdR)) {
					//getting n-1 pages of records into vectorR
					Record *copyMe = new Record();
					copyMe->Copy(rcdR);
					if(!pageR.Append(rcdR)) {
						rPages += 1;
						if(rPages >= n_pages -1) {
							rMore = true;
							break;
						}
						else {
							pageR.EmptyItOut();
							pageR.Append(rcdR);
							vectorR.push_back(copyMe);
						}
					} else {
						vectorR.push_back(copyMe);
					}
				}
				cout <<"r block number(9): "<<rPages<<endl;
				cout <<"vecotr size: " <<vectorR.size()<<endl;
				// now we have the n-1 pages records from Right relation
				dbFileL.MoveFirst(); //we should do this in each iteration
				//iterate all the tuples in Left
				int fileRN = 0;
				while(dbFileL.GetNext(*rcdL)) {
					for(vector<Record*>::iterator it=vectorR.begin(); it!=vectorR.end(); it++) {
						if(1 == cmp.Compare(rcdL, *it, mj->literal, mj->selOp)) {
							//applied to the CNF, then join
							joinNum++;
							Record *jr = new Record();
							Record *rr = new Record();
							rr->Copy(*it);
							jr->MergeRecords(rcdL, rr, leftAttr, rightAttr, attrToKeep, leftAttr+rightAttr, leftAttr);
							mj->outPipe->Insert(jr);
						}
					}
				}
				cout <<"getting file records: " <<fileRN<<endl;
				//clean up the vectorR
				CLEANUPVECTOR(vectorR);
			}
			cout <<"block join records: " <<joinNum<<endl;
			dbFileL.Close();
		}
	}

	cout <<"join shutting down the out pipe!"<<endl;
	mj->outPipe->ShutDown();
	return NULL;
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create(&thread, NULL, join, this);
}
void Join:: WaitUntilDone () {
	pthread_join (thread, NULL);
}
void Join::Use_n_Pages (int n) {
	this->nPages = n;
}

//-----------------DuplicateRemoval--------------
void *DuplicateRemoval::duplicateRemoval(void *arg) {
	cout <<"duplicate removal"<<endl;
	DuplicateRemoval *dm = (DuplicateRemoval *) arg;
	//create the ordermaker according to schema
	OrderMaker *om = new OrderMaker(dm->mySchema);
	if(!om)
		cerr <<"Can not allocate ordermaker!"<<endl;
	Pipe sortPipe(PIPE_SIZE);
	int runlen = 5;//temp...
	BigQ *bigQ = new BigQ(*(dm->inPipe), sortPipe, *om, runlen);

	ComparisonEngine cmp;
	Record *tmp = new Record();
	if(sortPipe.Remove(tmp)) {
		//insert the first one
		bool more = true;
		while(more) {
			more = false;
			Record *copyMe = new Record();
			copyMe->Copy(tmp);
			dm->outPipe->Insert(copyMe);

			Record *chk = new Record();
			while(sortPipe.Remove(chk)) {
				if(cmp.Compare(tmp, chk, om) != 0) { //not equal
					delete tmp;
					tmp = chk;
					more = true;
					break;
				}
			}
		}
	}

	dm->outPipe->ShutDown();
	return NULL;
}
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->mySchema = &mySchema;
	pthread_create(&thread, NULL, duplicateRemoval, this);
}
void DuplicateRemoval::WaitUntilDone () {
	pthread_join (thread, NULL);
}
void DuplicateRemoval::Use_n_Pages (int n) {
	this->nPages = n;
}

//-----------------Sum---------------------------
void *Sum::sum(void *arg) {
	cout <<"Sum"<<endl;
	Sum *sum = (Sum *) arg;
	Record *tmpRcd = new Record;

	double result=0.0;
	Type type;
	while(sum->inPipe->Remove(tmpRcd)) {
		int ir; double dr;
		type = sum->computeMe->Apply(*tmpRcd, ir, dr);
		result += (ir + dr);
	}

	delete tmpRcd;
	//compose a record and put it into the outPipe;
	Attribute attr;
	attr.name = (char *)"sum";
	attr.myType = (Type)DOUBLE;

	Schema *schema = new Schema ((char *)"dummy", 1, &attr);
//	cout <<"Yahui: " <<schema->GetNumAtts()<<endl;

	ostringstream ss;
	ss <<result;
	ss <<"|";
//	cout <<"Yahui: " << ss.str().c_str()<<endl;

	Record *rcd = new Record();
	rcd->ComposeRecord(schema, ss.str().c_str());
//	rcd->Print(schema);

	sum->outPipe->Insert(rcd);
	sum->outPipe->ShutDown();

	delete rcd;
	delete schema;
	return NULL;
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->computeMe = &computeMe;
	pthread_create(&thread, NULL, sum, this);
}
void Sum::WaitUntilDone () {
	pthread_join (thread, NULL);
}
void Sum::Use_n_Pages (int n) {
	this->nPages = n;
}
//-----------------GroupBy-----------------------
void *GroupBy::groupBy(void *arg) {
	GroupBy *gb = (GroupBy *) arg;
		cout <<"GroupBY."<<endl;

		Pipe sortPipe(PIPE_SIZE);
		BigQ *bigQ = new BigQ(*(gb->inPipe), sortPipe, *(gb->groupAtts), 50);

		int ir;  double dr;
		Type type;

		ComparisonEngine cmp;
		Record *tmpRcd = new Record();
//		vector<Record *> group;
		if(sortPipe.Remove(tmpRcd)) {

			Attribute attr;
			attr.name = (char *)"sum";
			attr.myType = type;

			int rcdAttr = ((int *) tmpRcd->bits)[1] / sizeof(int) -1;
			int totalAttr = rcdAttr+1;
			int attrToKeep[totalAttr];
			attrToKeep[0] = 0;

			for(int i = 1; i< totalAttr; i++)
				attrToKeep[i] = i-1;

			bool more = true;
			while(more) {
				//new group
				more = false;
	//			cout <<"a new group"<<endl;
				type = gb->compteMe->Apply(*tmpRcd, ir, dr);
//				group.push_back(tmpRcd);
				double sum=0;
				sum += (ir+dr);

				Record *r = new Record();
				Record *lastRcd = new Record;
				lastRcd->Copy(tmpRcd);
				while(sortPipe.Remove(r)) {
	//				cout <<"getting next one"<<endl;
					if(cmp.Compare(tmpRcd, r, gb->groupAtts) == 0){ //same group
						type = gb->compteMe->Apply(*r, ir, dr);
						sum += (ir+dr);
//						Record *cMe = new Record();
//						lastRcd->Consume(r);
//						delete r;
//						group.push_back(cMe);
					} else {
						tmpRcd->Consume(r);
						more = true;
						break;
					}
				}
				//now produce the output tuple together with the su

				Schema *schema = new Schema ((char *)"dummy", 1, &attr);
			//	cout <<"Yahui: " <<schema->GetNumAtts()<<endl;

				ostringstream ss;
				ss <<sum;
				ss <<"|";
			//	cout <<"Yahui: " << ss.str().c_str()<<endl;

				Record *rcd = new Record();
				rcd->ComposeRecord(schema, ss.str().c_str());

//				int numAttsToKeep = gb->groupAtts->numAtts + 1;
//				int attsToKeep[numAttsToKeep];
//				attsToKeep[0] = 0;  //for sumRec
////				cout <<"[ 0, ";
//				for(int i = 1; i < numAttsToKeep; i++)
//				{
//					attsToKeep[i] = gb->groupAtts->whichAtts[i-1];
////					cout <<attsToKeep[i];
//				}
//				cout <<"]"<<endl;
				Record *tuple = new Record;
				tuple->MergeRecords(rcd, lastRcd, 1, totalAttr - 1, attrToKeep,  totalAttr, 1);

				gb->outPipe->Insert(tuple);

				//last clean up this group
//				for(vector<Record *>::iterator it = group.begin(); it!=group.end(); it++) {
//					//in fact it should produce the sum with the tuple in the vector
//					delete *it;
//					*it = NULL;
//				}
//				group.clear();
//				CLEANUPVECTOR(group);
			}
		}

		gb->outPipe->ShutDown();
		return NULL;
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->compteMe = &computeMe;
	pthread_create(&thread, NULL, groupBy, this);
}
void GroupBy::WaitUntilDone () {
	pthread_join (thread, NULL);
}
void GroupBy::Use_n_Pages (int n) {
	this->nPages = n;
}
//-----------------WriteOut-----------------------
void *WriteOut::writeOut(void *arg) {
	cout <<"write out"<<endl;
	WriteOut *wo = (WriteOut *) arg;
	int n = wo->mySchema->GetNumAtts();
	Attribute *atts = wo->mySchema->GetAtts();

	int cnt = 1;
	Record *tmpRcd = new Record();
	while(wo->inPipe->Remove(tmpRcd)) {
		//write tmpRcd into file
		// loop through all of the attributes
		char *bits = tmpRcd->bits;
		fprintf(wo->outFile, "%d: ", cnt++);
		for (int i = 0; i < n; i++) {
			// print the attribute name
			fprintf(wo->outFile, "%s",atts[i].name);

			// use the i^th slot at the head of the record to get the
			// offset to the correct attribute in the record
			int pointer = ((int *) bits)[i + 1];

			// here we determine the type, which given in the schema;
			// depending on the type we then print out the contents
			fprintf(wo->outFile, "[");

			// first is integer
			if (atts[i].myType == Int) {
				int *myInt = (int *) &(bits[pointer]);
				fprintf(wo->outFile, "%d",*myInt);

			// then is a double
			} else if (atts[i].myType == Double) {
				double *myDouble = (double *) &(bits[pointer]);
				fprintf(wo->outFile, "%f", *myDouble);

			// then is a character string
			} else if (atts[i].myType == String) {
				char *myString = (char *) &(bits[pointer]);
				fprintf(wo->outFile, "%s", myString);
			}

			fprintf(wo->outFile, "]");

			// print out a comma as needed to make things pretty
			if (i != n - 1) {
				fprintf(wo->outFile, ", ");
			}
		}

		fprintf(wo->outFile, "\n");
	}
	return NULL;
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	this->inPipe = &inPipe;
	this->outFile = outFile;
	this->mySchema = &mySchema;
	pthread_create(&thread, NULL, writeOut, this);
}
void WriteOut::WaitUntilDone () {
	pthread_join (thread, NULL);
}
void WriteOut::Use_n_Pages (int n) {
	this->nPages = n;
}
