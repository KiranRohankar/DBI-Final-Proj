/*
 * QueryPlan.cpp
 *
 *  Created on: Apr 26, 2013
 *      Author: yahuihan
 */

#include "QueryPlan.h"

QueryPlanNode::QueryPlanNode() {
	this->parent = NULL;
	this->left = NULL;
	this->right = NULL;
	this->cnf = new CNF;
	this->literal = new Record;
}
QueryPlanNode::~QueryPlanNode(){
	delete cnf;
	delete literal;
}

QueryPlan::QueryPlan() {
	this->pipeNum = 0;
	this->dbNum = 0;
	this->output = new char[50];
	FILE *fp = fopen(output_path, "r");
	fscanf(fp, "%s", this->output);
	fclose(fp);
}

QueryPlan::~QueryPlan() {
}

void QueryPlan::PrintInOrder() {
	PrintNode(root);
}

void QueryPlan::PrintNode(QueryPlanNode *node) {
	if(node->left)
		PrintNode(node->left);
	//print node
	switch(node->opType) {
	case SELECTF:
		cout <<"*****************"<<endl;
		cout <<"SelectFromFile Operation"<<endl;
		cout <<"Input File:	"<<node->dbfilePath<<endl;
		cout <<"Output Pipe: "<<node->outPipeId<<endl;
		cout <<"Output Schema: " <<endl;
			node->outputSchema->Print();
		cout <<"Select CNF: " <<endl;
		cout <<"\t"; node->cnf->Print();
		cout <<"\n\n";
		break;
	case SELECTP:
		cout <<"*****************"<<endl;
		cout <<"SelectFromPipe Operation"<<endl;
		cout <<"Input Pipe:	"<<node->lPipeId<<endl;
		cout <<"Output Pipe: "<<node->outPipeId<<endl;
		cout <<"Output Schema: " <<endl;
			node->outputSchema->Print();
		cout <<"Select CNF: " <<endl;
		cout <<"\t"; node->cnf->Print();
		cout <<"\n\n";
		break;
	case PROJECT:
		cout <<"*****************"<<endl;
		cout <<"Project Operation"<<endl;
		cout <<"Input Pipe:	"<<node->lPipeId<<endl;
		cout <<"Output Pipe: "<<node->outPipeId<<endl;
		cout <<"Output Schema: " <<endl;
		node->outputSchema->Print();
		cout <<"Attributes to keep: "<<endl;
		cout <<"\t";
		for(int i=0;i<node->numAttsOutput;i++) {
			cout <<node->keepMe[i] <<", ";
		}
		cout <<endl;
		cout <<"\n";
		break;
	case JOIN:
		cout <<"*****************"<<endl;
		cout <<"Join Operation"<<endl;
		cout <<"Left Input Pipe: "<<node->lPipeId<<endl;
		cout <<"Right Input Pipe: "<<node->rPipeId<<endl;
		cout <<"Output Pipe: "<<node->outPipeId<<endl;
		cout <<"Output Schema: " <<endl;
			node->outputSchema->Print();
		cout <<"Select CNF: " <<endl;
		cout <<"\t"; node->cnf->Print();
		cout <<"\n\n";
		break;
	case SUM:
		cout <<"*****************"<<endl;
		cout <<"Sum Operation"<<endl;
		cout <<"Input Pipe:	"<<node->lPipeId<<endl;
		cout <<"Output Pipe: "<<node->outPipeId<<endl;
		cout <<"Output Schema: " <<endl;
		node->outputSchema->Print();
		cout <<"Sum Function: " <<endl;
		node->function->Print();
		cout <<endl;
		cout <<"\n";
		break;
	case GROUP_BY:
		cout <<"*****************"<<endl;
		cout <<"GroupBy Operation"<<endl;
		cout <<"Input Pipe:	"<<node->lPipeId<<endl;
		cout <<"Output Pipe: "<<node->outPipeId<<endl;
		cout <<"Output Schema: " <<endl;
		node->outputSchema->Print();
		cout <<"Group By OrderMaker: " <<endl;
		node->orderMaker->Print();
		cout <<endl;
		cout <<"Group By Function: " <<endl;
		node->function->Print();
		cout <<endl;
		cout <<"\n";
		break;
	case DISTINCT:
		cout <<"*****************"<<endl;
		cout <<"Duplicate Removal Operation"<<endl;
		cout <<"Input Pipe:	"<<node->lPipeId<<endl;
		cout <<"Output Pipe: "<<node->outPipeId<<endl;
		cout <<"Output Schema: " <<endl;
		node->outputSchema->Print();
		cout <<"\n";
		break;
	case WRITEOUT:
		cout <<"*****************"<<endl;
		cout <<"Write Out"<<endl;
		cout <<"Input Pipe:	"<<node->lPipeId<<endl;
//		cout <<"Output Target: "<<node->writePath<<endl;
		cout <<"Output Schema: " <<endl;
		node->outputSchema->Print();
		cout <<"\n";
		break;
	default:
		break;
	}

	if(node->right)
		PrintNode(node->right);
}

// ------------------Execute ------------
int QueryPlan::ExecuteCreateTable(CreateTable *createTable) {
	DBFile *db = new DBFile;
	char dbpath[100];
	sprintf(dbpath, "%s%s.bin", dbfile_dir, createTable->tableName);
//	cout <<"create at : " <<dbpath<<endl;
	SortInfo *info = new SortInfo;
	OrderMaker *om = new OrderMaker;
	if(createTable->type == SORTED) {
		NameList *sortAtt = createTable->sortAttrList;
		while(sortAtt) {
			AttrList *atts = createTable->attrList;
			int i=0;
			while(atts) {
				if(strcmp(sortAtt->name, atts->attr->attrName)){
					//got it
					om->whichAtts[om->numAtts] = i;
					om->whichTypes[om->numAtts] = (Type) atts->attr->type;
					om->numAtts++;
					break;
				}
				i++;
				atts = atts->next;
			}
			sortAtt = sortAtt->next;
		}
		info->myorder = om;
		info->runLength = RUNLEN;
		db->Create(dbpath, sorted, (void*)info);
	} else
		db->Create(dbpath, heap, NULL );
	db->Close();
	return 1;
}

int QueryPlan::ExecuteInsertFile(InsertFile *insertFile) {
	DBFile dbfile;
	char dbpath[100];
	sprintf(dbpath, "%s%s.bin", dbfile_dir, insertFile->tableName);
	dbfile.Open(dbpath);
	char fpath[100];
	sprintf(fpath, "%s%s", tpch_dir, insertFile->fileName);
	cout <<"loading " <<fpath<<endl;
	Schema schema((char*)catalog_path, insertFile->tableName);
	dbfile.Load(schema, fpath);
	dbfile.Close();
	return 1;
}

int QueryPlan::ExecuteDropTable(char *dropTable) {
	char dbpath[100];
	sprintf(dbpath, "%s%s.bin", dbfile_dir, dropTable);
	remove(dbpath);
	sprintf(dbpath, "%s.header", dbpath);
	remove(dbpath);
}

//----------------------------------------------------------Execute------------------------------

void QueryPlan::ExecuteNode(QueryPlanNode *node) {
//	cout <<"post-order execute"<<endl;
	if(node->left)
		ExecuteNode(node->left);

	if(node->right)
		ExecuteNode(node->right);

	//last run it self
	//first new pipe, put it into the pipes
	switch(node->opType) {
	case SELECTF:
	{
		cout <<"execute selectfrom file: " <<node->dbfilePath<<endl;
		SelectFile *selectFile = new SelectFile();
//		selectFile->Use_n_Pages(RUNLEN);
		Pipe *sfOutPipe = new Pipe(PIPE_SIZE);
		//add it to the pipe
		this->pipes[node->outPipeId] = sfOutPipe;
		dbs[this->dbNum] = new DBFile;
		dbs[this->dbNum]->Open((char*)node->dbfilePath.c_str());
		dbs[this->dbNum]->MoveFirst();
		selectFile->Run(*(dbs[this->dbNum++]), *sfOutPipe, *(node->cnf), *(node->literal));
//		this->operators.push_back(selectFile);
		break;
	}
	case SELECTP:
	{
		SelectPipe *selectPipe = new SelectPipe();
		selectPipe->Use_n_Pages(RUNLEN);
		Pipe *spOutPipe = new Pipe(PIPE_SIZE);
		//add it to the pipe
		this->pipes[node->outPipeId] = spOutPipe;
		Pipe *splPipe = this->pipes[node->lPipeId];
		selectPipe->Run(*splPipe, *spOutPipe, *(node->cnf), *(node->literal));
//		this->operators.push_back(selectPipe);
		break;
	}
	case PROJECT:
	{
		cout <<"Execute Project..."<<endl;
		Project *project = new Project;
//		project->Use_n_Pages(RUNLEN);
		Pipe *pOutPipe = new Pipe(PIPE_SIZE);
		//add it to the pipe
		this->pipes[node->outPipeId] = pOutPipe;
		Pipe *plPipe = this->pipes[node->lPipeId];
		project->Run(*plPipe, *pOutPipe, node->keepMe, node->numAttsInput, node->numAttsOutput);
//		this->operators.push_back(project);
		break;
	}
	case JOIN:
	{
		cout <<"Execute Join..."<<endl;
		Join *join = new Join;
//		join->Use_n_Pages(RUNLEN);
		Pipe *jOutPipe = new Pipe(PIPE_SIZE);
		//add it to the pipe
		this->pipes[node->outPipeId] = jOutPipe;
		Pipe *jlPipe = this->pipes[node->lPipeId];
		Pipe *jrPipe = this->pipes[node->rPipeId];
		join->Run(*jlPipe, *jrPipe, *jOutPipe, *(node->cnf), *(node->literal));
//		this->operators.push_back(join);
		break;
	}
	case SUM:
	{
		cout <<"Execute Sum..."<<endl;
		Sum *sum = new Sum;
//		sum->Use_n_Pages(RUNLEN);
		Pipe *sOutPipe = new Pipe(PIPE_SIZE);
		//add it to the pipe
		this->pipes[node->outPipeId] = sOutPipe;
		Pipe *slPipe = this->pipes[node->lPipeId];
		sum->Run(*slPipe, *sOutPipe, *(node->function));
//		this->operators.push_back(sum);
		break;
	}
	case GROUP_BY:
	{
		cout <<"Execute Group BY..."<<endl;
		GroupBy *groupBy = new GroupBy;
//		groupBy->Use_n_Pages(RUNLEN);
		Pipe *gbOutPipe = new Pipe(PIPE_SIZE);
		this->pipes[node->outPipeId] = gbOutPipe;
		Pipe *gblPipe = this->pipes[node->lPipeId];
		groupBy->Run(*gblPipe, *gbOutPipe, *(node->orderMaker), *(node->function));
//		this->operators.push_back(groupBy);
		break;
	}
	case DISTINCT:
	{
		cout <<"Execute Distinct...."<<endl;
		DuplicateRemoval *dr = new DuplicateRemoval;
//		dr->Use_n_Pages(RUNLEN);
		Pipe *drOutPipe = new Pipe(PIPE_SIZE);
		this->pipes[node->outPipeId] = drOutPipe;
		Pipe *drlPipe = this->pipes[node->lPipeId];
		dr->Run(*drlPipe, *drOutPipe, *(node->left->outputSchema));
//		this->operators.push_back(dr);
		break;
	}
	case WRITEOUT:
	{
		cout <<"Execute writeout..."<<endl;
		WriteOut *wo = new WriteOut;
//		wo->Use_n_Pages(RUNLEN);
		Pipe *wlPipe = this->pipes[node->lPipeId];
		wo->Run(*wlPipe, node->outFile, *(node->outputSchema));
//		cout <<"total pipe size: " <<this->pipes.size()<<endl;
		this->operators.push_back(wo);
		break;
	}
	default:
		break;
	}
}

int QueryPlan::ExecuteQueryPlan() {
//	cout <<"out put to " <<this->output<<endl;

	if( strcmp(this->output, "NONE") == 0) { //just print out the query plan
		this->PrintInOrder();
	} else {
		QueryPlanNode *writeOut = new QueryPlanNode;
		writeOut->opType = WRITEOUT;
		writeOut->left = this->root;
		writeOut->lPipeId = writeOut->left->outPipeId;
		writeOut->outputSchema = writeOut->left->outputSchema;
		if( strcmp(this->output, "STDOUT") == 0) {
			writeOut->outFile = stdout;
		} else {
			FILE *fp = fopen(this->output, "w");
			writeOut->outFile = fp;
		}
//		this->PrintNode(writeOut);
		this->ExecuteNode(writeOut);
		for(vector<RelationalOp *>::iterator roIt=this->operators.begin(); roIt!=this->operators.end();roIt++){
			RelationalOp *op = *roIt;
			op->WaitUntilDone();
		}
	}
	return 1;
}

