/*
 * QueryPlan.h
 *
 *  Created on: Apr 26, 2013
 *      Author: yahuihan
 */

//Class for to store one query plan for enumeration
#ifndef QUERYPLAN_H_
#define QUERYPLAN_H_

#include "RelOp.h"
#include "Pipe.h"
#include <map>
//#include "Defs.h"
#include <string.h>
#include "ParseTree.h"
#include "DBFile.h"
#include <iostream>
#include <fstream>
#include <vector>

using namespace std;

//char *catalog_path;
//char *dbfile_dir;
//char *tpch_dir;

class QueryPlanNode {
public:
//	RelationalOp *relOperator;

	QueryNodeType opType;

	QueryPlanNode *parent;//useful for join
	QueryPlanNode *left;
	QueryPlanNode *right;

	int lPipeId;
//	Pipe *lPipe;
	int rPipeId;
//	Pipe *rPipe;
	int outPipeId;
//	Pipe *outPipe;

	FILE *outFile; //stdout or file for writeout

	string dbfilePath;

	CNF *cnf;
	Record *literal;
	Schema *outputSchema;
	Function *function;

	OrderMaker *orderMaker;
	//for project
	int *keepMe;
	int numAttsInput, numAttsOutput;

	QueryPlanNode();
	~QueryPlanNode();
};

class QueryPlan {
public:
	QueryPlan();
	virtual ~QueryPlan();

	QueryPlanNode *root;

	int pipeNum;
	map<int, Pipe*> pipes; //used when execute
	vector<RelationalOp *> operators;

	int dbNum;
	DBFile *dbs[10];

	char* output;

	void PrintNode(QueryPlanNode *);
	void PrintInOrder();

	void ExecuteNode(QueryPlanNode *);
	int ExecuteQueryPlan();
	int ExecuteCreateTable(CreateTable*);
	int ExecuteInsertFile(InsertFile*);
	int ExecuteDropTable(char *);

	//Generating
//	void GenerateSelectFromFile(TableList *, map<string, AndList*>*);
};

#endif /* QUERYPLAN_H_ */
