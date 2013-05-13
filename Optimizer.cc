/*
 * Optimizer.cpp
 *
 *  Created on: Apr 26, 2013
 *      Author: yahuihan
 */

#include "Optimizer.h"

Optimizer::Optimizer() {
	// TODO Auto-generated constructor stub
}

Optimizer::Optimizer(struct FuncOperator *finalFunction,
			struct TableList *tables,
			struct AndList * boolean,
			struct NameList * pGrpAtts,
	        struct NameList * pAttsToSelect,
	        int distinct_atts, int distinct_func,
	        Statistics *s) {
	this->finalFunction = finalFunction;
	this->tables = tables;
	this->cnfAndList = boolean;
	this->groupAtts = pGrpAtts;
	this->selectAtts = pAttsToSelect;
	this->distinctAtts = distinct_atts;
	this->distinctFunc = distinct_func;
	this->statistics = s;
}

//void Optimizer::GetTables(vector<string> &relations) {
//	TableList *list = tables;
//	while(list) {
//		if(list->aliasAs) {
//			relations.push_back(list->aliasAs);
//			this->statistics->CopyRel(list->tableName, list->aliasAs);
//		} else{
//			relations.push_back(list->tableName);
//		}
//		list = list->next;
//	}
//}

void Optimizer::GetJoinsAndSelects(vector<AndList*> &joins, vector<AndList*> &selects,
			vector<AndList*> &selAboveJoin) {
	OrList *aOrList;
	AndList *aAndList = this->cnfAndList;
	while(aAndList) {
		aOrList = aAndList->left;
		if(!aOrList) {
			cerr <<"Error in cnf AndList"<<endl;
			return;
		}
		if(aOrList->left->code == EQUALS && aOrList->left->left->code == NAME
					&& aOrList->left->right->code == NAME){ //A.a = B.b
			AndList *newAnd = new AndList();
			newAnd->left= aOrList;
			newAnd->rightAnd = NULL;
			joins.push_back(newAnd);
		} else {
			if(!aOrList->rightOr) {  // A.a = 3 like
//				cout <<"right or null"<<endl;
				AndList *newAnd = new AndList();
				newAnd->left= aOrList;
				newAnd->rightAnd = NULL;
				selects.push_back(newAnd);
			} else {  // like A.a=3 OR A.b<10 ,  A.a=3 OR B.b >10, etc
				vector<string> involvedTables;
				OrList *olp = aOrList;
				while(aOrList != NULL){
					Operand *op = aOrList->left->left;
					if(op->code != NAME){
						op = aOrList->left->right;
					}
					string rel;
					if(this->statistics->ParseRelation(string(op->value), rel) ==0) {
						cerr <<"Error in parse relations"<<endl;
						return;
					}
					//clog << "Parsed out relation " << rel << endl;
					if(involvedTables.size() == 0){
						involvedTables.push_back(rel);
					}
					else if(rel.compare(involvedTables[0]) != 0){
							involvedTables.push_back(rel);
					}

					aOrList = aOrList->rightOr;
				}

				if(involvedTables.size() > 1){
					AndList *newAnd = new AndList();
					newAnd->left= olp;
					newAnd->rightAnd = NULL;
					selAboveJoin.push_back(newAnd);
				}
				else{
					AndList *newAnd = new AndList();
					newAnd->left= olp;
					newAnd->rightAnd = NULL;
					selects.push_back(newAnd);
				}
			}
		}
		aAndList = aAndList->rightAnd;
	}
}

map<string, AndList*>* Optimizer::OptimizeSelectAndApply(vector<AndList*> selects) {
	map<string, AndList*> *selectors = new map<string, AndList*>;
	for(vector<AndList*>::iterator it=selects.begin(); it!=selects.end();it++) {
		AndList *aAndList = *it;
		Operand *op = aAndList->left->left->left;
		if(op->code != NAME)
			op = aAndList->left->left->right;
		string rel;
		this->statistics->ParseRelation(string(op->value), rel);

		//add it to the select operator
		map<string, AndList*>::iterator mit;
		for(mit=selectors->begin(); mit!=selectors->end(); mit++) {
			if(mit->first.compare(rel) == 0) {
				AndList *lastAnd = mit->second;
				while(lastAnd->rightAnd!=NULL)
					lastAnd = lastAnd->rightAnd;
				lastAnd->rightAnd = aAndList;
				break;
			}
		}
		if(mit == selectors->end()) //new selector
			selectors->insert(make_pair(rel, aAndList));
	}

//	map<string, AndList*>::iterator mit;
//	for(mit=selectors->begin(); mit!=selectors->end(); mit++) {
//		char *name[] = {(char*)mit->first.c_str()};
//		this->statistics->Apply(mit->second, name, 1);
//	}
	return selectors;
}

vector<AndList*>* Optimizer::OptimizeJoinOrder(vector<AndList*> joins) {
	//A greedy algorithm to pick the sort order
	vector<AndList*> *orderedAndList = new vector<AndList*>;
	orderedAndList->reserve(joins.size());
	if(joins.size() <=1 ) {
		if(joins.size()==1)
			orderedAndList->push_back(joins[0]);
	} else {
//		set<string> joinedRels;
		int size = joins.size();
		for(int i=0; i<size; i++) {
			int joinNum = 2;
			double smallest = 0.0;
			string left_rel, right_rel;
			AndList *chooseAndList;
			int choosePos = -1;
			for(int j=0; j<joins.size(); j++ ){
				string rel1, rel2;
				AndList *andList = joins[j];
				Operand *l = andList->left->left->left;
				Operand *r = andList->left->left->right;
				this->statistics->ParseRelation(string(l->value), rel1);
				this->statistics->ParseRelation(string(r->value), rel2);

				//check wether this
//				for(set<string>::iterator it=joinedRels.begin();
//							it!=joinedRels.end(); it++) {
//					if(rel1.compare(*it) || rel2.compare(*it)) {
//						joinNum = joinedRels.size()+1;
//						break;
//					}
//				}

				char *estrels[] = {(char*)rel1.c_str(), (char*)rel2.c_str()};
				double cost = this->statistics->Estimate(andList,estrels, size*2);
				if(choosePos == -1 || cost < smallest) {
					smallest = cost;
					left_rel = rel1;
					right_rel = rel2;
					chooseAndList = andList;
					choosePos = j;
				}
			}
			//find the andList
			orderedAndList->push_back(chooseAndList);
			char *aplyrels[] = {(char*) left_rel.c_str(), (char*)right_rel.c_str()};
			this->statistics->Apply(chooseAndList, aplyrels, size*2);
//			joinedRels.insert(left_rel);
//			joinedRels.insert(right_rel);
			joins.erase(joins.begin()+choosePos);
		}
		return orderedAndList;
	}
}

//-------------------------print out function for test -----------Remember to comment

void PrintOperand(struct Operand *pOperand)
{
        if(pOperand!=NULL)
        {
                cout<<pOperand->value<<" ";
        }
        else
                return;
}

void PrintComparisonOp(struct ComparisonOp *pCom)
{
        if(pCom!=NULL)
        {
                PrintOperand(pCom->left);
                switch(pCom->code)
                {
                        case LESS_THAN:
                                cout<<" < "; break;
                        case GREATER_THAN:
                                cout<<" > "; break;
                        case EQUALS:
                                cout<<" = ";
                }
                PrintOperand(pCom->right);

        }
        else
        {
                return;
        }
}
void PrintOrList(struct OrList *pOr)
{
        if(pOr !=NULL)
        {
                struct ComparisonOp *pCom = pOr->left;
                PrintComparisonOp(pCom);

                if(pOr->rightOr)
                {
                        cout<<" OR ";
                        PrintOrList(pOr->rightOr);
                }
        }
        else
        {
                return;
        }
}
void PrintAndList(struct AndList *pAnd)
{
        if(pAnd !=NULL)
        {
                struct OrList *pOr = pAnd->left;
                PrintOrList(pOr);
                if(pAnd->rightAnd)
                {
                        cout<<" AND ";
                        PrintAndList(pAnd->rightAnd);
                }
        }
        else
        {
                return;
        }
}

void printStringVector(vector<string> v) {
	for(vector<string>::iterator it=v.begin(); it!=v.end(); it++) {
		cout <<*it<<", ";
	}
	cout <<endl;
}
void printAndListVector(vector<AndList*> *v) {
	for(vector<AndList*>::iterator it=v->begin(); it!=v->end(); it++) {
		PrintAndList(*it);
		cout <<";  ";
	}
	cout <<endl;
}
//---------------------------print out functions end-------------
OrderMaker *Optimizer::GenerateOM(Schema *schema) {
//	cout <<"Generate OM"<<endl;
	OrderMaker *order = new OrderMaker();
	NameList *name = this->groupAtts;
	while(name) {
		order->whichAtts[order->numAtts] = schema->Find(name->name);
		order->whichTypes[order->numAtts] = schema->FindType(name->name);
		order->numAtts++;
		name=name->next;
	}
	return order;
}
Function *Optimizer::GenerateFunc(Schema *schema) {
//	cout <<"Generate Function"<<endl;
//	schema->Print();
	Function *func = new Function();
	func->GrowFromParseTree(this->finalFunction, *schema);
	return func;
}

//-----------------------------------------------------build up the query plan---------
QueryPlan * Optimizer::OptimizedQueryPlan() {
	TableList *list = tables;
	while(list) {
		if(list->aliasAs) {
			this->statistics->CopyRel(list->tableName, list->aliasAs);
		}
		list = list->next;
	}

	vector<AndList*> joins;
	vector<AndList*> selects, selAboveJoin;
	GetJoinsAndSelects(joins, selects, selAboveJoin);

	map<string, AndList*>* selectors = this->OptimizeSelectAndApply(selects);
//	cout <<"optimized selector: " <<endl;
//	map<string, AndList*>::iterator mit = selectors->begin();
//	for(;mit!=selectors->end();mit++) {
//		cout <<mit->first<<": ";
//		PrintAndList(mit->second);
//		cout <<";"<<endl;
//	}
//	cout <<"Ordered Join: "<<endl;
	vector<AndList*>* orderedJoins = this->OptimizeJoinOrder(joins);
//	printAndListVector(orderedJoins);

	//-------now build the query plan ------
	QueryPlan *queryPlan = new QueryPlan();
	//first build the select from file
	map<string, QueryPlanNode *> selectFromFiles; //store the selector
//	cout <<"SelectFromFile Building"<<endl;
	for(TableList *table=this->tables; table!=NULL; table=table->next) {
		QueryPlanNode *selectFile = new QueryPlanNode;
		selectFile->opType = SELECTF;
		char name[100];
		sprintf(name, "%s%s.bin", dbfile_dir, table->tableName);
//		cout <<"dbfile path: " <<name<<endl;
		selectFile->dbfilePath = string(name);
		selectFile->outPipeId = queryPlan->pipeNum++;
//		cout <<selectFile->outPipeId<<endl;
		selectFile->outputSchema = new Schema(catalog_path, table->tableName);

		string relName(table->tableName);
		if(table->aliasAs) { //supp AS s
			selectFile->outputSchema->AdjustSchemaWithAlias(table->aliasAs);
			relName = string(table->aliasAs);
		}

//		selectFile->outputSchema->Print();
		map<string, AndList*>::iterator it;
		for(it=selectors->begin(); it!=selectors->end(); it++) {
			if(relName.compare(it->first)==0)
				break;
		}
		AndList *andList;
		if(it==selectors->end())
			andList = NULL;
		else
			andList = it->second;
		selectFile->cnf->GrowFromParseTree(andList, selectFile->outputSchema, *(selectFile->literal));
//		selectFile->cnf->Print();
		selectFromFiles.insert(make_pair(relName, selectFile));
	}

	//------------then build the joins
	map<string, QueryPlanNode *> builtJoins;
	QueryPlanNode *join = NULL;

	if(orderedJoins->size()>0) {
//		cout <<"orderedJoins size: " <<orderedJoins->size()<<endl;
//		PrintAndList(*(orderedJoins->begin()));
		for(vector<AndList*>::iterator jIt=orderedJoins->begin(); jIt!=orderedJoins->end(); jIt++) {
//			cout <<"Join Building"<<endl;
			AndList *aAndList = *jIt;
//			cout <<"a And List"<<endl;
//			PrintAndList(*jIt);
//			cout <<"print "<<endl;
			Operand *leftAtt = aAndList->left->left->left;
			string leftRel; this->statistics->ParseRelation(string(leftAtt->value), leftRel);
//			cout <<"left: " <<leftRel<<endl;
			Operand *rightAtt = aAndList->left->left->right;
			string rightRel; this->statistics->ParseRelation(string(rightAtt->value), rightRel);
//			cout <<"right: " <<rightRel<<endl;
			join = new QueryPlanNode;
			join->opType = JOIN;
//			cout <<"new query node"<<endl;
			QueryPlanNode *leftUpMost = builtJoins[leftRel];
			QueryPlanNode *rightUpMost = builtJoins[rightRel];
			if(!leftUpMost && !rightUpMost) { // !A and !B
				join->left = selectFromFiles[leftRel];
				join->right = selectFromFiles[rightRel];
//				join->lPipeId = join->left->outPipeId;
//				join->rPipeId = join->right->outPipeId;
				join->outputSchema = new Schema(join->left->outputSchema, join->right->outputSchema);
			} else if(leftUpMost) { //A and !B
				while(leftUpMost->parent )
					leftUpMost = leftUpMost->parent;
				join->left = leftUpMost; leftUpMost->parent = join;
				join->right = selectFromFiles[rightRel];
			} else if(rightUpMost) { //!A and B
				while(rightUpMost->parent)
					rightUpMost = rightUpMost->parent;
				join->left = rightUpMost; rightUpMost->parent = join;
				join->right = selectFromFiles[leftRel];
			} else { // A and B
				while(leftUpMost->parent )
					leftUpMost = leftUpMost->parent;
				while(rightUpMost->parent)
					rightUpMost = rightUpMost->parent;
				join->left = leftUpMost;
				leftUpMost->parent = join;
				join->right  = rightUpMost;
				rightUpMost->parent = join;
			}
//			cout <<"find the upmost join"<<endl;
			builtJoins[leftRel] = join;
			builtJoins[rightRel] = join;
			join->lPipeId = join->left->outPipeId;
			join->rPipeId = join->right->outPipeId;
			join->outputSchema = new Schema(join->left->outputSchema, join->right->outputSchema);
			join->outPipeId = queryPlan->pipeNum++;
//			join->cnf->GrowFromParseTree(aAndList, join->outputSchema, *(join->literal));
			join->cnf->GrowFromParseTree(aAndList, join->left->outputSchema, join->right->outputSchema, *(join->literal));
		}
	}

//	cout <<"Join done!"<<endl;
	// the selection above join
	QueryPlanNode *selAbvJoin = NULL;
	if(selAboveJoin.size() > 0 ) {
		selAbvJoin = new QueryPlanNode;
		selAbvJoin->opType = SELECTP;
		if(join==NULL) {
			selAbvJoin->left = selectFromFiles.begin()->second;
		} else {
			selAbvJoin->left = join;
		}
		selAbvJoin->lPipeId = selAbvJoin->left->outPipeId;
		selAbvJoin->outPipeId = queryPlan->pipeNum++;
		selAbvJoin->outputSchema = selAbvJoin->left->outputSchema;
		AndList *andList = *(selAboveJoin.begin());
		for(vector<AndList*>::iterator it=selAboveJoin.begin(); it!= selAboveJoin.end(); it++) {
			if(it!=selAboveJoin.begin()) {
				andList->rightAnd = *it;
			}
		}
		selAbvJoin->cnf->GrowFromParseTree(andList, selAbvJoin->outputSchema, *(selAbvJoin->literal));
	}

	//build group by if any
	QueryPlanNode *groupBy = NULL;
	if(this->groupAtts) {
//		cout <<"Group BY building!"<<endl;
		groupBy = new QueryPlanNode;
		groupBy->opType = GROUP_BY;
		if(selAbvJoin) {
			groupBy ->left = selAbvJoin;
		} else if(join) {
			groupBy->left = join;
		} else {
			groupBy->left = selectFromFiles.begin()->second;
		}
		groupBy->lPipeId = groupBy->left->outPipeId;
		groupBy->outPipeId = queryPlan->pipeNum++;
//		cout <<"group by pipe: " <<groupBy->outPipeId<<endl;
		groupBy->orderMaker = this->GenerateOM(groupBy->left->outputSchema);
		groupBy->function = this->GenerateFunc(groupBy->left->outputSchema);

		Attribute *attr = new Attribute[1];
		attr[0].name = (char *)"sum";
		attr[0].myType = Double;
		Schema *sumSchema = new Schema ((char *)"dummy", 1, attr);
//		groupBy->outputSchema = new Schema(sumSchema, groupBy->left->outputSchema);
		NameList *attName = this->groupAtts;
		int numGroupAttrs = 0;
		while(attName) {
			numGroupAttrs ++;
			attName = attName->next;
		}
		if(numGroupAttrs == 0) {
			groupBy->outputSchema = sumSchema;
		} else {
			Attribute *attrs = new Attribute[numGroupAttrs];
			int i = 0;
			attName = this->groupAtts;
			while(attName) {
				attrs[i].name = strdup(attName->name);
				attrs[i++].myType = groupBy->left->outputSchema->FindType(attName->name);
				attName = attName->next;
			}
			Schema *outSchema = new Schema((char *)"dummy", numGroupAttrs, attrs);
			groupBy->outputSchema = new Schema(sumSchema, outSchema);
		}
	}

	//------build the SUM if any------
	QueryPlanNode *sum = NULL;
	if(groupBy == NULL && this->finalFunction!=NULL) {
//		cout <<"Sum Building"<<endl;
		sum = new QueryPlanNode;
		sum->opType = SUM;
		if(selAbvJoin)
			sum->left = selAbvJoin;
		else if(join)
			sum->left = join;
		else
			sum->left = selectFromFiles.begin()->second;
		sum->lPipeId = sum->left->outPipeId;
		sum->outPipeId = queryPlan->pipeNum++;
		sum->function = this->GenerateFunc(sum->left->outputSchema);

		Attribute *attr = new Attribute[1];
		attr[0].name = (char *)"sum";
		attr[0].myType = Double;
//		string dummy("dummy");
		sum->outputSchema = new Schema ((char *)"dummy", 1, attr);
	}

	//-------build the project --------
	QueryPlanNode *project = new QueryPlanNode;
	project->opType = PROJECT;
	int outputNum = 0;
	NameList *name = this->selectAtts;
	Attribute *outputAtts;
	while(name) {  // Getting the output Attr num
		outputNum++;
		name = name->next;
	}
	int ithAttr = 0;
	if(groupBy) {
			project->left = groupBy;
			outputNum++;
			project->keepMe = new int[outputNum];
			project->keepMe[0] = groupBy->outputSchema->Find((char *)"sum");
			outputAtts = new Attribute[outputNum+1];
			outputAtts[0].name = (char *)"sum";
			outputAtts[0].myType = Double;
			ithAttr = 1;
	}else if(sum) { // we have SUM
		project->left = sum;
		outputNum++;
		project->keepMe = new int[outputNum];
		project->keepMe[0] = sum->outputSchema->Find((char *) "sum");
		outputAtts = new Attribute[outputNum];
		outputAtts[0].name = (char*)"sum";
		outputAtts[0].myType = Double;
		ithAttr = 1;
	}else if(join) {
		project->left = join;
		if(outputNum == 0) {
			cerr <<"No attributes assigned to select!"<<endl;
			return NULL;
		}
		project->keepMe = new int[outputNum];
		outputAtts = new Attribute[outputNum];
	} else {
		project->left = selectFromFiles.begin()->second;
		if(outputNum == 0) {
			cerr <<"No attributes assigned to select!"<<endl;
			return NULL;
		}
		project->keepMe = new int[outputNum];
		outputAtts = new Attribute[outputNum];
	}
	name = this->selectAtts;
	while(name) {
		project->keepMe[ithAttr] = project->left->outputSchema->Find(name->name);
		outputAtts[ithAttr].name = name->name;
		outputAtts[ithAttr].myType = project->left->outputSchema->FindType(name->name);
		ithAttr++;
		name = name->next;
	}
	project->numAttsInput = project->left->outputSchema->GetNumAtts();
	project->numAttsOutput = outputNum;
	project->lPipeId = project->left->outPipeId;
	project->outPipeId = queryPlan->pipeNum++;
	project->outputSchema = new Schema((char*)"dummy", outputNum, outputAtts);

	queryPlan->root = project;

	//-------build the distinct -------
	QueryPlanNode *distinct = NULL;
	if(this->distinctAtts) { //we have distinct
//		cout <<"Distince Building!"<<endl;
		distinct = new QueryPlanNode;
		distinct->opType = DISTINCT;
		distinct->left = project;
		distinct->lPipeId = distinct->left->outPipeId;
		distinct->outputSchema = distinct->left->outputSchema;
		distinct->outPipeId = queryPlan->pipeNum++;
		queryPlan->root = distinct;
	}

	return queryPlan;
}

Optimizer::~Optimizer() {
	// TODO Auto-generated destructor stub
}

