/* Part-A Equijoin implementation */

#include "exsort.h"
#include "header.h"
#include "equijoin.h"
#include "utils.h"
#include <assert.h>

int** make2dint(int,int);


static void *buffer1, *buffer2, *outbuffer;		//buffers
static FILE *out, *tempfile1, *tempfile2;
static int **attributes1, recsize1, maxrec1, *joinparams1;
static int **attributes2, recsize2, maxrec2, *joinparams2;
static int **attributesout, recsizeout, maxrecout;

static FILE* open_file(char* name, const char* mode){
	FILE* fp=fopen(name,mode);
	if(!fp)
	{
		printf("File not found!\n");
		return NULL;	//error
	}
	return fp;		//successful
}

//!Returns 1 if greater, 0 if equal and -1 if smaller
static int joincompare(void* rec1, void* rec2, int r1row, int r2row, int numjoinattrs, int joinattrlist1[], int joinattrlist2[]){
	
	int i, tempi, sign, ans;
	double tempd;
	void* a = rec1 + r1row*recsize1;
	void* b = rec2 + r2row*recsize2;
	IFDEBUG printf("numjoinattrs is %d\n", numjoinattrs); ENDBUG
	for(i=0;i<numjoinattrs;++i){
		int attr1 = joinattrlist1[i], attr2 = joinattrlist2[i];
		IFDEBUG printf("i %d attr1 %d attr2 %d\n", i, attr1, attr2 ); ENDBUG
		int type2 = attributes2[attr2][0];
		int offset2 = attributes2[attr2][2];
		int type1 = attributes1[attr1][0];
		int offset1 = attributes1[attr1][2];

		assert(type2 == type1);

		switch(type1)
		{
			case 1:								//Integer comparision
				IFDEBUG
					printf("JoinCompare type: %d, %d, %d\n",type1, (*(int*)(a+offset1)), (*(int*)(b+offset2)));
				ENDBUG
				tempi = (*(int*)(a+offset1))-(*(int*)(b+offset2));
				if(tempi>0)
					ans = 1;
				else if(tempi<0)
					ans = -1;
				else
					ans = 0;
				break;
				
			case 2:								//Double comparision
				IFDEBUG
					printf("JoinCompare type: %d, %f, %f\n",type1, (*(double*)(a+offset1)), (*(double*)(b+offset2)));
				ENDBUG
				tempd = (*(double*)(a+offset1))-(*(double*)(b+offset2));
				if(tempd>0.0)
					ans = 1;
				else if(tempd<0.0)
					ans = -1;
				else
					ans = 0;
				break;
				
			case 3:								//String comparision
				IFDEBUG
					printf("JoinCompare type: %d, %s, %s\n",type1, (char*)(a+offset1), (char*)(b+offset2));
				ENDBUG
				if(string((char*)(a+offset1))<string((char*)(b+offset2)))
					return -1;
				else if(string((char*)(a+offset1))>string((char*)(b+offset2)))
					return 1;
				else
					return 0;
				break;
		}

		if(ans!=0) break;

	}

	return ans;
}


int equijoin(char* rel1, char* rel2, char* outrel, int numjoinattrs, int attrlist1[], int attrlist2[], int numprojattrs, int projlist[][2])
{
	unsigned noattr1,noattr2;


	char* tmp1 = "tmp1";
	char* tmp2 = "tmp2";
	sort(rel1, "tmp1", numjoinattrs, attrlist1, BUFF_SIZE);
	sort(rel2, "tmp2", numjoinattrs, attrlist2, BUFF_SIZE);	

	IFDEBUG
		display("tmp1");
		display("tmp2");
	ENDBUG

	bool status = true;
	tempfile1 = open_file(rel1, "rb");
	// FILE* fp = open_file(rel1, "rb");
	status = (tempfile1 == NULL);
	tempfile2 = open_file(rel2, "rb");
	status = status && (tempfile2 == NULL);
	if(status){
		printf("error opening files1\n");
		return 1;
	}

	fread(&noattr1,sizeof(unsigned),1,tempfile1);	//no of attributes in rel1
	fread(&noattr2,sizeof(unsigned),1,tempfile2);	//no of attributes in rel2
	int temp = noattr1 + noattr2;
	int projlist2[temp][2];
	fclose(tempfile1);
	fclose(tempfile2);

	if (numprojattrs==0) {
		numprojattrs = temp;
		for(int i=0;i<noattr1;i++){
		  projlist2[i][0] = 1;
		  projlist2[i][1] = i+1;	
	  	}
	  	for(int i=0;i<noattr2;i++){
		  projlist2[noattr1 + i][0] = 2;
  		  projlist2[noattr1 + i][1] = i+1;	
	  	}
	  	projlist = projlist2;

	  	IFDEBUG
	  	for(int i=0; i < numprojattrs; i++)
	  	{
	  		printf("projlist %d: %d, %d\n", i, projlist[i][0], projlist[i][1]);
	  	}
	  	ENDBUG
	}

	IFDEBUG
	for(int i=0; i < numprojattrs; i++)
  		printf("projlist %d: %d, %d\n", i, projlist[i][0], projlist[i][1]);

	ENDBUG

	int bufsize = BUFF_SIZE/3;
	IFDEBUG printf("Buffer size: %d\n",bufsize); ENDBUG

	buffer1 = calloc(bufsize,sizeof(unsigned char));
	buffer2 = calloc(bufsize,sizeof(unsigned char));
	outbuffer = calloc(bufsize,sizeof(unsigned char));

	status = true;
	tempfile1 = open_file(tmp1, "rb");
	status = (tempfile1 == NULL);
	tempfile2 = open_file(tmp2, "rb");
	status = status && (tempfile2 == NULL);
	if(status){
		printf("error opening files1\n");
		return 1;
	}

	fread(&noattr1,sizeof(unsigned),1,tempfile1);	//no of attributes
	fread(&noattr2,sizeof(unsigned),1,tempfile2);	//no of attributes


	IFDEBUG printf("Number of attributes: %d, %d, %d\n",noattr1, noattr2, numprojattrs); ENDBUG

	//make 2d arrays
	attributes1 = make2dint(1+(signed)noattr1,3);
	attributes2 = make2dint(1+(signed)noattr2,3);
	attributesout = make2dint(1+(signed)numprojattrs,3);

	joinparams1 = (int*)calloc(1+noattr1,sizeof(int));
	joinparams2 = (int*)calloc(1+noattr2,sizeof(int));



	joinparams1[0] = numjoinattrs;
	for(int i=1;i<=noattr1;i++)
		joinparams1[i] = attrlist1[i-1];

	joinparams2[0] = numjoinattrs;
	for(int i=1;i<=noattr2;i++)
		joinparams2[i] = attrlist2[i-1];


	recsize1 = 0;
	for(int i=1;i<=(signed)noattr1;i++){
		int temp;
		fread(attributes1[i],sizeof(int),1,tempfile1);			//attributes[0][*] is not used.
		fread(&temp,sizeof(int),1,tempfile1);
		attributes1[i][1] = temp;
		attributes1[i][2] = recsize1;
		recsize1 += temp;
		IFDEBUG
			printf("1: attr no: %d, type: %d, size: %d, offset: %d\n", i, attributes1[i][0], attributes1[i][1], attributes1[i][2]);
		ENDBUG
	}

	recsize2 = 0;
	for(int i=1;i<=(signed)noattr2;i++){
		int temp;
		fread(&attributes2[i][0],sizeof(int),1,tempfile2);			//attributes[0][*] is not used.
		fread(&temp,sizeof(int),1,tempfile2);
		attributes2[i][1] = temp;
		attributes2[i][2] = recsize2;
		recsize2 += temp;
		IFDEBUG
			printf("2: attr no: %d, type: %d, size: %d, offset: %d\n", i, attributes2[i][0], attributes2[i][1], attributes2[i][2]);
		ENDBUG
	}

	recsizeout=0;
	for(int i=0;i<(signed)numprojattrs;i++){
		int temp, relnum=projlist[i][0];
		
		if(relnum==1){
			temp = attributes1[projlist[i][1]][1];
			attributesout[i][0] = attributes1[projlist[i][1]][0];
		}
			
		else{
			temp = attributes2[projlist[i][1]][1];
			attributesout[i][0] = attributes2[projlist[i][1]][0];
		}
			
		IFDEBUG
			printf("relnum: %d, attr no: %d, type: %d, size: %d\n", relnum, projlist[i][1], attributesout[i][0], temp);
		ENDBUG

		attributesout[i][1] = temp;
		attributesout[i][2] = recsizeout;
		recsizeout += temp;
	}

	maxrec1 = bufsize/recsize1;
	maxrec2 = bufsize/recsize2;
	maxrecout = bufsize/recsizeout;

	IFDEBUG 
		printf("record size: %d, %d, %d\n=====================\n", recsize1, recsize2, recsizeout);
		printf("max number of records: %d, %d, %d\n",maxrec1, maxrec2, maxrecout); 
	ENDBUG

	{
		out = open_file(outrel, "wb");
		if(out == NULL){
			printf("error opening files2\n");
			return 1;
		}
	}


	//////////
	//write the meta data of outrel
	//////////
	{
		IFDEBUG 
			printf("Meta Data:\n");
			printf("Num of arrtibutes: %d\n", numprojattrs); 
		ENDBUG

		fwrite(&numprojattrs,sizeof(numprojattrs),1,out);
		int count,type, length;
		for(count=0;count<numprojattrs;count++){
			type = attributesout[count][0];
		 	length = attributesout[count][1];
		 	fwrite(&type,sizeof(int),1,out);
		 	fwrite(&length,sizeof(int),1,out);
			IFDEBUG printf("Type & Length: %d, %d\n",type, length); ENDBUG
		}
	}

	
	int readstatus1, readstatus2, writestatus, recread1=0,recread2=0, recwrite=0;
	int writeoffset=0, readoffset1=0, readoffset2=0, comparestatus;
	readstatus1 = fread(buffer1,recsize1,maxrec1,tempfile1);
	readstatus2 = fread(buffer2,recsize2,maxrec2,tempfile2);
	bool valid_buffer = false;

	int loop_count=0;
	int same_count=0;

	int i,j;

	do{
		loop_count++;
		IFDEBUG printf("loop_count is: %d\nreadstatus1: %d, readstatus2: %d\n", loop_count, readstatus1, readstatus2); ENDBUG
		if(readstatus1==0 || readstatus2==0)
			break;

		
		for(i=0,j=0;i<readstatus1 && j<readstatus2;){
			IFDEBUG printf("forloop: i %d, j %d\n", i, j); ENDBUG
			comparestatus = joincompare(buffer1,buffer2,i,j,numjoinattrs,attrlist1,attrlist2);
			IFDEBUG printf("comparestatus : %d\n", comparestatus); ENDBUG
			switch(comparestatus){
				case 1:
					IFDEBUG printf("case 1:\n"); ENDBUG
					assert(same_count == 0);
					//recread2++;
					j++;
					break;

				case -1:
				IFDEBUG printf("case -1:\n"); ENDBUG
					if (same_count!=0)
					{
						IFDEBUG 
						printf("Same Count is: %d\n", same_count);
						ENDBUG
						comparestatus = joincompare(buffer1, buffer2, i+1, j-1,numjoinattrs,attrlist1,attrlist2);
						if (comparestatus == 0) {
							// next element of 1st buffer also matches
							j = j - same_count;
							//recread2 = recread2 - same_count;
							same_count = 0;
						} else {
							// not same.
							same_count = 0;
						}
					}
					//recread1++;
					i++;
					break;

				case 0:
				IFDEBUG printf("case 0:\n"); ENDBUG
					valid_buffer = true;
					int k;

					for(k=0;k<numprojattrs;++k){
						int size;
						if(projlist[k][0] == 1){
							size = attributes1[projlist[k][1]][1];
							//readoffset1=recsize1*recread1;
							readoffset1=recsize1*i;
							memcpy(outbuffer+writeoffset, buffer1 + readoffset1 + attributes1[projlist[k][1]][2], size);
						}
						else{
							size = attributes2[projlist[k][1]][1];
							//readoffset2=recsize2*recread2;
							readoffset2=recsize2*recread2;
							memcpy(outbuffer+writeoffset, buffer2 + readoffset2 + attributes2[projlist[k][1]][2], size);
						}

						// IFDEBUG
						// 	if(projlist[k][0] == 1){
						// 		printf("K: %d,Relation Number: %d,Attribute NUmber: %d, ",k,projlist[k][0],projlist[k][1]);
						// 		printf("readoffset1: %d, writeoffset:%d, attr size: %d\n",readoffset1,writeoffset,size);
						// 	}
						// 	else{
						// 		printf("K: %d,Relation Number: %d,Attribute NUmber: %d ",k,projlist[k][0],projlist[k][1]);
						// 		printf("readoffset2: %d, writeoffset:%d, attr size: %d\n",readoffset2,writeoffset,size);
						// 	}
						// ENDBUG

						writeoffset += size;

					}
					//recread2++;
					recwrite++;
					j++;
					same_count++;
					break;
				}


				//IFDEBUG printf("[*]recread1 %d recread2 %d, recwrite %d, same_count %d \n",recread1, recread2, recwrite, same_count ); ENDBUG
				IFDEBUG printf("[*]records read 1 %d records read 2 %d, recwrite %d, same_count %d \n",i, j, recwrite, same_count ); ENDBUG
				if(recwrite >= maxrecout){
					IFDEBUG printf("Writing out from writebuffer, recwrite: %d, i: %d, j %d\n", recwrite, i, j); ENDBUG
					valid_buffer = false;
					writestatus = fwrite(outbuffer, recsizeout, recwrite, out);
					IFDEBUG printf("status3: %d, %d, %d\n", readstatus1, readstatus2, writestatus);ENDBUG
					recwrite = 0;
					writeoffset = 0;
				}
				//if(recread1 >= maxrec1){
				if(i >= maxrec1){
					IFDEBUG printf("Reading in buffer1: records read 1 %d\n",i ); ENDBUG
					//recread1 = 0;
					i = 0;
					readstatus1 = fread(buffer1,recsize1,maxrec1,tempfile1);
					// readstatus1 = fread(buffer1,recsize1,maxrec1,tempfile1);
					IFDEBUG printf("status1: %d,%d, %d\n", readstatus1, readstatus2, writestatus);ENDBUG
					
				}
				//if(recread2 >= maxrec2){
				if(j >= maxrec2){
					IFDEBUG printf("records read 2: %d > maxrec2: %d\n", j, maxrec2); ENDBUG
					if (same_count!=0)
					{
						// Houston, there is a problem
						// we might have overflown buffer
						// fseek first to the oldest same element
						// and then read the buffer
						fseek(tempfile2, -same_count*recsize2, SEEK_CUR);
						readstatus2 = fread(buffer2,recsize2,maxrec2,tempfile2);
						IFDEBUG printf("Buffer overflown with same_count=%d; status2: %d,%d, %d\n", same_count,readstatus1, readstatus2, writestatus);ENDBUG
						//recread2 = same_count;
						j = same_count;
					} else {
						readstatus2 = fread(buffer2,recsize2,maxrec2,tempfile2);
						IFDEBUG printf("status2: %d,%d, %d\n", readstatus1, readstatus2, writestatus);ENDBUG
						//recread2 = 0;
						j = 0;	
					}
					
				}
				IFDEBUG printf("At the end of for loop, i %d j %d readstatus1 %d readstatus2 %d\n", i, j, readstatus1, readstatus2); ENDBUG
			}
			if(j >= readstatus2 && (i+1) >= readstatus1 && i<maxrec1 && j<= maxrec2){
					//printf("%s\n", );
					break;
			}

	}
	while((readstatus1!=0) && (readstatus2!=0));


	
	// IFDEBUG
	// 	printf("records read 1, records read 2, recwrite: %d, %d, %d\n",i, j, recwrite);
	// 	printf("read/write status: %d,%d, %d\n", readstatus1, readstatus2, writestatus);
	// 	printf("Loop count: %d\n", loop_count);
	// ENDBUG

	if(valid_buffer){
		IFDEBUG printf("************************fart**********%d\n", recwrite); ENDBUG
		writestatus = fwrite(outbuffer, recsizeout, recwrite, out);
		IFDEBUG printf("Write the last buffer: %d\n", writestatus); ENDBUG
	}

	//close the open file descriptors
	fclose(tempfile1);
	fclose(tempfile2);
	fclose(out);

	printf("\n========================\nSucess!! %s and %s are joined.\nResult is in %s\n========================\n",rel1, rel2, outrel);

	system("rm tmp1 tmp2");
	return 0;
}
