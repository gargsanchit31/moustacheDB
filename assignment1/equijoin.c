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
	for(i=0;i<numjoinattrs;++i){
		int attr1 = joinattrlist1[i], attr2 = joinattrlist2[i];
		int type1 = attributes1[attr1][0], offset1 = attributes1[attr1][2];
		int type2 = attributes2[attr2][0], offset2 = attributes2[attr2][2];

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

	printf("\n Implementing this function \n");

	unsigned noattr1,noattr2;


	char* tmp1 = "tmp1";
	char* tmp2 = "tmp2";
	sort(rel1, "tmp1", numjoinattrs, attrlist1, BUFF_SIZE);
	sort(rel2, "tmp2", numjoinattrs, attrlist2, BUFF_SIZE);	


	display("tmp1");
	display("tmp2");

	int bufsize = BUFF_SIZE/3;
	IFDEBUG printf("Buffer size: %d\n",bufsize); ENDBUG

	buffer1 = calloc(bufsize,sizeof(unsigned char));
	buffer2 = calloc(bufsize,sizeof(unsigned char));
	outbuffer = calloc(bufsize,sizeof(unsigned char));
	
	{
		bool status = true;
		tempfile1 = open_file(tmp1, "rb");
		status = (tempfile1 == NULL);
		tempfile2 = open_file(tmp2, "rb");
		status = status && (tempfile2 == NULL);
		if(status){
			printf("error opening files1\n");
			return 1;
		}
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
	}

	recsize2 = 0;
	for(int i=1;i<=(signed)noattr2;i++){
		int temp;
		fread(attributes2[i],sizeof(int),1,tempfile2);			//attributes[0][*] is not used.
		fread(&temp,sizeof(int),1,tempfile2);
		attributes2[i][1] = temp;
		attributes2[i][2] = recsize2;
		recsize2 += temp;
	}

	recsizeout=0;
	for(int i=0;i<(signed)numprojattrs;i++){
		int temp, relnum=projlist[i][0];
		
		if(relnum==1)
			temp = attributes1[projlist[i][1]][1];
		else
			temp = attributes2[projlist[i][1]][1];

		attributesout[i][0] = attributes1[projlist[i][1]][0];
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

	do{
		loop_count++;
		if(readstatus1==0 && readstatus2==0)
			break;

		int i,j;
		for(i=0,j=0;i<readstatus1 && j<readstatus2;){
			comparestatus = joincompare(buffer1,buffer2,i,j,numjoinattrs,attrlist1,attrlist2);

			switch(comparestatus){
				case 1:
					IFDEBUG printf("case 1:\n"); ENDBUG
					assert(same_count == 0);
					recread2++;j++;
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
							recread2 = recread2 - same_count;
							same_count = 0;
						} else {
							// not same.
							same_count = 0;
						}
					}
					recread1++;
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
							readoffset1=recsize1*recread1;
							memcpy(outbuffer+writeoffset, buffer1 + readoffset1 + attributes1[projlist[k][1]][2], size);
						}
						else{
							size = attributes2[projlist[k][1]][1];
							readoffset2=recsize2*recread2;
							memcpy(outbuffer+writeoffset, buffer2 + readoffset2 + attributes2[projlist[k][1]][2], size);
						}

						IFDEBUG
							if(projlist[k][0] == 1){
								printf("K: %d,Relation Number: %d,Attribute NUmber: %d, ",k,projlist[k][0],projlist[k][1]);
								printf("readoffset1: %d, writeoffset:%d, attr size: %d\n",readoffset1,writeoffset,size);
							}
							else{
								printf("K: %d,Relation Number: %d,Attribute NUmber: %d ",k,projlist[k][0],projlist[k][1]);
								printf("readoffset2: %d, writeoffset:%d, attr size: %d\n",readoffset2,writeoffset,size);
							}
						ENDBUG

						writeoffset += size;

					}
					recread2++;
					recwrite++;
					j++;
					same_count++;
					break;

				if(recwrite >= maxrecout){
					valid_buffer = false;
					writestatus = fwrite(outbuffer, recsizeout, recwrite, out);
					IFDEBUG printf("status3: %d,%d, %d\n", readstatus1, readstatus2, writestatus);ENDBUG
					recwrite = 0;
				}
				if(recread1 >= maxrec1){
					readstatus1 = fread(buffer1,recsize1,maxrec1,tempfile1);
					IFDEBUG printf("status1: %d,%d, %d\n", readstatus1, readstatus2, writestatus);ENDBUG
					recread1 = 0;
				}
				if(recread2 >= maxrec2){
					readstatus2 = fread(buffer2,recsize2,maxrec2,tempfile2);
					IFDEBUG printf("status2: %d,%d, %d\n", readstatus1, readstatus2, writestatus);ENDBUG
					recread2 = 0;
				}
			}
			if ((j == readstatus2) && (same_count!=0) && (i < readstatus1))
			{
				IFDEBUG 
				printf("Same Count is: %d\n", same_count);
				ENDBUG
				
				j = j - same_count;
				recread2 = recread2 - same_count;
				i++;
				recread1++;
				same_count = 0;
			}	
		}

	}
	while(!feof(tempfile1) && !feof(tempfile2));


	
	IFDEBUG
		printf("recread1, recread2, recwrite: %d, %d, %d\n",recread1, recread2, recwrite);
		printf("read/write status: %d,%d, %d\n", readstatus1, readstatus2, writestatus);
		printf("Loop count: %d\n", loop_count);
	ENDBUG

	if(valid_buffer){
		writestatus = fwrite(outbuffer, recsizeout, recwrite, out);
		IFDEBUG printf("Write the last buffer: %d\n", writestatus); ENDBUG
	}

	//close the open file descriptors
	fclose(tempfile1);
	fclose(tempfile2);
	fclose(out);

	system("rm tmp1 tmp2");
	return 0;
}
