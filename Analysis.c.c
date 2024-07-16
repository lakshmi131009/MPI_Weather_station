#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include<mpi.h>
#include <time.h>
#include<math.h>
#include<float.h>
 

int main( int argc, char *argv[])
{
	int myrank;
	int size;
	float **A;
	float* mat=NULL;
	int M,Y; // no of rows=M,no of columns=Y after removing header and lat long 
	MPI_Init(&argc, &argv);
	MPI_Comm_rank( MPI_COMM_WORLD, &myrank );
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	
if(myrank==0)
{
   FILE *fp;
   fp = fopen(argv[1], "r");   // read file
   if (fp == NULL)   // check if file is NULL or not
   {
      printf("Error while opening the file.\n");
      exit(EXIT_FAILURE);
   }
   char buff[1000];
   int row=0, col=0;
   while(fgets(buff, 1000, fp))
   {
	char arr[1]; 
	row++;
        arr[0] = buff[0];
   }	
   fgets(buff, 1000, fp);
   char *field = strtok(buff, ",");
   while (field != NULL) {
	col++;
        field = strtok(NULL, ",");
   }
   M=row-1;
   int N=col;

   Y=N-2;

  // dynamically create array of pointers of size M
   float **A = (float **)malloc(M * sizeof(float *));        // or int* A[M];
   // dynamically allocate memory of size N for each row
   for (int r = 0; r < M; r++)
   {
   	A[r] = (float *)malloc(Y * sizeof(float));
   }

fclose(fp);

mat=(float*)malloc(sizeof(float)*M*Y); 	



FILE *fpp;
fpp = fopen(argv[1], "r");   // read file
char buf[1000];
int j=0;
for(int i=0;fgets(buf,sizeof(buf),fpp);++i)
{
   	if(i==0)
        	continue;
   	
   	char *v=strtok(buf,",");
      
	v=strtok(NULL,",");   
    v=strtok(NULL,",");
     

	while(v)
        {
        mat[j]=atof(v);


                v=strtok(NULL,",");
                j++;
        }
   }
   


   int b=0;
   for(int r=0;r<M;r++)
   {
	   for(int c=0;c<Y;c++)
	   {
	
		   A[r][c]=mat[b];
	b++;
	   }

   }
   
   
  //storing csv file data in 1D buffer in column major order 
  int k=0;
  for(int r=0;r<Y;r++)
  {
	  for(int c=0;c<M;c++)
	  {
		  mat[k]=A[c][r];

		  k++;
	  }
  }


  fclose(fpp);   // closes the file 
 
   // deallocate memory
   for (int r = 0; r < M; r++)
        free(A[r]);
   free(A);

}

//starting timer
double sTime=MPI_Wtime();


//bcasting no of rows and columns
MPI_Bcast(&M, 1, MPI_INT, 0, MPI_COMM_WORLD);
MPI_Bcast(&Y, 1, MPI_INT, 0, MPI_COMM_WORLD);


 
//int cc=Y/size;
 
 
 
//to calculate send count and displacement
 
int send_count[size];
int disp[size];
int d=0;
int sendd_count[size];
int di[size]; 
int dd=0;
  if(myrank==0)
  {
      int k=Y;
    for(int i=0;i<size;i++)
    {
      if(k>=Y/size)
      {
    send_count[i]=Y/size;
    sendd_count[i]=(Y/size)*M;
    disp[i]=d;
    di[i]=dd;
    dd+=send_count[i];
    d+=send_count[i]*M;
    k=k-Y/size;
      }
      }
       send_count[size-1]= send_count[size-1]+k;
       sendd_count[size-1]= sendd_count[size-1]+k*M;
    
}

// bcasting send_count and displacement
MPI_Bcast(send_count, size, MPI_INT, 0, MPI_COMM_WORLD);
MPI_Bcast(disp, size, MPI_INT, 0, MPI_COMM_WORLD);
MPI_Bcast(sendd_count, size, MPI_INT, 0, MPI_COMM_WORLD);



/*MPI_Datatype vtype;
MPI_Type_vector(M,1,Y,MPI_FLOAT,&vtype);
MPI_Type_commit(&vtype);
*/   
 
 
//recv buff at each rank to hold scatterdata of dimensions rows X columns

float* rbuf=NULL;
rbuf=(float*)malloc(sizeof(float)*M*Y); 	

   
   
 

//buffer to store min of each column	
float* gbuf=NULL;
gbuf=(float*)malloc(sizeof(float)*Y); 	

 
//buffer to store min of each column at a rescpective rank
float* redbuf=NULL;
redbuf=(float*)malloc(sizeof(float)*send_count[myrank]);





//to scatter no of columns/no of ranks columns   

MPI_Scatterv(mat,sendd_count,disp,MPI_FLOAT,rbuf,sendd_count[myrank],MPI_FLOAT,0,MPI_COMM_WORLD);




//to calculating min of each column
int c=0;
int x=0;
int MM=M;
while(c<(sendd_count[myrank]))
{
    float min=FLT_MAX;
    for(int i=c;i<MM;i++)
    {
       
            if(rbuf[i]<min)
            min=rbuf[i];
        }
   
    
    redbuf[x]=min;
    x++;
    c=c+M;
    MM+=M;
}
 
  
  
//gathering at root
MPI_Gatherv (redbuf,send_count[myrank], MPI_FLOAT,gbuf,send_count, di, MPI_FLOAT, 0, MPI_COMM_WORLD);
  
 
 
//finding year wise min and global min 
 if(myrank==0)
 {
     float finmin=FLT_MAX;
     for(int i=0;i<Y;i++)
     {
         printf("%.2f\t",gbuf[i]);
	 if(gbuf[i]<finmin)
         finmin=gbuf[i];
     }
     printf("\n%.2f\n",finmin);
 }
 

 
//ending timer

double eTime=MPI_Wtime();
double time=eTime-sTime;
 
 
double maxt;
MPI_Reduce(&time,&maxt,1,MPI_DOUBLE,MPI_MAX,0,MPI_COMM_WORLD);
 
 

if(myrank==0)
{
     printf("max:%lf",maxt);
      
   // deallocating buffer
   free(mat);
}
 // deallocating buffers
free(gbuf);
free(redbuf);
free(rbuf);
MPI_Finalize();
   return 0;
}
