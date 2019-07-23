#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <stddef.h>
#include "tweets.h"

void merge(tweet *a, int l, int m, int r);
void sequential_merge(tweet *a, int l, int r);

int main(int argc, char** argv) {
    int size,rank;
    int tag_chunksize = 10;
	char *sorting_key;
	int n = 16777216;
    int chunksize=16777216;
    int chunksize2 = chunksize/2;
	int n2 = n/2;
	tweet_buffer *tb;
    double t0,t1;

    

    MPI_Init(&argc,&argv); // Need to initialize the MPI execution environment to use MPI statements.
	MPI_Comm_size(MPI_COMM_WORLD, &size); // number of processes
	MPI_Comm_rank(MPI_COMM_WORLD, &rank); // current process rank

    // Need to create a custom MPI datatype for sending and receiving tweets between processes.
	MPI_Datatype mpi_tweet;
	int mpi_tweet_blocklength[4] = {1, 1, 11, 141};
	MPI_Datatype mpi_tweet_types[4] = {MPI_SHORT, MPI_INT, MPI_CHAR, MPI_CHAR}; // file number, number within file, date, tweet message
	MPI_Aint mpi_tweet_disp[4] = {offsetof(tweet, tw_file), offsetof(tweet, tw_number), offsetof(tweet, tw_date), offsetof(tweet, tw_text)};
	MPI_Type_create_struct(4, mpi_tweet_blocklength, mpi_tweet_disp, mpi_tweet_types, &mpi_tweet);
	MPI_Type_commit(&mpi_tweet);


    set_search_key("er");

    char tweetPath[256];
	sprintf(tweetPath, "%d_%d.tweets", n, rank);

	tb = read_tweets(1,chunksize,chunksize,tweetPath);
    printf("Rank[%d] read 16777217_%d.tweets [FINISH]\n",rank,rank);
    sequential_merge(tb->x,0,chunksize-1);
    printf("Rank[%d] squential_merg 16777217_%d.tweets [FINISH]\n",rank,rank);
    


    if (size > 1) {
        int height = log2(size);
            for (int i = (height - 1); i >= 0; i--) {
                unsigned int mpi_dest = rank ^ (1 << i);
                unsigned int right = (rank < mpi_dest) ? 0 : 1;

            if(right){

                printf("RANK %d , d=%d\n",rank,height);
                MPI_Send(tb->x,chunksize2,mpi_tweet,mpi_dest,0,MPI_COMM_WORLD);
                MPI_Send(tb->x+chunksize2,chunksize2,mpi_tweet,mpi_dest,0,MPI_COMM_WORLD);

                MPI_Recv(tb->y, chunksize2, mpi_tweet, mpi_dest, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Recv(tb->y+chunksize2, chunksize2, mpi_tweet, mpi_dest, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);



            } else {
                printf("RANK %d , d=%d\n",rank,height);
                MPI_Recv(tb->y, chunksize2, mpi_tweet, mpi_dest, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Recv(tb->y+chunksize2, chunksize2, mpi_tweet, mpi_dest, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                MPI_Send(tb->x,chunksize2,mpi_tweet,mpi_dest,0,MPI_COMM_WORLD);
                MPI_Send(tb->x+chunksize2,chunksize2,mpi_tweet,mpi_dest,0,MPI_COMM_WORLD);

            }


            merge(tb->y,0,chunksize2-1,chunksize);



                

              

            }
            


    }

    char file[256];
    sprintf(file, "%ld_%d.stweets", chunksize, rank);
	//sprintf(file, "../tweets/%ld_%d.stweets", chunks->length, world_rank);
    write_tweets(tb->y, chunksize, file);
	

	//tb = read_tweets(1, n, n, filename);
	//printf("RANK %d  loaded tweet file 16777216_%d.tweets\n",rank,rank);
	
    

    

    

	
/* 
	if(size > 1){
			int height = log2(size);
			for (int i = (height - 1); i >= 0; i--) {
				int des = rank ^ (1<<i);

			}


	}

*/

    free(tb->x);
    free(tb->y);
    free(tb);
    tb->y=NULL;
    tb->x=NULL;
    tb=NULL;
    MPI_Finalize();
}

void merge(tweet *a, int l, int m, int r)
{

	/* Two temp arrays */
    const int a1 = (m - l + 1);
    const int a2 = (r - m);

	tweet* L = malloc(a1 * sizeof(tweet));
	tweet* R = malloc(a2 * sizeof(tweet));
 	
 	int i,j,x;
 
    /* Copy data from arrays to be merged to temp arrays*/
    for(i = 0; i < a1; i++)
        L[i] = a[l + i];
    for(j = 0; j < a2; j++)
        R[j] = a[m + 1+ j];
 
 
    i = j = 0;
    x = l;
	int k = 0;
    while (j < a1 && i < a2) //  && k < 2)
    {
		//k++;
        if (compare_tweets(&L[j], &R[i]) <= 0)
        {
            a[x] = L[j];
            j++;
        }
        else
        {
            a[x] = R[i];
            i++;
        }
        x++;
    }
 
    /* If remaianing copy elements of left array*/
    while (j < a1)
    {
        a[x] = L[j];
        j++;
        x++;
    }
 
    /* If remaianing copy elements of right array*/
    while (i < a2)
    {
        a[x] = R[i];
        i++;
        x++;
    }

    free(L);
    free(R);
}


void sequential_merge(tweet *a, int l, int r)
{
	if(l-r)
	{
		int m= (l+r)/2;
		sequential_merge(a,l,m);
		sequential_merge(a,m+1,r);
		merge(a,l,m,r);
	}
}