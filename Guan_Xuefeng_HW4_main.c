#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <ctype.h>

/*
Define an array of mutex for finer grained locking for better
runtime.
*/
#define NUM_MUTEXES 16
pthread_mutex_t mutexes[NUM_MUTEXES];

/*Create struct for HashItems, which includes the word name,
word length and occupation status.
*/
#define TABLE_SIZE 20480
typedef struct
{
    char word[30];
    int count;
    int occupied;
} HashItem;

// Create a table to hold individual HashItems.
typedef struct
{
    HashItem items[TABLE_SIZE];
} HashTable;

/*C
reate a struct that includes the necessary information to
pass to each threads.
*/
typedef struct
{
    char *filename;
    long int offset;
    long int charsPerThread;
    int threadIndex;
    int numOfThreads;
    HashTable *table;
} ThreadData;

// Here goes all the headers
unsigned long int hashNum(char *string);
HashTable *hashInsert(char *word, HashTable *table);
void hashCreate(HashTable *table);
void hashFindTopTen(HashTable *bigTable, int threadNum);
void *counter(void *ptr);
void tableMerge(HashTable *table, HashTable *bigTable);

// Globally define delim and a big table to store the result
char *delim = "\"\'.“”‘’?:;-,—*($%)! \t\n\x0A\r";
HashTable bigTable;

int main(int argc, char *argv[])
{
    //**************************************************************
    // DO NOT CHANGE THIS BLOCK
    // Time stamp start
    struct timespec startTime;
    struct timespec endTime;

    clock_gettime(CLOCK_REALTIME, &startTime);
    //**************************************************************

    // Open the file in read only mode
    int numOfThreads = atoi(argv[2]);
    int file;
    file = open(argv[1], O_RDONLY);

    // Make sure to check if open file successful or failed
    if (file == -1)
    {
        fprintf(stderr, "Failed to open file");
        exit(EXIT_FAILURE);
    }

    // Get the size of the file(how many byte of characters)
    long int sizeOfFile;
    sizeOfFile = lseek(file, 0L, SEEK_END);
    if (sizeOfFile == -1)
    {
        fprintf(stderr, "Failed to direct to the end of the file");
        exit(EXIT_FAILURE);
    }
    if (lseek(file, 0L, SEEK_SET) == -1)
    {
        fprintf(stderr, "Failed to direct to the start of the file");
        exit(EXIT_FAILURE);
    }

    /*
    Separate the content from the txt file evenly based on the number of
    threads created.
    */
    long double tempChar = (double)sizeOfFile / numOfThreads;
    long int charsPerThread;

    /*
    Create multiple threads based on the number of threads
    specified in the command line argument.
    */
    pthread_t threads[numOfThreads];
    ThreadData threadData[numOfThreads];

    // Instantiate the table to make the occupation status to unoccupied
    hashCreate(&bigTable);

    if (tempChar != (long int)tempChar)
    {
        charsPerThread = tempChar + 1;
    }
    else
    {
        charsPerThread = tempChar;
    }

    // Instantiate mutexes base on the number of threads
    for (int i = 0; i < NUM_MUTEXES; i++)
    {
        pthread_mutex_init(&mutexes[i], NULL);
    }

    // Create threads and fill up the struct that is passing to each threads
    // Make sure to check if thread creation succeeded or failed
    for (int i = 0; i < numOfThreads; i++)
    {
        threadData[i].filename = argv[1];
        threadData[i].offset = i * charsPerThread;
        threadData[i].threadIndex = i;
        threadData[i].charsPerThread = charsPerThread;
        threadData[i].numOfThreads = numOfThreads;
        threadData[i].table = malloc(sizeof(HashTable));
        if (threadData[i].table == NULL)
        {
            fprintf(stderr, "Failed to allocate memory for threadData[%d].table\n", i);
            exit(EXIT_FAILURE); 
        }
        hashCreate(threadData[i].table);
        int ret = pthread_create(&threads[i], NULL, counter, &threadData[i]);
        if (ret != 0)
        {
            fprintf(stderr, "Error creating thread %d: %s\n", i, strerror(ret));
            exit(EXIT_FAILURE);
        }
    }

    // Wait for all threads to finish
    // Make sure to print out any errors encountered if there are any
    for (int i = 0; i < numOfThreads; i++)
    {
        int ret = pthread_join(threads[i], NULL);
        if (ret != 0)
        {
            fprintf(stderr, "Error joining thread %d: %s\n", i, strerror(ret));
        }
    }

    // Sort the data and print out the result
    hashFindTopTen(&bigTable, numOfThreads);

    //**************************************************************
    // DO NOT CHANGE THIS BLOCK
    // Clock output
    clock_gettime(CLOCK_REALTIME, &endTime);
    time_t sec = endTime.tv_sec - startTime.tv_sec;
    long n_sec = endTime.tv_nsec - startTime.tv_nsec;
    if (endTime.tv_nsec < startTime.tv_nsec)
    {
        --sec;
        n_sec = n_sec + 1000000000L;
    }

    printf("Total Time was %ld.%09ld seconds\n", sec, n_sec);
    //**************************************************************

    // Clean up
    for (int i = 0; i < NUM_MUTEXES; i++)
    {
        pthread_mutex_destroy(&mutexes[i]);
    }
    for (int i = 0; i < numOfThreads; i++)
    {
        free(threadData[i].table);
    }

    return 0;
}

/*
    Create a simple hash table algorithm

1. This hashtable contains insert/update
2. This hashtable doesn't need delete function
3. There also is a function to find top 10 frequently
   appearing words in the hashtable
*/
/*
A hashing algorithm that would try its best to spread out items
stored in the hashtable. This takes the name of the item, then
returns the value after the "hashing" algorithm
*/
unsigned long int hashNum(char *string)
{
    unsigned long int hashNumber = 0;
    int strLen = strlen(string);
    for (int i = 0; i < strLen; i++)
    {
        hashNumber = (hashNumber << 5) + string[i];
    }
    return hashNumber % TABLE_SIZE;
}

/*
Insert the value into the hashtable, this function takes two
parameters, one is the word, and one is to load the table.

This function will first have the word get its hash number.
Then store the word at the hash number index.

If there's a collision, then find the first available spot
to the right, I used linear probing.
*/
HashTable *hashInsert(char *word, HashTable *table)
{
    unsigned long int hashNumber = hashNum(word);
    while (table->items[hashNumber].occupied == 1)
    {
        /*
        I'm not using strcasecmp because the 
        requirement doesn't specify that
        */
        if (strcmp(table->items[hashNumber].word, word) == 0)
        {
            table->items[hashNumber].count++;
            return table;
        }
        hashNumber = (hashNumber + 1) % TABLE_SIZE;
    }
    table->items[hashNumber].count = 1;
    strcpy(table->items[hashNumber].word, word);
    table->items[hashNumber].occupied = 1;
    return table;
}

/*
Merge two hashtables into one hashtable.

This function will add the values from the local table for each
thread to the result table.
*/
void tableMerge(HashTable *table, HashTable *bigTable)
{
    for (int i = 0; i < TABLE_SIZE; i++)
    {
        if (table->items[i].occupied == 1)
        {
            unsigned long int hashNumber = hashNum(table->items[i].word);
            if (bigTable->items[hashNumber].occupied == 0)
            {
                strcpy(bigTable->items[hashNumber].word, table->items[i].word);
                bigTable->items[hashNumber].count = table->items[i].count;
                bigTable->items[hashNumber].occupied = 1;
            }
            else
            {
                while (bigTable->items[hashNumber].occupied == 1)
                {
                    if (strcmp(bigTable->items[hashNumber].word, table->items[i].word) == 0)
                    {
                        bigTable->items[hashNumber].count += table->items[i].count;
                        break;
                    }
                    hashNumber = (hashNumber + 1) % TABLE_SIZE;
                }
            }
        }
    }
}

/*
Make a hashCreate function to initialize all the occupied statuses
to an unoccupied state
*/
void hashCreate(HashTable *table)
{
    for (int i = 0; i < TABLE_SIZE; i++)
    {
        table->items[i].occupied = 0;
    }
}

typedef struct
{
    char word[30];
    int count;
} WordCount;

// Compare function for Qsort. Will return the difference of two word counts
int compare(const void *a, const void *b)
{
    const WordCount *wordA = (WordCount *)a;
    const WordCount *wordB = (WordCount *)b;
    return wordB->count - wordA->count;
}

/*
Finds the top 10 frequently appeared words in the hashtable.

Copy the content into a local array but don't copy empty or null ptrs
Sort the array and print the first 10 results out
*/
void hashFindTopTen(HashTable *bigTable, int threadNum)
{
    WordCount wordCounts[TABLE_SIZE];
    int count = 0;

    for (int i = 0; i < TABLE_SIZE; i++)
    {
        if (bigTable->items[i].occupied)
        {
            strcpy(wordCounts[count].word, bigTable->items[i].word);
            wordCounts[count].count = bigTable->items[i].count;
            count++;
        }
    }

    // Sort the array, in this case quick sort was used
    qsort(wordCounts, count, sizeof(WordCount), compare);

    // Print sorted array. The top 10 most frequent words
    printf("\n\n");
    printf("Word Frequency Count on WarAndPeace.txt with %d threads\n",threadNum);
    printf("Printing top 10 words 6 characters or more.\n");
    for (int i = 0; i < 10 && i < count; i++)
    {
        printf("%s: %d\n", wordCounts[i].word, wordCounts[i].count);
    }
}

/*
Thread starts here, and ends when this function ends.

Tokenize the given string first, then pass the tokens to insert
them into the hashtable.

When tokenizing make sure to use strtok_r for thread safe.
*/
void *counter(void *ptr)
{
    ThreadData *data = (ThreadData *)ptr;
    char *filename = data->filename;
    long int offset = data->offset;
    long int charsPerThread = data->charsPerThread;
    int threadNum = data->threadIndex;
    int totalThreads = data->numOfThreads;

    // Create local hash tables for each thread
    HashTable *table = data->table;

    // Open file again, read in characters based on the value got from struct
    int file = open(filename, O_RDONLY);
    if (file == -1)
    {
        fprintf(stderr, "Failed to open file in thread\n");
        pthread_exit(NULL);
    }

    // Allocate extra space for potential word overlap
    char *buffer = (char *)malloc(sizeof(char) * (charsPerThread + 100));
    if (buffer == NULL)
    {
        fprintf(stderr, "Buffer allocation failed\n");
        close(file);
        pthread_exit(NULL);
    }

    long int charsRead;

    // For all threads except the last one, read extra characters
    if (threadNum < totalThreads - 1)
    {
        charsRead = pread(file, buffer, charsPerThread + 100, offset);
    }
    else
    {
        // Last thread reads until the end of file
        charsRead = pread(file, buffer, charsPerThread, offset);
    }

    // If pread fails, output the error code
    if (charsRead == -1)
    {
        fprintf(stderr, "pread failed in thread\n");
        free(buffer);
        close(file);
        pthread_exit(NULL);
    }
    // Null terminate the buffer, and close the file descriptor
    buffer[charsRead] = '\0';
    close(file);

    char *start = buffer;
    char *end = buffer + charsPerThread;

    // Adjust start for all threads except the first
    if (threadNum > 0)
    {
        while (start < end && !isspace(*start))
            start++;
    }

    /*
    Adjust end for all threads except the last by checking if
    the buffer ends with space or not.
    */
    if (threadNum < totalThreads - 1)
    {
        while (end < buffer + charsRead && !isspace(*end))
            end++;
    }
    else
    {
        end = buffer + charsRead;
    }

    *end = '\0'; // Null terminate the  buffer

    // Tokenized the buffer, and start inserting it to the table
    char *saveptr;
    char *token = strtok_r(start, delim, &saveptr);
    while (token != NULL)
    {
        if (strlen(token) >= 6)
        {
            hashInsert(token, table);
        }
        token = strtok_r(NULL, delim, &saveptr);
    }

    /*
    Merge the local tables, and the merging process is protected
    by mutex to make sure racing condition doesn't happen
    */
    pthread_mutex_lock(&mutexes[threadNum % NUM_MUTEXES]);
    tableMerge(table, &bigTable);
    pthread_mutex_unlock(&mutexes[threadNum % NUM_MUTEXES]);

    // Free the buffer and exit the thread
    free(buffer);
    pthread_exit(NULL);
}
