#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void pp(char *string){
    printf("%s\n", string);
}

#define CHUNK_SIZE 65536 // 64KB
#define INITIAL_ROWS_CAPACITY 256 // Initial number of rows in the dataframe
#define INITIAL_COLS_CAPACITY 10  // Initial number of columns per row


// todo why are we pointing to the function?
char **split_line_into_columns(
    const char *line, 
    size_t *num_columns,
    size_t expected_columns
){
    size_t cols_capacity = (expected_columns > 0) ? expected_columns : INITIAL_COLS_CAPACITY;
    char **columns = malloc(cols_capacity * sizeof(char *));
    if(columns == NULL)
    {
        perror("Error Allocating Memory For Columns");
        return NULL;
    }

    size_t col_count = 0;
    // todo, how to make this dynamic / come from configuration
    const char *delimiter = ",";
    // todo why do we need to duplicate? isn't this wasteful?
    char *line_copy = strdup(line); 
    if(line_copy == NULL)
    {
        perror("Error Duplicating Line");
        free(columns);
        return NULL;
    }

    char *token = strtok(line_copy, delimiter);
    while(token != NULL)
    {        
        if(col_count == cols_capacity)
        {
          // rows have more columns than header row
            if(expected_columns > 0)
            {
                fprintf(stderr, "Error: Row has more columns than Expected");
                free(line_copy);
                for(size_t i = 0; i < col_count; i++)
                {
                    free(columns[i]);
                }
                free(columns);
                return NULL;
            }

            // otherwise dynamically double 
            cols_capacity *= 2;
            // we can't just re-assign, we don't know if we have enough space
            char **temp = realloc(columns, cols_capacity * sizeof(char *));
            if(temp == NULL)
            {
                perror("Error reallocating memory for columns");
                free(line_copy);
                for(size_t i = 0; i < col_count; i++){
                    free(columns[i]);
                }
                free(columns);
                return NULL;
            }
            // now that we're sure we have space, re-assign
            columns = temp;
        }

        // copy the column value
        columns[col_count] = strdup(token);
        if(columns[col_count] == NULL)
        {
            perror("Error Duplicating Column Value");
            free(line_copy);
            for(size_t i = 0; i < col_count; i++){
                free(columns[i]);
            }
            free(columns);            
        }

        col_count++;
        // how does this move to next column? explain how tokenizing null gives me the next token?
        token = strtok(NULL, delimiter);
    }

    while(col_count < expected_columns)
    {
        columns[col_count++] = strdup(""); 
    }

    *num_columns = col_count;
    free(line_copy);
    return columns;
}


int main()
{
    //const char *filename = "dummy_file1.csv";
    const char *filename = "data/dummy_long_uniform.csv";    

    FILE *file = fopen(filename, "r");

    if(file == NULL){
        perror("Error Opening File");
        return 1;
    }
    printf("Succesfully Read File %s\n\n", filename);

    // this is the buffer for a chunk (I assume we will replace whats in here #todo confirm)
    char *chunk_buffer = malloc(CHUNK_SIZE + 1); // +1 for null terminator
    if(chunk_buffer == NULL){
        perror("Error Allocating Memory For Initial Chunk Buffer");
        fclose(file);
        return 1;
    }

    // array to store line pointers
    size_t rows_capacity = INITIAL_ROWS_CAPACITY;

    char ***rows = malloc(rows_capacity * sizeof(char *));
    if(rows == NULL){
        perror("Error allocating memory for lines array");
        free(chunk_buffer); // obvi not using this anymore
        fclose(file);
        return 1;
    } 

    size_t row_count = 0;
    size_t column_count = 0;
    char *carryover = NULL; // leftovers from previous chunk


    while(!feof(file)){
        // 1 for size means 'read in 1 byte at a time)
        size_t bytes_read = fread(chunk_buffer, 1, CHUNK_SIZE, file);
        // append a null terminator at tail+1 position of the buffer
        chunk_buffer[bytes_read] = '\0';


        if(carryover)
        {
            size_t carryover_len = strlen(carryover);
            char *temp_buffer = malloc(carryover_len + bytes_read + 1);
            if(temp_buffer == NULL){
                perror("Error allocating memory for temp buffer");
                free(carryover);
                free(chunk_buffer);
                fclose(file);
                return 1;                
            }

            strcpy(temp_buffer, carryover);
            strcat(temp_buffer, chunk_buffer);
            free(carryover);
            carryover = NULL;
            chunk_buffer = temp_buffer;
            bytes_read += carryover_len;
        }
        
        // the pointer to the first memory address of chunk buffer
        char *line_start = chunk_buffer;

        // why no pointer on the ++ ?
        for(char *ptr = chunk_buffer; *ptr; ptr++)
        {
            if (*ptr == '\n'){
                *ptr = '0'; // null terminate current line

                // split the line into columns 
                size_t num_columns = 0;
                char **columns = split_line_into_columns(line_start, &num_columns, column_count);
                if(columns == NULL){
                    //todo why do we use fprintf instead of perror
                    fprintf(stderr, "Error Parsing Row. \n");
                    free(chunk_buffer);
                    fclose(file);
                    return 1;
                }

                // determine the column count only from the first row
                if(row_count == 0){
                    column_count = num_columns;
                } else if (num_columns != column_count){
                    fprintf(stderr, "Error: Row %zu has inconsistent column count. \n", row_count + 1);
                    free(chunk_buffer);
                    fclose(file);
                    return 1;
                }


                // if my current row is the same as my row capacity we need to dynamically reallocate more space
                if(row_count == rows_capacity)
                {
                    // todo double seems intense? Does duckdb double? Do they use a proprietary way of duplicating with minimal sys calls but least memory waste?
                    rows_capacity *= 2;
                    char ***temp = realloc(rows, rows_capacity * sizeof(char *));
                    if(temp == NULL){
                        // we've malloced rows, why are we not freeing it?
                        free(chunk_buffer);
                        fclose(file);
                        return 1;
                    }
                    rows = temp;
                    // todo why are we not freeing temp right here?
                }
                rows[row_count++] = columns;

                line_start = ptr + 1;
            }
        }

        // Save any leftover for the next chunk
        if(line_start < chunk_buffer + bytes_read){
            carryover = strdup(line_start);
        }
    }

    // print our datafram!
    for(size_t row = 0; row < row_count; row++){
        for(size_t col = 0; col < column_count; col++){
            printf("%s ", rows[row][col]);
        }
        printf("\n");
    }

    // Free up mem!
    for(size_t row = 0; row < row_count; row++){
        for(size_t col = 0; col < column_count; col++){
            free(rows[row][col]);
        }
        free(rows[row]);
    }
    free(rows);
    fclose(file);   
    return 0;
}

