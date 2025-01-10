#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255


// InputBuffer is a wrapper around state needed to interact with getline()
typedef struct {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;


typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1]; // +1 for null terminator
    char email[COLUMN_EMAIL_SIZE + 1];    
} Row;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;



// pre-processor macro (taking 2 args, name of struct and name of member of that strucute)
// that computes size of attribute in given structure
// runs during compilation
// used for memory alignment or serialization purposes
#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);

// id will be at offset 0, will take up (n)bytes, so username will be the next offset and so on
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;


typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct {
    StatementType type;
    Row row_to_insert;
} Statement;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,    
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID
} PrepareResult;

typedef enum { 
    EXECUTE_SUCCESS, 
    EXECUTE_TABLE_FULL 
} ExecuteResult;

const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
    int file_descriptor;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    Pager *pager;
    uint32_t num_rows;
} Table;


typedef struct {
    Table * table;
    uint32_t row_num;
    bool end_of_table; // pos 1 past the last element
} Cursor;


// Essentially the setter from getline into our InputBuffer struct
InputBuffer *new_input_buffer()
{
    // (InputBuffer *) is casting 
    // Here we are allocating space for the whole InputBuffer, not for the pointer to an input buffer
    InputBuffer *input_buffer = (InputBuffer *)malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;    
    return input_buffer;
}

void close_input_buffer(InputBuffer *input_buffer)
{
    // qq: is freeing enough? should I have set the values to NULL before?
    free(input_buffer->buffer);
    free(input_buffer);    
}


void print_prompt(){
    printf("meowdb > ");
}

void read_input(InputBuffer *input_buffer)
{
    // qq: what are the params of getline? what is stdin? 
    ssize_t bytes_read = 
        getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    if(bytes_read == 0){
        perror("Error Reading Input \n");
        exit(1);
    }

    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0; // qq: is this null termination?
}



void print_row(Row* row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement)
{
    statement->type = STATEMENT_INSERT;

    char *statement_delimiter = " ";

    char *keyword = strtok(input_buffer->buffer, statement_delimiter);
    char *id_string = strtok(NULL, statement_delimiter);
    char *username = strtok(NULL, statement_delimiter);
    char *email = strtok(NULL, statement_delimiter);

    if (id_string == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }

    // maybe use strtol which detects errors?
    int id = atoi(id_string);

    // todo, bug, i can pass strings as IDs and they become 0
    if(id < 0){
        return PREPARE_NEGATIVE_ID;
    }

    if(strlen(username) > COLUMN_USERNAME_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }
    if(strlen(email) > COLUMN_EMAIL_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }

    // qq: ints we can just put, while strings we have to copy, why?
    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}


PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement){

    // strncmp compares only the first n bytes
    if(strncmp(input_buffer->buffer, "insert", 6) == 0){
        return prepare_insert(input_buffer, statement);
    }

    if(strncmp(input_buffer->buffer, "select", 6) == 0){
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;        
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}


void serialize_row(Row *source, void *destination)
{
    // memcpy: copy to, from, how many bytes
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);    
}

void deserialize_row(void *source, Row *destination)
{
    // memcpy: copy to, from, how many bytes
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void *get_page(Pager *pager, uint32_t page_num)
{
    if(page_num > TABLE_MAX_PAGES){
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,TABLE_MAX_PAGES);
        exit(1);
    }

    if(pager->pages[page_num] == NULL){
        // Cache Miss. Allocate memory and load from file
        void *page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        // we might save a partial page at the end of the file
        if(pager->file_length % PAGE_SIZE){
            num_pages += 1;
        }

        if(page_num <= num_pages){
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}    

// where to read/write in memory for particular row
void *row_slot(Table *table, uint32_t row_num)
{
    uint32_t page_num = row_num / ROWS_PER_PAGE;

    void *page = get_page(table->pager, page_num);

    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    // pointer to start of page + the offset in bytes
    return page + byte_offset;
}


ExecuteResult execute_insert(Statement *statement, Table *table)
{
    if(table->num_rows >= TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }

    Row *row_to_insert = &(statement->row_to_insert);

    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    // increment to offset how many rows we have
    table->num_rows += 1;

    return EXECUTE_SUCCESS;
}

// todo add where clause conditional filtering!
ExecuteResult execute_select(Statement *statement, Table *table)
{
    Row row;
    for(uint32_t row_number = 0; row_number < table->num_rows; row_number++)
    {
        deserialize_row(row_slot(table, row_number), &row);
        // todo: maybe not print by actually return into an array?
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}



ExecuteResult execute_statement(Statement *statement, Table *table)
{
    switch(statement->type){
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case (STATEMENT_SELECT):
            return execute_select(statement, table);
    }
}

Pager *pager_open(const char *filename)
{
    int fd = open(filename,
        O_RDWR | O_CREAT,
        0666 
    );

    if(fd == -1){
        printf("Unable To Open File.\n");
        exit(1);
    }

    //lseek = reposition file read/write offset (fd, offset, whence)
    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager *pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;

    for(uint32_t page = 0; page < TABLE_MAX_PAGES; page++){
        pager->pages[page] = NULL;
    }

    return pager;
}


Table *db_open(const char *filename)
{
    Pager *pager = pager_open(filename);
    uint32_t num_rows = pager->file_length / ROW_SIZE;

    Table *table = (Table *)malloc(sizeof(Table));
    table->pager = pager;
    table->num_rows = num_rows;
    return table;
}



void pager_flush(Pager *pager, uint32_t page_num, uint32_t size)
{
    if(pager->pages[page_num] == NULL){
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);        
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);

    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }    
}



void db_close(Table *table)
{
    Pager *pager = table->pager;
    uint32_t num_full_ages = table->num_rows / ROWS_PER_PAGE;

    for(uint32_t page = 0; page < num_full_ages; page++){
        if(pager->pages[page] = NULL){
            continue;
        }
        pager_flush(pager, page, PAGE_SIZE);
        free(pager->pages[page]);
        pager->pages[page] = NULL;
    }

    // Partial Pages to write at the end of a file (will not be needed after B Tree)
    uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if(num_additional_rows > 0){
        uint32_t page_num = num_full_ages;
        if(pager->pages[page_num] != NULL){
            pager_flush(pager, page_num, num_additional_rows * PAGE_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
    }

    int result = close(pager->file_descriptor);
    if(result == -1){
        printf("Error closing db file.\n");
        exit(1);        
    }

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        void *page = pager->pages[i];
        if(page){
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table)
{
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    db_close(table);
    exit(0);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }    
}


int main(int argc, char *argv[])
{

    if(argc < 2){
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);        
    }

    char *filename = argv[1];
    Table *table = db_open(filename);


    InputBuffer *input_buffer = new_input_buffer();

    while(true){
        print_prompt(); // prints meowdb >

        read_input(input_buffer);

        // metacommands (.exit, .tables, etc)
        if(input_buffer->buffer[0] == '.'){
            switch (do_meta_command(input_buffer, table)){
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized metacommand '%s'\n", input_buffer->buffer);                
                    continue;
            }
        }

        Statement statement;
        // todo: errors can be more verbouse IMO
        switch(prepare_statement(input_buffer, &statement)){
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be positive.\n");
                continue;
            case (PREPARE_STRING_TOO_LONG):
                printf("String Too Long.\n");
                continue;                
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;                
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized command '%s'\n", input_buffer->buffer);
                continue;
        }

        switch(execute_statement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table full.\n");
                break;
        }
    }
    return 0;
}