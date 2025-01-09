#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

// ############# REGION INPUTBUFFER ################


// InputBuffer is a wrapper around state needed to interact with getline()
typedef struct {
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

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

// ############# ENDREGION INPUTBUFFER ################

// ############# REGION REPL ################

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

// ############# ENDREGION REPL ################


// ############# REGION METACOMMAND ################
// metacommands begin with a .

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;


MetaCommandResult do_meta_command(InputBuffer *input_buffer)
{
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }    
}


// ############# ENDREGION METACOMMAND ################


// ############# REGION STATEMENT ################

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];    
} Row;

void print_row(Row* row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

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
    PREPARE_UNRECOGNIZED_STATEMENT
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
    uint32_t num_rows;
    // qq: what is pages? a list? why is it void?
    void *pages[TABLE_MAX_PAGES];
} Table;

PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement){

    // strncmp compares only the first n bytes
    if(strncmp(input_buffer->buffer, "insert", 6) == 0){

        //sscanf scans formatted input according to defined format
        int args_assigned = sscanf(
            input_buffer->buffer,
            "insert %d %s %s",
            &(statement->row_to_insert.id),
            statement->row_to_insert.username,
            statement->row_to_insert.email
        );
        if(args_assigned < 3){
            return PREPARE_SYNTAX_ERROR;
        }
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;        
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


// where to read/write in memory for particular row
void *row_slot(Table *table, uint32_t row_num)
{
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = table->pages[page_num];
    if(page == NULL){
        // allocate memory only when trying to access page
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
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

// ############# ENDREGION STATEMENT ################



// ############# REGION TABLE ################



Table *new_table()
{
    Table *table = (Table *)malloc(sizeof(Table));
    table->num_rows = 0;

    for(uint32_t page = 0; page < TABLE_MAX_PAGES; page++){
        table->pages[page] = NULL;
    }
    return table;
}

void drop_table(Table *table)
{
    for(uint32_t page = 0; page < TABLE_MAX_PAGES; page++){
        free(table->pages[page]);
    }    
    free(table);
}

// ############# ENDREGION TABLE ################



// ############# REGION TEMPLATE ################
// ############# ENDREGION TEMPLATE ################

int main(int argc, char *argv[])
{
    Table *table = new_table();


    InputBuffer *input_buffer = new_input_buffer();

    while(true){
        print_prompt(); // prints meowdb >

        read_input(input_buffer);

        // metacommands (.exit, .tables, etc)
        if(input_buffer->buffer[0] == '.'){
            switch (do_meta_command(input_buffer)){
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized metacommand '%s'\n", input_buffer->buffer);                
                    continue;
            }
        }

        Statement statement;
        switch(prepare_statement(input_buffer, &statement)){
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                // todo: maybe return a better error here?
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