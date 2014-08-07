#include "_csv_reader.hpp"

#include <iostream>
#include <fstream>
#include <string>
#include <stdio.h>

using namespace std;


namespace koalas {


_CsvChunk::_CsvChunk(CHAR* buffer_)
:last_CHAR(buffer_)
,buffer(buffer_)
,current_field(buffer_)
{
    current_row = new ROW();
}


_CsvChunk::~_CsvChunk() {
    delete current_row;
    vector<ROW*>::const_iterator row_it;
    for (row_it = rows.begin();
         row_it != rows.end();
         ++row_it) {
        delete *row_it;
    }
}

void _CsvChunk::new_field() {
    current_row->push_back(current_field);
    current_field = _Field(last_CHAR, 0);
}

void _CsvChunk::new_row() {
    rows.push_back(current_row);
    current_row = new ROW();
}

void _CsvChunk::push(CHAR c) {
    *last_CHAR = c;
    last_CHAR++;
    current_field.length++;
}

void _CsvChunk::end() {
    // TODO
}

void _CsvChunk::set_error(const string&  error_msg_) {
     error_msg = error_msg_;
}

const _Field* _CsvChunk::get(int i, int j) const {
    const ROW* row = rows[i];
    if (j < row->size()) {
        return &(*row)[j];
    }
    else {
        return NULL;
    }
}

int _CsvChunk::nb_rows() const {
    return rows.size();
}

int _CsvChunk::nb_columns() const {
    int nb_cols = 0;
    vector<ROW*>::const_iterator row_it;
    for (row_it = rows.begin();
         row_it != rows.end();
         ++row_it) {
        const int cur_row = (*row_it) -> size();
        nb_cols = cur_row > nb_cols ? cur_row : nb_cols;
    }
    return nb_cols;    
}

_CsvReader::_CsvReader(const _CsvDialect* dialect_)
:state(START_FIELD)
,dialect(*dialect_) {}



struct Cursor {
    Cursor(CHAR* buffer, int length_)
    :cursor(buffer)
    ,length(length_)
    ,offset(0) {
        // token = *cursor;
    }

    CHAR token;
    CHAR* cursor;
    int length;
    int offset;
  
    bool next() {
        if (offset < length) {
            token = *cursor;
            offset++;
            cursor++;
            return true;
        }
        else {
            return false;
        }
    }
};


static const int CR = '\r';
static const int LF = '\n';

_CsvChunk* _CsvReader::read_chunk(CHAR* data, const int length) {
    if (PyString_CHECK_INTERNED(data)) {
        // WE NEED TO COPY THE BUFFER BEFORE WORKING
    }

    _CsvChunk* chunk = new _CsvChunk(data);
   
    Cursor cursor = Cursor(data, length);
    while (cursor.next()) {
        if (cursor.token == CR) {
            // getting rid of all \r
            // \r alone will not be interpreted as
            // a carriage return
            continue;
        }
        switch (state) {
            case START_FIELD:
                if (cursor.token == dialect.quotechar) {
                    state = QUOTED;
                }
                else if (cursor.token == dialect.delimiter) {
                    chunk->new_field();
                }
                else if (cursor.token == LF) {
                    chunk->new_field();
                    chunk->new_row();
                }
                else {
                    chunk->push(cursor.token);
                    state = UNQUOTED;
                }
                break;
            case QUOTE_IN_QUOTED:
                if (cursor.token == dialect.quotechar) {
                    chunk->push(dialect.quotechar);
                    state = QUOTED;
                }
                else if (cursor.token == dialect.delimiter) {
                    chunk->new_field();
                    state = START_FIELD;
                }
                else if (cursor.token == LF) {
                    chunk->new_field();
                    chunk->new_row();
                    state = START_FIELD;
                }
                else {
                    chunk->set_error("Forbidden CHAR after \" in quoted field"); // TODO specify CHAR
                }
                break;
            case QUOTED:
                if (cursor.token == dialect.quotechar) {
                    state = QUOTE_IN_QUOTED;
                }
                else {
                    chunk->push(cursor.token);
                }
                break;
            case UNQUOTED:
                if (cursor.token == dialect.quotechar) {
                    // ERROR
                    chunk->set_error("Quotation mark within unquoted field forbidden");
                }
                else if (cursor.token == dialect.delimiter) {
                    chunk->new_field();
                    state = START_FIELD;
                }
                else if (cursor.token == LF) {
                    chunk->new_field();
                    chunk->new_row();
                    state = START_FIELD;
                }
                else {
                    chunk->push(cursor.token);
                }
                break;
        }
    }
    return chunk;
}


}
