#include "_csv_reader.hpp"

#include <iostream>
#include <fstream>
#include <string>
#include <stdio.h>

using namespace std;


namespace koalas {


//////////////////////////////
//  _CsvDialect

_CsvDialect::_CsvDialect()
:delimiter(',')
,quotechar('"')
,escapechar('\\')
,doublequote(true)
,lineterminator(LF_TERMINATOR)
,quoting(QUOTE_MINIMAL) {}




//////////////////////////////
//  _CsvChunk

_CsvChunk::_CsvChunk(const pychar* buffer_, int length)
:last_pychar(NULL)
{
    buffer = new pychar[length];
    // memcpy(buffer, buffer_, length * sizeof(pychar));
    last_pychar = buffer;
    current_row = new Row();
    rows.push_back(current_row);
}


_CsvChunk::~_CsvChunk() {
    delete buffer;
    vector<Row*>::const_iterator row_it;
    for (row_it = rows.begin();
         row_it != rows.end();
         ++row_it) {
        delete *row_it;
    }
}


void _CsvChunk::new_field() {
    current_row->push_back(_Field(last_pychar, 0));
    current_field = &*(current_row->rbegin());
}


void _CsvChunk::new_row() {
    current_row = new Row();
    rows.push_back(current_row);
}


void _CsvChunk::push(pychar c) {
    *last_pychar = c;
    last_pychar++;
    current_field->length++;
}


void _CsvChunk::end_of_chunk() {
    if (current_row->size() == 0) {
        rows.pop_back();
    }
}


void _CsvChunk::set_error(const string&  error_msg_) {
     error_msg = error_msg_;
}


const _Field* _CsvChunk::get(int i, int j) const {
    const Row* row = rows[i];
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
    vector<Row*>::const_iterator row_it;
    for (row_it = rows.begin();
         row_it != rows.end();
         ++row_it) {
        const int cur_row = (*row_it) -> size();
        nb_cols = cur_row > nb_cols ? cur_row : nb_cols;
    }
    return nb_cols;    
}



//////////////////////////////
//  _CsvReader

_CsvReader::_CsvReader(const _CsvDialect* dialect_)
:state(START_Row)
,dialect(*dialect_)
,remaining(NULL) {}


_CsvReader::~_CsvReader() {
    if (remaining != NULL) {
        delete remaining;
        remaining = NULL;
    }
}


struct Cursor {
    Cursor(const pychar* buffer, int length_)
    :cursor(buffer)
    ,length(length_)
    ,offset(0) {
        // token = *cursor;
    }

    pychar token;
    const pychar* cursor;
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

_CsvChunk* _CsvReader::read_chunk(const pychar* data, const int length) {
    _CsvChunk* chunk = new _CsvChunk(data, length);
    size_t parsed_chars = 0;
    Cursor cursor = Cursor(data, length);
    while (cursor.next()) {
        if (cursor.token == CR) {
            // getting rid of all \r
            // \r alone will not be interpreted as
            // a carriage return
            continue;
        }
        switch (state) {
            case START_Row:
                if (cursor.token == dialect.quotechar) {
                    chunk->new_field();
                    state = QUOTED;
                }
                else if (cursor.token == dialect.delimiter) {
                    chunk->new_field();
                    state = START_FIELD;
                }
                else if (cursor.token == LF) {
                    chunk->new_row();
                    parsed_chars = cursor.offset;
                }
                else {
                    chunk->new_field();
                    chunk->push(cursor.token);
                    state = UNQUOTED;
                }
                break;
            case START_FIELD:
                if (cursor.token == dialect.quotechar) {
                    state = QUOTED;
                }
                else if (cursor.token == dialect.delimiter) {
                    chunk->new_field();
                }
                else if (cursor.token == LF) {
                    chunk->new_row();
                    parsed_chars = cursor.offset;
                    chunk->new_field();
                    state = START_Row;
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
                    chunk->new_row();
                    parsed_chars = cursor.offset;
                    chunk->new_field();
                    state = START_Row;
                }
                else {
                    //chunk->push(dialect.quotechar);
                    chunk->push(cursor.token);
                    state = QUOTED;
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
                if (cursor.token == dialect.delimiter) {
                    chunk->new_field();
                    state = START_FIELD;
                }
                else if (cursor.token == dialect.quotechar) {
                    state = QUOTED; //< actually an error but the csv
                                    // module behaves like that.
                }
                else if (cursor.token == LF) {
                    chunk->new_row();
                    parsed_chars = cursor.offset;
                    state = START_Row;
                }
                else {
                    chunk->push(cursor.token);
                }
                break;
        }
    }
    chunk->end_of_chunk();
    remaining = new pychar[length - parsed_chars];

    return chunk;
}




}
