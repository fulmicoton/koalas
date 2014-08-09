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

_CsvChunk::_CsvChunk(size_t length)
:last_pychar(NULL)
{
    buffer = new pychar[length];
    last_pychar = buffer;
    // current_row = new Row();
    // rows.push_back(current_row);
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



void _CsvChunk::trim_last() {
    if (rows.size() > 0) {
        if ((*rows.rbegin())->size() == 0) {
            rows.pop_back();
        }
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
//:state(START_ROW)
:dialect(*dialect_)
,remaining(NULL)
,remaining_length(0) {}


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


static const pychar CR = '\r';
static const pychar LF = '\n';



size_t _CsvReader::_read_chunk(_CsvChunk* chunk,
                               const pychar* buffer,
                               const size_t length,
                               _CsvReaderState& state) {
    size_t parsed_chars = 0;
    Cursor cursor = Cursor(buffer, length);
    while (cursor.next()) {
        if (cursor.token == CR) {
            // getting rid of all \r
            // \r alone will not be interpreted as
            // a carriage return
            continue;
        }
        switch (state) {
            case START_ROW:
                parsed_chars = cursor.offset;
                chunk -> new_row();
                if (cursor.token == dialect.quotechar) {
                    chunk->new_field();
                    state = QUOTED;
                }
                else if (cursor.token == dialect.delimiter) {
                    chunk->new_field();
                    state = START_FIELD;
                }
                else if (cursor.token == LF) {}
                else {
                    chunk->new_field();
                    chunk->push(cursor.token);
                    state = UNQUOTED;
                }
                break;
            case START_FIELD:
                chunk -> new_field();
                if (cursor.token == dialect.quotechar) {
                    state = QUOTED;
                }
                else if (cursor.token == dialect.delimiter) {
                }
                else if (cursor.token == LF) {
                    state = START_ROW;
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
                    state = START_FIELD;
                }
                else if (cursor.token == LF) {
                    state = START_ROW;
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
                    state = START_FIELD;
                }
                else if (cursor.token == dialect.quotechar) {
                    chunk->set_error("Error : quote in unquoted char are forbidden.");
                    return parsed_chars;
                }
                else if (cursor.token == LF) {
                    state = START_ROW;
                }
                else {
                    chunk->push(cursor.token);
                }
                break;
        }
    }
    return parsed_chars;
}


_CsvChunk* _CsvReader::read_chunk(const pychar* data,
                                  const size_t length,
                                  bool last_chunk) {
    _CsvChunk* chunk = new _CsvChunk(length + remaining_length);
    _CsvReaderState state = START_ROW;
    if (remaining_length > 0) {
        _read_chunk(chunk, remaining, remaining_length, state);
    }
    if (!chunk->ok()) {
        return chunk;
    }
    size_t parsed_chars = _read_chunk(chunk, data, length, state);
    if (last_chunk) {
        _read_chunk(chunk, &LF, 1, state);
    }
    else {
        remaining_length = length - parsed_chars;
        remaining = new pychar[length - parsed_chars];
    }
    cout << "Remaining " << remaining_length << endl;
    memcpy(remaining, data + parsed_chars, remaining_length*sizeof(pychar));
    chunk->end_of_chunk();
    return chunk;
}




}
