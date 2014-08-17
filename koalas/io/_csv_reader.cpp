#include "_csv_reader.hpp"

#include <iostream>
#include <fstream>
#include <string>
#include <stdio.h>

using namespace std;


namespace koalas {


static int MAX_CHARS_PER_LINE = 100000;

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
//  _Field


void _Field::print() const {
    for (int i=0; i<length; ++i) {
        cout << (char)s[i]; 
    } 
    cout << endl; 
}

bool _Field::to_int(long long* dest) const {
    /* Only accepts (-|+)?\d+ patterns.
       Returns true if valid, or false if invalid.
       When valid, writes the result in the point dest.
     */
    
    if (length == 0) {
        return false;
    }

    int offset=0;
    int sign = 1;
    int res = 0;
    pychar* cur = s;

    if (*cur == '+') {
        cur++; 
        offset++;

    }
    else if (*cur == '-') {
        sign = -1;
        cur++;
        offset++;
    }

    for (; offset < length; ) {
        res *= 10;
        int digit = (*cur) - '0';
        if ((digit < 0) || (digit > 9)) {
            return false;
        }
        res += digit;
        cur++;
        offset++;   
    }
    
    if (sign == 1) {
        *dest = res;
    }
    else {
        *dest = -res;
    }
    
    return true;
}

bool _Field::to_float(double* dest) const {
    /* */
    if (length == 0) {
        return false;
    }

    
    int offset=0;
    int sign = 1;
    int int_part = 0;
    float dec_part = 0.0;
    float res = 0.0;
    float dec = 1.0;

    pychar* cur = s;

    if (*cur == '+') {
        cur++; 
        offset++;

    }
    else if (*cur == '-') {
        sign = -1;
        cur++;
        offset++;
    }
    for (; offset < length;) {
        int_part *= 10;
        int digit = (*cur) - '0';
        if ((digit < 0) || (digit > 9)) {
            if ((*cur) == '.') {
                offset++;
                cur++;
                break;
            }
            else {
                return false;    
            }    
        }
        int_part += digit;
        offset++;
        cur++;
    }
    
    for (; offset < length;) {
        int digit = (*cur) - '0';
        if ((digit < 0) || (digit > 9)) {
            return false;
        }
        dec /= 10.;
        dec_part += dec * digit;
        cur++;
        offset++;
    }
    res = int_part + dec_part;
    if (sign == 1) {
        *dest = res;
    }
    else {
        *dest = -res;
    }
    return true;
}


//////////////////////////////
//  _CsvChunk


_CsvChunk::_CsvChunk(size_t length)
:last_pychar(NULL)  
{
    buffer = new pychar[length];
    last_pychar = buffer;
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



bool _CsvChunk::ok() const {
    return error_msg.size() == 0;
}

void _CsvChunk::new_field() {
    current_row->push_back(_Field(last_pychar, 0));
    current_field = &*(current_row->rbegin());
}


void _CsvChunk::new_row() {
    current_row = new Row();
    rows.push_back(current_row);
}

void _CsvChunk::remove_row(size_t i) {
    rows.erase(rows.begin() + i);
}

void _CsvChunk::push(pychar c) {
    *last_pychar = c;
    last_pychar++;
    current_field->length++;
}


void _CsvChunk::pop_last() {
    rows.pop_back();
}


void _CsvChunk::set_error(const string&  error_msg_) {
     error_msg = error_msg_;
}

bool _CsvChunk::fill_int(size_t col, long long* dest) const {
    vector<Row*>::const_iterator row_it;
    for (row_it=rows.begin(); row_it!=rows.end(); ++row_it) {
        const Row& row = **row_it;
        if (row.size() <= col) {
            return false;
        }
        const _Field& field = row[col];
        if (!field.to_int(dest)) {
            return false;
        }
        dest++;
    }
    return true;
}


bool _CsvChunk::fill_float(size_t col, double* dest) const {
    for (size_t i=0; i<rows.size(); i++) {
        const Row& row = *rows[i];
        if (row.size() <= col) {
            return false; // make it NaN
        }
        const _Field& field = row[col];
        if (!field.to_float(dest)) {
            return false;
        }
        dest++;
    }
    return true;
}


const _Field* _CsvChunk::get(size_t i, size_t j) const {
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


int _CsvChunk::nb_cols() const {
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
:remaining(NULL)
,remaining_length(0)
,dialect(*dialect_)
{
    remaining = new pychar[MAX_CHARS_PER_LINE];
}


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
    ,offset(0) {}

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
                if (cursor.token == dialect.quotechar) {
                    state = QUOTED;
                }
                else if (cursor.token == dialect.delimiter) {
                    chunk->new_field();
                }
                else if (cursor.token == LF) {
                    parsed_chars = cursor.offset;
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
                    chunk -> new_field();
                    state = START_FIELD;
                }
                else if (cursor.token == LF) {
                    parsed_chars = cursor.offset;
                    state = START_ROW;
                }
                else {
                    if (dialect.doublequote) {
                        chunk->push(cursor.token);
                        state = UNQUOTED;
                    }
                    else {
                        chunk->push(cursor.token);
                        state = QUOTED;
                    }
                }
                break;
            case QUOTED:
                if (cursor.token == dialect.quotechar) {
                    if (dialect.doublequote) {
                        state = QUOTE_IN_QUOTED;
                    }
                    else {
                        state = UNQUOTED;   
                    }
                }
                else {
                    chunk->push(cursor.token);
                }
                break;
            case UNQUOTED:
                if (cursor.token == dialect.delimiter) {
                    chunk -> new_field();
                    state = START_FIELD;
                }
                else if (cursor.token == LF) {
                    parsed_chars = cursor.offset;
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
    size_t parsed_chars = 0;
    size_t parsed_chars_remaining = 0;
    size_t parsed_chars_buffer = 0;
    if (remaining_length > 0) {
        parsed_chars_remaining = _read_chunk(chunk, remaining, remaining_length, state);
    }
    if (!chunk->ok()) {
        return chunk;
    }
    parsed_chars_buffer += _read_chunk(chunk, data, length, state);
    if (parsed_chars_buffer > 0) {
        parsed_chars_remaining = remaining_length;
    }
    parsed_chars = parsed_chars_buffer + parsed_chars_remaining;
    if (!last_chunk) {
        // if this is not the last chunk, we want to consider 
        // the last row as incomplete, it is removed from this chunk and 
        // will be retokenized in the next chunk.
        const size_t new_remaining_length = length + remaining_length - parsed_chars;
        if (new_remaining_length > 0) {
            if (new_remaining_length > MAX_CHARS_PER_LINE) {
                chunk->set_error("Line is greater than MAX_CHARS_PER_LINE chars is forbidden.");
                return chunk;
            }
            if (new_remaining_length > length) {
                // the new remaining section includes some chars from
                // the former remaining section
                for (size_t i = 0; i < remaining_length - parsed_chars; ++i) {
                    remaining[i] = remaining[parsed_chars + i];
                }
                memcpy(remaining + remaining_length - parsed_chars, data, length * sizeof(pychar));
            }
            else {
                memcpy(remaining, data + (length - new_remaining_length), new_remaining_length * sizeof(pychar));   
            }
            chunk -> pop_last();
        }
        // we rewind the last incomplete row
        remaining_length = new_remaining_length;
    }
    return chunk;
}




}
