#include "_csv_reader.hpp"

#include <iostream>
#include <fstream>
#include <string>
#include <stdio.h>

using namespace std;


namespace koalas {




////////////////////////////////////////////////////
// rfc4180



_CsvChunk::_CsvChunk(CHAR* buffer_)
:_buffer(buffer_)
,_last_CHAR(buffer_)
,_current_field(buffer_)
{
    _current_row = new ROW();
}


_CsvChunk::~_CsvChunk() {
    delete _current_row;
    vector<ROW*>::const_iterator row_it;
    for (row_it = _rows.begin(); row_it!=_rows.end(); ++row_it) {
        delete *row_it;
    }
}

void _CsvChunk::new_field() {
    _current_row->push_back(_current_field);
    _current_field = _Field(_last_CHAR, 0);
}

void _CsvChunk::new_row() {
    _rows.push_back(_current_row);
    _current_row = new ROW();
}

void _CsvChunk::push(CHAR c) {
    *_last_CHAR = c;
    _last_CHAR++;
    _current_field.length++;
}

void _CsvChunk::take(CHAR c) {
    _current_field.length++;
}


void _CsvChunk::end() {
    if (_current_row != _current_row) {
        new_row();
    }
}

void _CsvChunk::error(const char* msg) {
    cerr << "Error:" << msg << endl;
}

const _Field* _CsvChunk::get(int i, int j) const {
    const ROW* row = _rows[i];
    if (j < row->size()) {
        return &(*row)[j];
    }
    else {
        return NULL;
    }
}

int _CsvChunk::nb_rows() const {
    return _rows.size();
}

int _CsvChunk::nb_columns() const {
    int nb_cols = 0;
    vector<ROW*>::const_iterator row_it;
    for (row_it = _rows.begin(); row_it != _rows.end(); ++row_it) {
        const int cur_row = (*row_it) -> size();
        nb_cols = cur_row>nb_cols ? cur_row : nb_cols;
    }
    return nb_cols;    
}

_CsvReader::_CsvReader()
:_state(START_FIELD)
,_dialect(_CsvDialect::rfc4180())
{
}


_CsvReader::_CsvReader(const _CsvDialect& dialect_)
:_dialect(dialect_) {}


_CsvChunk* _CsvReader::read_chunk(CHAR* data, const int remaining_length) {
    _CsvChunk* csv_data = new _CsvChunk(data);
    _CsvChunk& chunk = *csv_data;
    for (int i=0; i<remaining_length; i++) {
        CHAR c = *data;
        if (c== '\0') {
            chunk.end();
            return csv_data;
        }
        switch (_state) {
            case START_FIELD:
                if (c == _dialect.quote()) {
                    _state = QUOTED;
                }
                else if (c == _dialect.separator()) {
                    chunk.new_field();
                }
                else if (c == _dialect.carret()) {
                    chunk.new_field();
                    chunk.new_row();
                }
                else {
                    chunk.push(c);
                    _state = UNQUOTED;
                }
                break;
            case QUOTED:
                if (c == '\0') {
                    chunk.error("EOF reach from within quoted field");
                }
                else if (c == _dialect.quote()) {
                    data++;
                    c = *data;
                    if (c == _dialect.quote()) {
                        chunk.push(_dialect.quote());
                    }
                    else if (c == _dialect.separator()) {
                        chunk.new_field();
                        _state = START_FIELD;
                    }
                    else if (c == _dialect.carret()) {
                        chunk.new_field();
                        chunk.new_row();
                        _state = START_FIELD;
                    }
                    else {
                        chunk.error("Forbidden CHAR after \" in quoted field"); // TODO specify CHAR
                    }
                }
                else {
                    chunk.push(c);
                }
                break;
            case UNQUOTED:
                if (c == _dialect.quote()) {
                    // ERROR
                    chunk.error("Quotation mark within unquoted field forbidden");
                }
                else if (c == _dialect.separator()) {
                    chunk.new_field();
                    _state = START_FIELD;
                }
                else if (c == _dialect.carret()) {
                    chunk.new_field();
                    chunk.new_row();
                    _state = START_FIELD;
                }
                else {
                    chunk.take(c);
                }
                break;
        }
        data++;
    }
    return csv_data;
}


}
