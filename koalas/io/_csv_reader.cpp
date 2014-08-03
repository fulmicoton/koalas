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
,_current_token(buffer_) {
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
    _current_row->push_back(_current_token);
    push('\0');
    _current_token = _last_CHAR;
}

void _CsvChunk::new_row() {
    _rows.push_back(_current_row);
    _current_row = new ROW();
}

void _CsvChunk::push(CHAR c) {
    *_last_CHAR = c;
    _last_CHAR++;
}

void _CsvChunk::end() {
    if (_current_row != _current_row) {
        new_row();
    }
}

void _CsvChunk::error(const char* msg) {
    cerr << "Error:" << msg << endl;
}


const CHAR* _CsvChunk::get(int i, int j) const {
    const ROW* row = _rows[i];
    if (j < row->size()) {
        return (*row)[j];
    }
    else {
        return NULL;
    }
}

void _CsvChunk::print() const {
    cout << "Tokenizer"<< endl;
    cout << _rows.size() << " rows" << endl;
    vector<ROW*>::const_iterator row_it;
    int row_id = 0;
    for (row_it = _rows.begin(); row_it!=_rows.end(); ++row_it) {
        const ROW& row = **row_it;
        cout << "ROW:" << row_id << endl;
        vector<CHAR*>::const_iterator field_it = row.begin();
        for (; field_it != row.end(); field_it++) {
            cout << "   " << *field_it << endl;
        }
        row_id +=1;
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
    _CsvChunk& state = *csv_data;
    for (int i=0; i<remaining_length; i++) {
        CHAR c = *data;
        if (c== '\0') {
            state.end();
            return csv_data;
        }
        switch (_state) {
            case START_FIELD:
                if (c == _dialect.quote()) {
                    _state = QUOTED;
                }
                else if (c == _dialect.separator()) {
                    state.new_field();
                }
                else if (c == _dialect.carret()) {
                    state.new_field();
                    state.new_row();
                }
                else {
                    state.push(c);
                    _state = UNQUOTED;
                }
                break;
            case QUOTED:
                state.error("not suporting quote yet"); // TODO
                return csv_data;

            case UNQUOTED:
                if (c == _dialect.quote()) {
                    // ERROR
                    state.error("Quotation mark within unquoted field forbidden");
                }
                else if (c == _dialect.separator()) {
                    state.new_field();
                    _state = START_FIELD;
                }
                else if (c == _dialect.carret()) {
                    state.new_field();
                    state.new_row();
                    _state = START_FIELD;
                }
                else {
                    state.push(c);
                }
                break;
        }
        data++;
    }
    return csv_data;
}


// void _CsvReader::tokenize_start_field(_CsvChunk& state, const CHAR* data, const int remaining_length) const {
//     CHAR c = *data;
//     if (c == '\0') {
//         state.end();
//     }
//     else if (c == _dialect.quote()) {
//         tokenize_quoted(state, ++data, remaining_length-1);
//     }
//     else if (c == _dialect.separator()) {
//         state.new_field();
//         tokenize_start_field(state, ++data, remaining_length-1);
//     }
//     else if (c == _dialect.carret()) {
//         state.new_field();
//         state.new_row();
//         tokenize_start_field(state, ++data, remaining_length-1);
//     }
//     else {
//         state.push(c);
//         tokenize_unquoted(state, ++data, remaining_length-1);
//     }
// }


// void _CsvReader::tokenize_quoted(_CsvChunk& state, const CHAR* data, int remaining_length) const {
//     CHAR c = *data;
//     if (c == '\0') {
//         state.error("EOF reach from within quoted field");
//     }
//     else if (c == _dialect.quote()) {
//         data++;
//         c = *data;
//         if (c == _dialect.quote()) {
//             state.push(_dialect.quote());
//             tokenize_quoted(state, ++data, remaining_length-1);
//         }
//         else if (c == _dialect.separator()) {
//             state.new_field();
//             tokenize_start_field(state, ++data, remaining_length-1);
//         }
//         else if (c == _dialect.carret()) {
//             state.new_field();
//             state.new_row();
//             tokenize_start_field(state, ++data, remaining_length-1);
//         }
//         else {
//             state.error("Forbidden CHAR after \" in quoted field"); // TODO specify CHAR
//         }
//     }
//     else {
//         state.push(c);
//         tokenize_quoted(state, ++data, --remaining_length);
//     }
// }




}
