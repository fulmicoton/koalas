#ifndef CSV_READER
#define CSV_READER

#include <Python.h>
#include <vector>

namespace koalas {


typedef Py_UNICODE CHAR;


class _Field {
public:
    _Field(CHAR* s_)
    :s(s_)
    ,length(0) {}

    _Field(CHAR* s_, int length_)
    :s(s_)
    ,length(length_) {}

    _Field(const _Field& other)
    :s(other.s)
    ,length(other.length) {}

    CHAR* s;
    int length;   
};


typedef std::vector<_Field> ROW;


class _CsvDialect {

public:
    static _CsvDialect rfc4180() {
        return _CsvDialect(';', '\"');
    }
    _CsvDialect(CHAR separator_, CHAR quote_)
    :_separator(separator_)
    ,_quote(quote_) {};
    inline CHAR separator() const {
        return _separator;
    };
    inline CHAR quote() const { 
        return _quote;
    };
    inline CHAR carret() const {
        return '\n';
    };

private:
    CHAR _separator;
    CHAR _quote;
};




class _CsvChunk {

public:
    _CsvChunk(CHAR* buffer);
    ~_CsvChunk();
    int nb_rows() const;
    int nb_columns() const;
    const _Field* get(int i, int j) const;

    void append_buffer(CHAR* buffer);
    void new_field();
    void new_row();
    void push(CHAR c);
    void take(CHAR c);
    void error(const char* msg);
    void end();

private:
    std::vector<CHAR*> _buffers;
    CHAR* _current_buffer;

    CHAR* _buffer;
    CHAR* _last_CHAR;

    _Field _current_field;
    std::vector<ROW*> _rows;
    ROW* _current_row;
};


enum _CsvReaderState {
    START_FIELD,
    QUOTED,
    UNQUOTED
};

class _CsvReader {
public:
    _CsvReader();
    _CsvReader(const _CsvDialect& dialect_);
    _CsvChunk* read_chunk(CHAR* buffer, const int buffer_length);
    
private:
    _CsvReaderState _state;
    const _CsvDialect _dialect;

};

}


#endif // CSV_READER
