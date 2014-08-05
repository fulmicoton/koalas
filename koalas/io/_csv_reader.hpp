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
    _CsvDialect()
    :_separator(',')
    ,_quote('"') {}

    void set_quote(CHAR quote) {
        _quote = quote;
    };

    void set_separator(CHAR separator) {
        _separator = separator;
    }

    inline CHAR separator() const {
        return _separator;
    };

    inline CHAR quote() const { 
        return _quote;
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
    void error(const char* msg);
    void end();
    CHAR* _last_CHAR;

private:
    std::vector<CHAR*> _buffers;
    CHAR* _buffer;

    _Field _current_field;
    std::vector<ROW*> _rows;
    ROW* _current_row;
};


enum _CsvReaderState {
    START_FIELD,
    QUOTED,
    UNQUOTED,
    QUOTE_IN_QUOTED
};





class _CsvReader {
public:
    _CsvReader(const _CsvDialect* dialect_);
    _CsvChunk* read_chunk(CHAR* buffer, const int buffer_length);
private:
    _CsvReaderState _state;
    const _CsvDialect _dialect;
};

}


#endif // CSV_READER
