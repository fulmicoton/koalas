#ifndef CSV_READER
#define CSV_READER

#include <Python.h>
#include <vector>
#include <string>


namespace koalas {


typedef Py_UNICODE pychar;


struct _Field {

    _Field(pychar* s_)
    :s(s_)
    ,length(0) {}

    _Field(pychar* s_, int length_)
    :s(s_)
    ,length(length_) {}

    _Field(const _Field& other)
    :s(other.s)
    ,length(other.length) {}

    pychar* s;
    int length;

};



typedef std::vector<_Field> Row;


// --------------------------
// PEP 0305
//
// http://legacy.python.org/dev/peps/pep-0305/

enum LINE_TERMINATOR {
    LF_TERMINATOR,    // \n     UNIX STYLE
    CRLF_TERMINATOR   // \r\n   WINDOWS STYLE
};



enum Quoting {
    QUOTE_MINIMAL,
    QUOTE_ALL,
    QUOTE_NONNUMERIC,
    QUOTE_NONE
};



struct _CsvDialect {

    _CsvDialect();

    pychar delimiter;
    pychar quotechar;
    pychar escapechar;
    bool doublequote;
    // bool skipinitialspace;
    LINE_TERMINATOR lineterminator;
    Quoting quoting;

};



class _CsvChunk {

public:
    _CsvChunk(const pychar* buffer, int length);
    ~_CsvChunk();
    int nb_rows() const;
    int nb_columns() const;
    const _Field* get(int i, int j) const;

    void append_buffer(pychar* buffer);
    void new_field();
    void new_row();
    void push(pychar c);
    void set_error(const std::string&);
    void end_of_chunk();
    std::string error_msg;

private:
    pychar* last_pychar;
    std::vector<pychar*> buffers;
    pychar* buffer;
    _Field* current_field;
    std::vector<Row*> rows;
    Row* current_row;
};


enum _CsvReaderState {
    START_FIELD,
    START_Row,
    QUOTED,
    UNQUOTED,
    QUOTE_IN_QUOTED
};



class _CsvReader {

public:
    _CsvReader(const _CsvDialect* dialect_);
    ~_CsvReader();
    _CsvChunk* read_chunk(const pychar* buffer, const int buffer_length);
private:
    _CsvReaderState state;
    const _CsvDialect dialect;
    pychar* remaining;
};



}


#endif // CSV_READER
