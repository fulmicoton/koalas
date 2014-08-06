#ifndef CSV_READER
#define CSV_READER

#include <Python.h>
#include <vector>
#include <string>

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


// --------------------------
// PEP 0305
//
// http://legacy.python.org/dev/peps/pep-0305/

enum LINE_TERMINATOR {
    LF_TERMINATOR,    // \n     UNIX STYLE
    CRLF_TERMINATOR   // \r\n   WINDOWS STYLE
};

enum QUOTING {
    QUOTE_MINIMAL,
    QUOTE_ALL,
    QUOTE_NONNUMERIC,
    QUOTE_NONE
};

struct _CsvDialect {
public:
    _CsvDialect()
    :delimiter(',')
    ,quotechar('"')
    ,escapechar('\\')
    ,doublequote(true)
    ,lineterminator(LF_TERMINATOR)
    ,quoting(QUOTE_MINIMAL) {}

    CHAR delimiter;
    CHAR quotechar;
    CHAR escapechar;
    bool doublequote;
    // bool skipinitialspace;
    LINE_TERMINATOR lineterminator;
    QUOTING quoting;
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
    void set_error(const std::string&);
    void end();
private:
    std::string error_msg;
    CHAR* last_CHAR;
    std::vector<CHAR*> buffers;
    CHAR* buffer;
    _Field current_field;
    std::vector<ROW*> rows;
    ROW* current_row;
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
    _CsvReaderState state;
    const _CsvDialect dialect;
};

}


#endif // CSV_READER
