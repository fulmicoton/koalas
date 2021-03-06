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

    template<typename TInt> inline bool to_int(TInt*) const;
    template<typename TFloat> inline bool to_float(TFloat*) const;

    void print() const;

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
    _CsvChunk(size_t length);
    ~_CsvChunk();
    int nb_rows() const;
    int nb_cols() const;
    const _Field* get(size_t i, size_t j) const;

    void append_buffer(pychar* buffer);
    void new_field();
    void new_row();
    void push(pychar c);
    bool ok() const;
    void set_error(const std::string&);
    void pop_last();
    std::string error_msg;
    void remove_row(size_t row_id);
    template<typename TInt> bool fill_int(size_t col, TInt* dest) const;
    template<typename TFloat> bool fill_float(size_t col, TFloat* dest) const;

private:
    pychar* last_pychar;
    pychar* buffer;
    _Field* current_field;
    std::vector<Row*> rows;
    Row* current_row;
};


enum _CsvReaderState {
    START_FIELD,
    START_ROW,
    QUOTED,
    UNQUOTED,
    QUOTE_IN_QUOTED
};



class _CsvReader {

public:

    _CsvReader(const _CsvDialect* dialect_);
    ~_CsvReader();
    _CsvChunk* read_chunk(const pychar* buffer,
                          const size_t length,
                          bool last_chunk);
    pychar* remaining;
    size_t remaining_length;
private:
    size_t _read_chunk(_CsvChunk* dest, const pychar* buffer, const size_t length, _CsvReaderState& state);
    const _CsvDialect dialect;

};


#include "_csv_reader_impl.hpp"

}


#endif // CSV_READER
