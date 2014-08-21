
template<typename TInt>
bool _CsvChunk::fill_int(size_t col, TInt* dest) const {
    std::vector<Row*>::const_iterator row_it;
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


template<typename TInt>
bool _Field::to_int(TInt* dest) const {
    /* Only accepts (-|+)?\d+ patterns.
       Returns true if valid, or false if invalid.
       When valid, writes the result in the point dest.
     */
    if (length == 0) {
        return false;
    }

    int offset=0;
    int sign = 1;
    TInt res = 0;
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

template<typename TFloat>
bool _CsvChunk::fill_float(size_t col, TFloat* dest) const {
    std::vector<Row*>::const_iterator row_it;
    for (row_it=rows.begin(); row_it!=rows.end(); ++row_it) {
        const Row& row = **row_it;
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


template<typename TFloat>
bool _Field::to_float(TFloat* dest) const {
    /* */
    if (length == 0) {
        return false;
    }

    
    int offset=0;
    int sign = 1;
    int int_part = 0;
    TFloat dec_part = 0.0;
    TFloat res = 0.0;
    TFloat dec = 1.0;

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