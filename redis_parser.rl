#pragma once

#include "redis_cmd.h"
#include "ragel_parser_base.h"

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate. */
static int __string2ll(const char *s, size_t slen, int64_t *value) {
    const char *p = s;
    size_t plen = 0;
    int negative = 0;
    uint64_t v;

    if (plen == slen)
        return 0;

    /* Special case: first and only digit is 0. */
    if (slen == 1 && p[0] == '0') {
        if (value != NULL) *value = 0;
            return 1;
    }

    if (p[0] == '-') {
        negative = 1;
        p++; plen++;

    /* Abort on only a negative sign. */
    if (plen == slen)
        return 0;
    }

    /* First digit should be 1-9, otherwise the string should just be 0. */
    if (p[0] >= '1' && p[0] <= '9') {
        v = p[0]-'0';
        p++; plen++;
    } else if (p[0] == '0' && slen == 1) {
        *value = 0;
        return 1;
    } else {
        return 0;
    }

    while (plen < slen && p[0] >= '0' && p[0] <= '9') {
        if (v > (ULLONG_MAX / 10)) /* Overflow. */
            return 0;
        v *= 10;

        if (v > (ULLONG_MAX - (p[0]-'0'))) /* Overflow. */
            return 0;
        v += p[0]-'0';

        p++; plen++;
    }

    /* Return if not all bytes were used. */
    if (plen < slen)
        return 0;

    if (negative) {
        if (v > ((uint64_t)(-(LLONG_MIN+1))+1)) /* Overflow. */
            return 0;
        if (value != NULL) *value = -v;
    } else {
        if (v > LLONG_MAX) /* Overflow. */
            return 0;
        if (value != NULL) *value = v;
    }
    return 1;
}

inline int __hex_digit_to_int(char c) {
    switch(c) {
        case '0': return 0;
        case '1': return 1;
        case '2': return 2;
        case '3': return 3;
        case '4': return 4;
        case '5': return 5;
        case '6': return 6;
        case '7': return 7;
        case '8': return 8;
        case '9': return 9;
        case 'a': case 'A': return 10;
        case 'b': case 'B': return 11;
        case 'c': case 'C': return 12;
        case 'd': case 'D': return 13;
        case 'e': case 'E': return 14;
        case 'f': case 'F': return 15;
        default: return 0;
    }
}

%% machine redis_cmd;

%%{

access _fsm_;

action mark {
    g.mark_start(p);
}

action store_argc {
    auto s = str();
    int ok = __string2ll(s.data(), s.size(), &_argc);
    if (!ok || _argc > 1024*1024) {
        _err.emplace("Protocol error: invalid multibulk length");
        fbreak;
    }
}

action mb_bulk_length_error {
    _err.emplace("Protocol error: invalid bulk length");
    fbreak;
}

action store_arg_len {
    auto s = str();
    int ok = __string2ll(s.data(), s.size(), &_arg_len);
    if (!ok || _arg_len > 512*1024*1024) {
        _err.emplace("Protocol error: invalid bulk length");
        fbreak;
    }
}

action test_arg_len {
        _arg_len-- > 0
}

action store_arg {
    auto s = str();
    auto& argv = _cmd->_argv;
    argv.push_back(std::move(s));
}

action bulk_count_clear {
    _bulk_len = 0;
}

action mb_bulk_end_error {
    _err.emplace("Protocol error: invalid bulk end");
    fbreak;
}

action mb_bulk_length_digit_error {
    _err.emplace("Protocol error: bulk length should be digit");
    fbreak;
}

action mb_expect_dollor_error {
    char s[256];
    std::sprintf(s, "Protocol error: expected '$', got '%c'", *p);
    _err.emplace(s);
    fbreak;
}

action mb_mbulk_count {
    if (_bulk_len++ > PROTO_INLINE_MAX_SIZE) {
        _err.emplace("Protocol error: too big mbulk count string");
        fbreak;
    }
}

action mb_bulk_count {
    if (_bulk_len++ > PROTO_INLINE_MAX_SIZE) {
        _err.emplace("Protocol error: too big bulk count string");
        fbreak;
    }
}

action inl_count {
    if (_bulk_len++ > PROTO_INLINE_MAX_SIZE) {
        _err.emplace("Protocol error: too big inline request");
        fbreak;
    }
}

action inl_unbalanced_quotes_error {
    _err.emplace("Protocol error: unbalanced quotes in request");
    fbreak;
}

action unknown_error {
    _err.emplace("Protocol error");
    fbreak;
}

action inl_append_hex {
    auto hex = str();
    _arg.push_back(static_cast<char>(__hex_digit_to_int(hex[0]) * 16 + __hex_digit_to_int(hex[1])));
}

action inl_append_byte {
    _arg.push_back(*p);
}

action inl_store_arg {
    _cmd->_argv.emplace_back(_arg.begin(), _arg.end());
    _arg.clear();
}

action done {
    if (_argc_cnt >= _argc) {
        _cmd->_flow = _cmd->_flow - (pe - p) + 1;
        done = true;
        fbreak;
    }
}

mb_crlf = "\r\n";
mb_arg_count = "*" ('-'? digit+) >mark >bulk_count_clear $mb_mbulk_count %store_argc mb_crlf;
mb_arg_len = "$" @err(mb_expect_dollor_error) (digit+ >mark >bulk_count_clear $mb_bulk_count %store_arg_len mb_crlf) @err(mb_bulk_length_error);

mb_arg = mb_arg_len (any when test_arg_len)* >mark %store_arg <: mb_crlf @{ ++_argc_cnt; };
mb_cmd = mb_arg_count (mb_arg when {_argc > _argc_cnt})*;

inl_crlf = [\r\n];
inl_separator = [\t\0 ];
inl_plain = any - inl_separator - inl_crlf - '"' - "'";
inl_single_quotes = ("'" (("\\'" @inl_append_byte) | ((any - '\\') @inl_append_byte)) "'") $err(inl_unbalanced_quotes_error);
inl_quotes_hex = "\\x" xdigit{2} >mark %inl_append_hex;
inl_quotes_slash = '\\' (any - 'x' - '"') @inl_append_byte;
inl_quotes_plain = (any - '\\' - '"') @inl_append_byte;
inl_quotes = ('"' (inl_quotes_hex | inl_quotes_slash | inl_quotes_plain)+ '"') $err(inl_unbalanced_quotes_error);
inl_quotes_part = (inl_single_quotes | inl_quotes);
inl_arg = ((inl_plain+ $inl_append_byte inl_quotes_part?) | inl_quotes_part) %inl_store_arg <: inl_separator*;
inl_cmd = ((inl_separator* <: inl_arg+ :>> inl_crlf+) - ('*' any*)) >bulk_count_clear $inl_count;
main := inl_crlf* <: (inl_cmd | mb_cmd) @done @err(unknown_error);

}%%

class redis_parser : public ragel_parser_base<redis_parser> {
    %% write data nofinal noprefix;
private:
    enum class state {
        error,
        eof,
        done,
    };
    std::unique_ptr<redis_cmd> _cmd;
    std::vector<char> _arg;
    int _bulk_len = 0;
    int64_t _argc = 0;
    int64_t _argc_cnt = 0;
    int64_t _arg_len = 0;
    state _state = state::eof;
    std::experimental::optional<std::string> _err;
public:
    redis_parser(redis_parser&& p) noexcept :
            _cmd(std::move(p._cmd)),
            _arg(std::move(p._arg)),
            _bulk_len(p._bulk_len),
            _argc(p._argc),
            _argc_cnt(p._argc_cnt),
            _arg_len(p._arg_len),
            _state(p._state),
            _err(std::move(p._err)) {}
    redis_parser& operator=(redis_parser&& c) noexcept {
        if (this != &c) {
            this->~redis_parser();
            new (this) redis_parser(std::move(c));
        }
        return *this;
    }
    redis_parser(const redis_parser& p) noexcept = delete;
    redis_parser& operator=(const redis_parser& p) noexcept = delete;
    redis_parser() = default;

    void init() {
        init_base();
        _cmd = std::make_unique<redis_cmd>();
        _arg.clear();
        _bulk_len = 0;
        _argc = 0;
        _argc_cnt = 0;
        _arg_len = 0;
        _state = state::eof;
        _err = {};
        %% write init;
    }
    char* parse(char* p, char* pe, char* eof) {
        string_builder::guard g(_builder, p, pe);
        auto str = [this, &g, &p] { g.mark_end(p); return get_str(); };
        bool done = false;
        if (p == pe) {
            _state = state::eof;
            return p;
        }
        _cmd->_flow = _cmd->_flow + pe - p;

        auto orig = p;
        %% write exec;
        if (_err) {
            _state = state::error;
            return orig;
        }
        if (!done) {
            p = nullptr;
        } else {
            _state = state::done;
        }
        return p;
    }
    bool eof() const {
        return _state == state::eof;
    }
    const std::experimental::optional<std::string>& err() const {
        return _err;
    }
    std::unique_ptr<redis_cmd> get_cmd() {
        if (_state == state::done) {
            return std::move(_cmd);
        }
        return std::unique_ptr<redis_cmd>();
    }
};
