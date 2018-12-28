#pragma once

#include <ostream>
#include <string>
#include <vector>
#include <experimental/optional>

#define PROTO_INLINE_MAX_SIZE   (1024*64) /* Max size of inline reads */

class redis_cmd {
private:
    std::vector<std::string> _argv;
    size_t _flow = 0;

    template<typename char_type, typename char_traits>
    friend
    std::basic_ostream<char_type, char_traits> &
    operator<<(std::basic_ostream<char_type, char_traits> &os, const redis_cmd &cmd);

    friend class redis_parser;
public:
    const std::vector<std::string>& argv() const {
        return _argv;
    }

    const std::string& argv(unsigned i) const {
        return _argv[i];
    }

    size_t flow() const {
        return _flow;
    }
};

template<typename char_type, typename char_traits>
inline std::basic_ostream<char_type, char_traits> &
operator<<(std::basic_ostream<char_type, char_traits> &os, const redis_cmd &cmd) {
    bool first = true;
    os << "{";
    for (auto &&elem : cmd._argv) {
        if (!first) {
            os << ", ";
        } else {
            first = false;
        }
        os << elem;
    }
    os << "}";
    os << ", flow[" << cmd._flow << "]";
    return os;
}