#pragma once

#include <string>

class string_builder {
    std::string _value;
    const char* _start = nullptr;
public:
    class guard;
public:
    std::string get() && {
        return std::move(_value);
    }
    void reset() {
        _value.clear();
        _start = nullptr;
    }
    friend class guard;
};

class string_builder::guard {
    string_builder& _builder;
    const char* _block_end;
public:
    guard(string_builder& builder, const char* block_start, const char* block_end)
            : _builder(builder), _block_end(block_end) {
        if (!_builder._value.empty()) {
            mark_start(block_start);
        }
    }
    ~guard() {
        if (_builder._start) {
            mark_end(_block_end);
        }
    }
    void mark_start(const char* p) {
        _builder._start = p;
    }
    void mark_end(const char* p) {
        if (_builder._value.empty()) {
            // avoid an allocation in the common case
            _builder._value = std::string(_builder._start, p);
        } else {
            _builder._value += std::string(_builder._start, p);
        }
        _builder._start = nullptr;
    }
};

template <typename ConcreteParser>
class ragel_parser_base {
protected:
    int _fsm_cs;
    std::unique_ptr<int[]> _fsm_stack = nullptr;
    int _fsm_stack_size = 0;
    int _fsm_top;
    int _fsm_act;
    char* _fsm_ts;
    char* _fsm_te;
    string_builder _builder;
protected:
    void init_base() {
        _builder.reset();
    }
    void prepush() {
        if (_fsm_top == _fsm_stack_size) {
            auto old = _fsm_stack_size;
            _fsm_stack_size = std::max(_fsm_stack_size * 2, 16);
            assert(_fsm_stack_size > old);
            std::unique_ptr<int[]> new_stack{new int[_fsm_stack_size]};
            std::copy(_fsm_stack.get(), _fsm_stack.get() + _fsm_top, new_stack.get());
            std::swap(_fsm_stack, new_stack);
        }
    }
    void postpop() {}
    std::string get_str() {
        auto s = std::move(_builder).get();
        return std::move(s);
    }
};