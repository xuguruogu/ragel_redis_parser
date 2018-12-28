#define BOOST_TEST_MODULE redis_parser

#include <boost/test/included/unit_test.hpp>
#include "redis_parser.hh"

std::unique_ptr<redis_cmd> test_str(redis_parser& parser, std::string s) {
    char* p = const_cast<char*>(s.data());
    char* pe = p + s.size();
    char* eof = s.empty() ? pe : nullptr;
    parser.parse(p, pe, eof);
    auto err = parser.err();
    if (err) {
        std::cout << "err: " << *err << std::endl;
    }
    auto cmd = parser.get_cmd();
    if (cmd) {
        std::cout << *cmd << std::endl;
    }

    return std::move(cmd);
}

BOOST_AUTO_TEST_CASE(test_multi_bulk_normal) {
    redis_parser parser;
    parser.init();
    std::string s = "*3\r\n$3\r\nget\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv(0), "get");
    BOOST_REQUIRE_EQUAL(c.argv(1), "foo");
    BOOST_REQUIRE_EQUAL(c.argv(2), "bar");
    BOOST_REQUIRE_EQUAL(c.flow(), s.size());
}

BOOST_AUTO_TEST_CASE(test_multi_bulk_normal_with_newline) {
    redis_parser parser;
    parser.init();
    std::string s = "\r\n\r\n*3\r\n$3\r\nget\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv(0), "get");
    BOOST_REQUIRE_EQUAL(c.argv(1), "foo");
    BOOST_REQUIRE_EQUAL(c.argv(2), "bar");
    BOOST_REQUIRE_EQUAL(c.flow(), s.size());
}

BOOST_AUTO_TEST_CASE(test_multi_bulk_normal_negative_mbulk_length) {
    redis_parser parser;
    parser.init();
    std::string s = "*-10\r\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv().empty(), true);
    BOOST_REQUIRE_EQUAL(c.flow(), s.size());
}

BOOST_AUTO_TEST_CASE(test_multi_bulk_error_mbulk_length_out_of_range) {
    redis_parser parser;
    parser.init();
    std::string s = "*20000000\r\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), true);
    BOOST_REQUIRE_EQUAL(*parser.err(), "Protocol error: invalid multibulk length");
    BOOST_REQUIRE_EQUAL(bool(cmd), false);
}

BOOST_AUTO_TEST_CASE(test_multi_bulk_error_mbulk_larger) {
    redis_parser parser;
    parser.init();
    std::string s = "*1048578\r\n$3\r\nget\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), true);
    BOOST_REQUIRE_EQUAL(*parser.err(), "Protocol error: invalid multibulk length");
    BOOST_REQUIRE_EQUAL(bool(cmd), false);
}

BOOST_AUTO_TEST_CASE(test_multi_bulk_error_bulk_larger) {
    redis_parser parser;
    parser.init();
    std::string s = "*3\r\n$536870913\r\nget\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), true);
    BOOST_REQUIRE_EQUAL(*parser.err(), "Protocol error: invalid bulk length");
    BOOST_REQUIRE_EQUAL(bool(cmd), false);
}

BOOST_AUTO_TEST_CASE(test_multi_bulk_error_bulk_length_negative) {
    redis_parser parser;
    parser.init();
    std::string s = "*3\r\n$-10\r\nget\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), true);
    BOOST_REQUIRE_EQUAL(*parser.err(), "Protocol error: invalid bulk length");
    BOOST_REQUIRE_EQUAL(bool(cmd), false);
}

BOOST_AUTO_TEST_CASE(test_multi_bulk_error_expect_dollor) {
    redis_parser parser;
    parser.init();
    std::string s = "*3\r\n$3\r\nget\r\n3\r\nfoo\r\n$3\r\nbar\r\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), true);
    BOOST_REQUIRE_EQUAL(*parser.err(), "Protocol error: expected '$', got '3'");
    BOOST_REQUIRE_EQUAL(bool(cmd), false);
}

BOOST_AUTO_TEST_CASE(test_multi_bulk_error_unknown) {
    redis_parser parser;
    parser.init();
    std::string s = "*3\r\n$a\r\nget\r\n3\r\nfoo\r\n$3\r\nbar\r\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), true);
    BOOST_REQUIRE_EQUAL(*parser.err(), "Protocol error: invalid bulk length");
    BOOST_REQUIRE_EQUAL(bool(cmd), false);
}

BOOST_AUTO_TEST_CASE(test_multi_bulk_normal_split_cmd) {
    redis_parser parser;
    parser.init();
    std::string s1 = "*3";
    std::string s2 = "\r\n$";
    std::string s3 = "3\r\nget\r\n$3\r\n";
    std::string s4 = "f";
    std::string s5 ="oo\r\n$3\r";
    std::string s6 = "\nbar\r\n";
    BOOST_REQUIRE_EQUAL(bool(test_str(parser, s1)), false);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(test_str(parser, s2)), false);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(test_str(parser, s3)), false);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(test_str(parser, s4)), false);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(test_str(parser, s5)), false);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    auto cmd = test_str(parser, s6);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv(0), "get");
    BOOST_REQUIRE_EQUAL(c.argv(1), "foo");
    BOOST_REQUIRE_EQUAL(c.argv(2), "bar");
    BOOST_REQUIRE_EQUAL(c.flow(), s1.size() + s2.size() + s3.size() + s4.size() + s5.size() + s6.size());
}

BOOST_AUTO_TEST_CASE(test_multi_bulk_normal_pipline) {
    redis_parser parser;
    parser.init();
    std::string s = "*3\r\n$3\r\nget\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*3\r\n$3\r\nget\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv(0), "get");
    BOOST_REQUIRE_EQUAL(c.argv(1), "foo");
    BOOST_REQUIRE_EQUAL(c.argv(2), "bar");
    BOOST_REQUIRE_EQUAL(c.flow(), s.size()/2);
}

BOOST_AUTO_TEST_CASE(test_inline_normal) {
    redis_parser parser;
    parser.init();
    std::string s = "set foo bar\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv(0), "set");
    BOOST_REQUIRE_EQUAL(c.argv(1), "foo");
    BOOST_REQUIRE_EQUAL(c.argv(2), "bar");
    BOOST_REQUIRE_EQUAL(c.flow(), s.size());
}

BOOST_AUTO_TEST_CASE(test_inline_normal_space) {
    redis_parser parser;
    parser.init();
    std::string s = "\r\nset foo bar\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv(0), "set");
    BOOST_REQUIRE_EQUAL(c.argv(1), "foo");
    BOOST_REQUIRE_EQUAL(c.argv(2), "bar");
    BOOST_REQUIRE_EQUAL(c.flow(), s.size());
}

BOOST_AUTO_TEST_CASE(test_inline_normal_single) {
    redis_parser parser;
    parser.init();
    std::string s = "ping\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv(0), "ping");
    BOOST_REQUIRE_EQUAL(c.flow(), s.size());
}

BOOST_AUTO_TEST_CASE(test_inline_normal_split_cmd) {
    redis_parser parser;
    parser.init();
    std::string s1 = "set";
    std::string s2 = " foo b";
    std::string s3 = "ar";
    std::string s4 = "\n";
    BOOST_REQUIRE_EQUAL(bool(test_str(parser, s1)), false);
    BOOST_REQUIRE_EQUAL(bool(test_str(parser, s2)), false);
    BOOST_REQUIRE_EQUAL(bool(test_str(parser, s3)), false);
    auto cmd = test_str(parser, s4);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv(0), "set");
    BOOST_REQUIRE_EQUAL(c.argv(1), "foo");
    BOOST_REQUIRE_EQUAL(c.argv(2), "bar");
    BOOST_REQUIRE_EQUAL(c.flow(), s1.size() + s2.size() + s3.size() + s4.size());
}

BOOST_AUTO_TEST_CASE(test_inline_normal_pipline) {
    redis_parser parser;
    parser.init();
    std::string s = "set foo bar\nset foo bar\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv(0), "set");
    BOOST_REQUIRE_EQUAL(c.argv(1), "foo");
    BOOST_REQUIRE_EQUAL(c.argv(2), "bar");
    BOOST_REQUIRE_EQUAL(c.flow(), s.size()/2);
}

BOOST_AUTO_TEST_CASE(test_inline_normal_single_quotes) {
    redis_parser parser;
    parser.init();
    std::string s = "set fo\'o\' bar\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv(0), "set");
    BOOST_REQUIRE_EQUAL(c.argv(1), "foo");
    BOOST_REQUIRE_EQUAL(c.argv(2), "bar");
    BOOST_REQUIRE_EQUAL(c.flow(), s.size());
}

BOOST_AUTO_TEST_CASE(test_inline_error_single_quotes_len_larger) {
    redis_parser parser;
    parser.init();
    std::string s = "set f\'oo\' bar\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), true);
    BOOST_REQUIRE_EQUAL(*parser.err(), "Protocol error: unbalanced quotes in request");
    BOOST_REQUIRE_EQUAL(bool(cmd), false);
}

BOOST_AUTO_TEST_CASE(test_inline_normal_single_quotes_sq) {
    redis_parser parser;
    parser.init();
    std::string s = "set foo\'\\\'\' bar\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv(0), "set");
    BOOST_REQUIRE_EQUAL(c.argv(1), "foo\'");
    BOOST_REQUIRE_EQUAL(c.argv(2), "bar");
    BOOST_REQUIRE_EQUAL(c.flow(), s.size());
}

BOOST_AUTO_TEST_CASE(test_inline_normal_quotes) {
    redis_parser parser;
    parser.init();
    std::string s = "set f\"oo\" bar\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv(0), "set");
    BOOST_REQUIRE_EQUAL(c.argv(1), "foo");
    BOOST_REQUIRE_EQUAL(c.argv(2), "bar");
    BOOST_REQUIRE_EQUAL(c.flow(), s.size());
}

BOOST_AUTO_TEST_CASE(test_inline_normal_quotes_unbalanced_number) {
    redis_parser parser;
    parser.init();
    std::string s = "set f\"\"\"oo\"\"\" bar\r\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), true);
    BOOST_REQUIRE_EQUAL(*parser.err(), "Protocol error: unbalanced quotes in request");
    BOOST_REQUIRE_EQUAL(bool(cmd), false);
}

BOOST_AUTO_TEST_CASE(test_inline_normal_quotes_slash) {
    redis_parser parser;
    parser.init();
    std::string s = "set fo\"\\o\" bar\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv(0), "set");
    BOOST_REQUIRE_EQUAL(c.argv(1), "foo");
    BOOST_REQUIRE_EQUAL(c.argv(2), "bar");
    BOOST_REQUIRE_EQUAL(c.flow(), s.size());
}

BOOST_AUTO_TEST_CASE(test_inline_normal_quotes_hex) {
    redis_parser parser;
    parser.init();
    std::string s = "set \"f\\x6fo\" bar\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), false);
    BOOST_REQUIRE_EQUAL(bool(cmd), true);
    auto& c = *cmd;
    BOOST_REQUIRE_EQUAL(c.argv(0), "set");
    BOOST_REQUIRE_EQUAL(c.argv(1), "foo");
    BOOST_REQUIRE_EQUAL(c.argv(2), "bar");
    BOOST_REQUIRE_EQUAL(c.flow(), s.size());
}

BOOST_AUTO_TEST_CASE(test_inline_error_quotes_hex_len_smaller) {
    redis_parser parser;
    parser.init();
    std::string s = "set fo\"\\xf\" bar\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), true);
    BOOST_REQUIRE_EQUAL(*parser.err(), "Protocol error: unbalanced quotes in request");
    BOOST_REQUIRE_EQUAL(bool(cmd), false);
}

BOOST_AUTO_TEST_CASE(test_inline_error_quotes_hex_larger) {
    redis_parser parser;
    parser.init();
    std::string s = "set fo\"\\x6g\" bar\n";
    auto cmd = test_str(parser, s);
    BOOST_REQUIRE_EQUAL(bool(parser.err()), true);
    BOOST_REQUIRE_EQUAL(*parser.err(), "Protocol error: unbalanced quotes in request");
    BOOST_REQUIRE_EQUAL(bool(cmd), false);
}