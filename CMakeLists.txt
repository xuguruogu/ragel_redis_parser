cmake_minimum_required (VERSION 3.5)

project (redis_parser CXX)
list (APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR})

find_package (ragel 6.10 REQUIRED)

function (generate_ragel)
    set (one_value_args TARGET VAR IN_FILE OUT_FILE)
    cmake_parse_arguments (args "" "${one_value_args}" "" ${ARGN})
    get_filename_component (out_dir ${args_OUT_FILE} DIRECTORY)

    add_custom_command (
            DEPENDS ${args_IN_FILE}
            OUTPUT ${args_OUT_FILE}
            COMMAND ${CMAKE_COMMAND} -E make_directory ${out_dir}
            COMMAND ${ragel_RAGEL_EXECUTABLE} -G2 -o ${args_OUT_FILE} ${args_IN_FILE}
            COMMAND sed -i -e "'1h;2,$$H;$$!d;g'" -re "'s/static const char _nfa[^;]*;//g'" ${args_OUT_FILE})

    add_custom_target (${args_TARGET}
            DEPENDS ${args_OUT_FILE})

    set (${args_VAR} ${args_OUT_FILE} PARENT_SCOPE)
endfunction ()

generate_ragel(
        TARGET redis_parser
        VAR redis_parser_file
        IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/redis_parser.rl
        OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/redis_parser.hh)

add_executable(redis_parser_test
        ${redis_parser_file}
        redis_parser_test.cc)

target_include_directories (redis_parser_test
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
        PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
