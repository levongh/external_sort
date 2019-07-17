cmake_minimum_required (VERSION 2.6)
project (ExternalSort)
add_executable(ExternalSort main.cpp)

TARGET_LINK_LIBRARIES(ExternalSort pthread)
