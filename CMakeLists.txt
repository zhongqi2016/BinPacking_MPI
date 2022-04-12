cmake_minimum_required(VERSION 3.17)
project(BinPacking_MPI)

find_package(MPI)
include_directories( ${MPI_INCLUDE_PATH})


set(CMAKE_CXX_STANDARD 20)

add_executable(BinPacking_MPI main.cpp BinPacking.cpp BinPacking.h Item.cpp Item.h Branch.cpp Branch.h ReadFiles.cpp ReadFiles.h)
target_link_libraries(BinPacking_MPI ${MPI_LIBRARIES})
