

# This is a skeleton CMakeLists.txt file, auto-generated on
# Fri Mar 15 13:50:19 CET 2024.  The developer(s) of this package should delete
# this comment as well as adding dependent targets, packages,
# etc. specific to the package. For details on how to write a package,
# please see
# https://dune-daq-sw.readthedocs.io/en/latest/packages/daq-cmake/

cmake_minimum_required(VERSION 3.12)
project(tpgsandbox VERSION 0.0.0)

find_package(daq-cmake REQUIRED)
find_package(fdreadoutlibs REQUIRED)
find_package(trgdataformats REQUIRED)

daq_setup_environment()

find_package(Boost COMPONENTS unit_test_framework REQUIRED)

##############################################################################
# Extra options and tweaks
set(TPGTOOLS_USE_INTRINSICS ON)

if(${TPGTOOLS_USE_INTRINSICS})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2")
endif()

##############################################################################
# Main library
daq_add_library( *.cpp LINK_LIBRARIES fdreadoutlibs::fdreadoutlibs trgdataformats::trgdataformats )


# python bindings
daq_add_python_bindings(*.cpp LINK_LIBRARIES ${PROJECT_NAME} ) # Any additional libraries to link in beyond the main library not yet determined


##############################################################################

daq_install()

