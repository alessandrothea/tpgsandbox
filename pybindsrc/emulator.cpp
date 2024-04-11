/**
 * @file renameme.cpp
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

#include "tpgsandbox/TPGEmulator.hpp"

namespace py = pybind11;

namespace dunedaq::tpgsandbox::python {

void
register_emulator(py::module& m) {

  py::class_<tpgsandbox::TPGEmulator>(m, "TPGEmulator")
    .def("initialize", &tpgsandbox::TPGEmulator::initialize)
    .def("execute_tpg", &tpgsandbox::TPGEmulator::execute_tpg )
    ;

  py::class_<tpgsandbox::AVXTPGEmulator, tpgsandbox::TPGEmulator>(m, "AVXTPGEmulator")
    .def(py::init<const std::string &, const std::string &>())
  ;

  py::class_<tpgsandbox::NaiveTPGEmulator, tpgsandbox::TPGEmulator>(m, "NaiveTPGEmulator")
    .def(py::init<const std::string &, const std::string &>())
  ;

}

} // namespace dunedaq::tpgtools::python
