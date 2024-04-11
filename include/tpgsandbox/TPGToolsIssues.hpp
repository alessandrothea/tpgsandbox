/**
 * @file tpg_apps_issues.hpp
 * 
 * ERS issues for TPG applications
 *
 * This is part of the DUNE DAQ , copyright 2023.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef TPGSANDBOX_INCLUDE_TPGSANDBOX_TPGISSUES_HPP_
#define TPGSANDBOX_INCLUDE_TPGSANDBOX_TPGISSUES_HPP_


#include <ers/Issue.hpp>
#include <string>

namespace dunedaq {

ERS_DECLARE_ISSUE(tpgsandbox,
                  TPGAlgorithmInexistent,
                  "The selected algorithm does not exist: " << algorithm_selection << " . Check your command line options and select either SimpleThreshold or AbsRS",
                  ((std::string)algorithm_selection))

ERS_DECLARE_ISSUE(tpgsandbox,
                  FileInexistent,
                  "The selected input file does not exist. Input file: " << input_file_path << "  Check the path of the input file",
                  ((std::string)input_file_path))

ERS_DECLARE_ISSUE(tpgsandbox,
                  InvalidImplementation,
                  "The selected TPG algorithm implementation does not exist: " << input_file_path << "  Check your command line options and select either NAIVE or AVX",
                  ((std::string)input_file_path))

}



#endif // TPGSANDBOX_INCLUDE_TPGSANDBOX_TPGISSUES_HPP_
