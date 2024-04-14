#ifndef __TPGSANDBOX_TRIGGERPROCESSOR_HPP__
#define __TPGSANDBOX_TRIGGERPROCESSOR_HPP__

#include "hdf5libs/HDF5RawDataFile.hpp"
#include <fmt/core.h>
#include <fmt/format.h>

namespace dunedaq {
namespace tpgsandbox {

class TriggerRecordProcessor
{
private:
  /* data */
  std::unique_ptr<hdf5libs::HDF5RawDataFile> m_input_file;
  std::unique_ptr<hdf5libs::HDF5RawDataFile> m_output_file;

  void open_files(std::string input_path, std::string output_path);
  void close_files();

  void process( daqdataformats::TriggerRecord& tls );

  // Can modify?
  std::function<void(daqdataformats::TriggerRecord&)> m_processor; 

public:

  
  TriggerRecordProcessor(std::string input_path, std::string output_path);
  ~TriggerRecordProcessor();

  void set_processor(std::function<void(daqdataformats::TriggerRecord&)> processor);
  void loop(uint64_t num_records = 0, uint64_t offset = 0, bool quiet = false);

};

} // namespace tpgsandbox
} // namespace dunedaq

#endif /* __TPGSANDBOX_TRIGGERPROCESSOR_HPP__ */
