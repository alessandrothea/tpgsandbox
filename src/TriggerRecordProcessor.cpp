#include "tpgsandbox/TriggerRecordProcessor.hpp"

#include <filesystem>

namespace dunedaq {
namespace tpgsandbox {

//-----------------------------------------------------------------------------
TriggerRecordProcessor::TriggerRecordProcessor(std::string input_path, std::string output_path) 
{
  this->open_files(input_path, output_path);
}

//-----------------------------------------------------------------------------
TriggerRecordProcessor::~TriggerRecordProcessor()
{
  this->close_files();
}

//-----------------------------------------------------------------------------
void
TriggerRecordProcessor::open_files(std::string input_path, std::string output_path) {

  if (!std::filesystem::exists(input_path)) {
    throw std::runtime_error(fmt::format("ERROR: input file '{}' does not exist'", input_path));
  }

  // Open input file
  m_input_file = std::make_unique<hdf5libs::HDF5RawDataFile>(input_path);

  if (!m_input_file->is_trigger_record_type()) {
    fmt::print("ERROR: input file '{}' not of type 'TriggerRecord'\n", input_path);
    throw std::runtime_error(fmt::format("ERROR: input file '{}' not of type 'TriggerRecord'", input_path));
  }

  auto run_number = m_input_file->get_attribute<daqdataformats::run_number_t>("run_number");
  auto file_index = m_input_file->get_attribute<size_t>("file_index");
  auto application_name = m_input_file->get_attribute<std::string>("application_name");

  fmt::print("Run Number: {}\nFile Index: {}\nApp name: '{}'\n", run_number, file_index, application_name);

  if (!output_path.empty()) {
    if (std::filesystem::exists(output_path)) {
      fmt::print("Output file {} exists  - removing\n", output_path);
      std::filesystem::remove(output_path);
    }

    auto writing_file = output_path+".writing";
    if (std::filesystem::exists(writing_file)) {
      fmt::print("Output temp file file {} exists  - removing\n", writing_file);
      std::filesystem::remove(writing_file);
    }


    // Open output file
    m_output_file = std::make_unique<hdf5libs::HDF5RawDataFile>(
      output_path,
      m_input_file->get_attribute<daqdataformats::run_number_t>("run_number"),
      m_input_file->get_attribute<size_t>("file_index"),
      m_input_file->get_attribute<std::string>("application_name"),
      m_input_file->get_file_layout().get_file_layout_params(),
      m_input_file->get_srcid_geoid_map()
    );
  }
}

//-----------------------------------------------------------------------------
void
TriggerRecordProcessor::close_files() {
  // Do something?
}

//-----------------------------------------------------------------------------
void
TriggerRecordProcessor::set_processor(std::function<void(daqdataformats::TriggerRecord& )> processor) {
  m_processor = processor;
}

//-----------------------------------------------------------------------------
void
TriggerRecordProcessor::process( daqdataformats::TriggerRecord& tls ) {
  if (m_processor)
    m_processor(tls);
}

//-----------------------------------------------------------------------------
void
TriggerRecordProcessor::loop(uint64_t num_records, uint64_t offset, bool quiet) {

  // Replace with a record selection?
  auto records = m_input_file->get_all_record_ids();

  if (!num_records) {
    num_records = (records.size()-offset);
  }

  uint64_t first_rec = offset, last_rec = offset+num_records;

  uint64_t i_rec(0);
  for( const auto& rid : records ) {

    if (i_rec < first_rec || i_rec >= last_rec ) {
      ++i_rec;
      continue;
    }

    if (!quiet)
      fmt::print("\n-- Processing TSL {}:{}\n\n", rid.first, rid.second);
    // auto tsl = m_input_file->get_timeslice(rid);
    auto tr = m_input_file->get_trigger_record(rid);
    // Or filter on a selection here using a lambda?

    // if (!quiet)
      // fmt::print("TSL number {}\n", tsl.get_header().timeslice_number);

    // Add a process method
    this->process(tr);

    if (m_output_file)
      m_output_file->write(tr);

    ++i_rec;
    fmt::print("\n-- Finished TR {}:{}\n\n", rid.first, rid.second);

  }

}

} // namespace tpgsandbox
} // namespace dunedaq