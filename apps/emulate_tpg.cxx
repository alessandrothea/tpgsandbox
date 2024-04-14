#include "CLI/App.hpp"
#include "CLI/Config.hpp"
#include "CLI/Formatter.hpp"

#include <fmt/core.h>
#include <fmt/format.h>

#include "hdf5libs/HDF5RawDataFile.hpp"
#include "trgdataformats/TriggerPrimitive.hpp"
#include "triggeralgs/TriggerActivityFactory.hpp"
#include "triggeralgs/TriggerCandidateFactory.hpp"
#include "triggeralgs/TriggerObjectOverlay.hpp"
#include "detchannelmaps/TPCChannelMap.hpp"

#include "tpgsandbox/TriggerRecordProcessor.hpp"
#include "tpgsandbox/TPGEmulator.hpp"

using namespace dunedaq;

//-----------------------------------------------------------------------------
int main(int argc, char const *argv[])
{

  CLI::App app{"tapipe"};
  // argv = app.ensure_utf8(argv);

  std::string input_file_path;
  app.add_option("-i", input_file_path, "Input TPStream file path")->required();
  
  std::string output_file_path;
  app.add_option("-o", output_file_path, "Output TPStream file path");

  std::string algorithm_name = "SimpleThreshold";
  app.add_option("-a", algorithm_name, "Detector Channel Map");
  

  std::string channel_map_name = "VDColdboxChannelMap";
  app.add_option("-m", channel_map_name, "Detector Channel Map");
  
  // std::string config_name;
  // app.add_option("-j", config_name, "Trigger Activity and Candidate config JSON to use.")->required();
  
  uint64_t skip_rec(0);
  app.add_option("-s", skip_rec, "Skip records");
  
  uint64_t num_rec(0);
  app.add_option("-n", num_rec, "Process records");

  bool quiet = false;
  app.add_flag("--quiet", quiet, "Quiet outputs.");
  CLI11_PARSE(app, argc, argv);


  if (!quiet)
    fmt::print("TPStream file: {}\n", input_file_path);

  tpgsandbox::TriggerRecordProcessor rp(input_file_path, output_file_path);

  // TP source id (subsystem)
  // auto tp_subsystem_requirement = daqdataformats::SourceID::Subsystem::kTrigger;

  auto channel_map = dunedaq::detchannelmaps::make_map(channel_map_name);


  // Generic filter hook
  std::function<bool(const std::unique_ptr<dunedaq::daqdataformats::Fragment>&)> fragment_filter;


  auto wibeth_filter = [&](const std::unique_ptr<dunedaq::daqdataformats::Fragment>& frag) -> bool {
    return (
      // Detector readout fragment
      (frag->get_element_id().subsystem == daqdataformats::SourceID::Subsystem::kDetectorReadout ) && 
      (frag->get_fragment_type() == daqdataformats::FragmentType::kWIBEth)
    );
  };

  fragment_filter = wibeth_filter;

  rp.set_processor([&]( daqdataformats::TriggerRecord& tr ) -> void {

    // Create a TP buffer
    std::vector<trgdataformats::TriggerPrimitive> tp_buffer;
    tp_buffer.reserve(128);

    const std::vector<std::unique_ptr<daqdataformats::Fragment>>& frags = tr.get_fragments_ref();
    fmt::print("The numbert of fragments: {}\n", frags.size());
    for( const auto& frag : frags ) {

      if ( fragment_filter && !fragment_filter(frag)) {
        fmt::print("Fragment {} {} discarded\n", frag->get_element_id().to_string(), fragment_type_to_string(frag->get_fragment_type()));
        continue;
      } 

      // This bit should be outside the loop
      if (!quiet)
        fmt::print("  Fragment id: {} [{}]\n", frag->get_element_id().to_string(), daqdataformats::fragment_type_to_string(frag->get_fragment_type()));

      // Check data size
      if (size_t r = frag->get_data_size() % sizeof(fddetdataformats::WIBEthFrame); r != 0 ) {
        fmt::print("ERROR: the frame payload size is not multiple of a WIBEthFrame size\n");
        continue;
      }

      auto emu = std::make_unique<tpgsandbox::AVXTPGEmulator>(algorithm_name, channel_map_name);
      emu->set_tpg_threshold(100);
      emu->initialize();



      size_t n_frames = frag->get_data_size() / sizeof(fddetdataformats::WIBEthFrame);
      fddetdataformats::WIBEthFrame* frames = reinterpret_cast<fddetdataformats::WIBEthFrame*>(frag->get_data());

      for( size_t i(0); i<n_frames; ++i) {
        auto tp_vec = emu->execute_tpg(frames+i);
        std::move(tp_vec.begin(), tp_vec.end(), std::back_inserter(tp_buffer)); 
      }
    }

    fmt::print("tp buffer {} {}\n", (void*)tp_buffer.data(), tp_buffer.size());

    // // Create a new TP fragment
    // std::unique_ptr<daqdataformats::Fragment> tp_frag = std::make_unique<daqdataformats::Fragment>(tp_buffer.data(), tp_buffer.size());

    // // Set header
    // daqdataformats::FragmentHeader frag_hdr = frag->get_header();

    // // Customise the source id (10000)
    // frag_hdr.element_id = daqdataformats::SourceID{daqdataformats::SourceID::Subsystem::kTrigger, 10000};

    // tp_frag->set_header_fields(frag_hdr);
    // tp_frag->set_type(daqdataformats::FragmentType::kTriggerPrimitive);

    // tr.add_fragment(std::move(tp_frag));
    // if (!quiet)
    //   fmt::print("New Fragment moved into the TR.\n");
    // //
    // // Frame Processing Ends
    // //

    auto hdr_data = tr.get_header_data();
    // fmt::print("{}\n", hdr_data);
    std::cout << hdr_data << std::endl;

  });

  rp.loop(num_rec, skip_rec);

  /* code */
  return 0;
}
