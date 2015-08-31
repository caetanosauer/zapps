#include "dbinspect.h"

void DBInspect::setupOptions()
{
    po::options_description opt("DBInspect Options");
    opt.add_options()
        ("file,f", po::value<string>(&file)->required(),
         "DB file to be inspected")
        ;
    options.add(opt);
}

void DBInspect::run()
{
    ifstream in(file, std::ifstream::binary);
    // vid_t vid = vol->vid();

    generic_page page;

    // Volume header page can be just printed out
    in.seekg(0);
    in.read((char*) &page, sizeof(generic_page));
    cout << page << endl;

    shpid_t p = 1;
    while (in) {
        in.seekg(p * sizeof(generic_page));
        in.read((char*) &page, sizeof(generic_page));
        cout << "Page " << p
            << " LSN=" << page.lsn
            << endl;
        p++;
    }
}

