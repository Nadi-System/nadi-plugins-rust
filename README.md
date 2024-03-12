# Rust Plugins for [Nadi System](https://github.com/Nadi-System/nadi)

For C refer: [C plugin examples](https://github.com/Nadi-System/nadi-plugins-c)

The plugins here are provided as an example on how to write user plugins, as well as to showcase the capabilitities of the plugins on the nadi system.

The examples are written in `Rust`, and are compiled into shared libraries (`.so` in linux, `.dll` in windows and `.dynlib` in OSX).

Use the command `cargo build --release` to compile all the plugins into dynamic library and then move them to a folder the nadi-system reads the plugins from. Or load them manually from the program.

# Available functions
## check_sf: Check negative streamflow
Args: 
1. timeseries name

This simple plugin checks if there are any negative values in a
timeseries and prints the count.

The function provided by the plugin can be called like this:

    check_sf("streamflow")

I ran this on 1555 stations on the Ohio Basin. And found there are
problems on 11 of them as shown in the output below.

    Calling "check_sf"
    stn-03121500.csv 8 Negative values in streamflow
    stn-03293510.csv 651 Negative values in streamflow
    stn-03294550.csv 2 Negative values in streamflow
    stn-03294570.csv 3 Negative values in streamflow
    stn-03307000.csv 1 Negative values in streamflow
    stn-03321500.csv 5 Negative values in streamflow
    stn-03322100.csv 6 Negative values in streamflow
    stn-03331224.csv 1 Negative values in streamflow
    stn-03335671.csv 1 Negative values in streamflow
    stn-03382500.csv 239 Negative values in streamflow
    stn-03430200.csv 31 Negative values in streamflow

  
A better plugin could show the errors more visually using graphs, or
interactive plots.

## print_name: Print names of the nodes with inputs and outputs
Args: None

## print_attrs: Print attributes of the nodes
Args: None
