# Rust Plugins for [Nadi System](https://github.com/Nadi-System/nadi)

For C refer: [C plugin examples](https://github.com/Nadi-System/nadi-plugins-c)

The plugins here are provided as an example on how to write user
plugins, as well as to showcase the capabilitities of the plugins on
the nadi system.

The examples are written in `Rust`, and are compiled into shared
libraries (`.so` in linux, `.dll` in windows and `.dynlib` in OSX).

Although the examples are written in two ways, with and without using
the `nadi_plugin` macros, please use the macros for plugin development
as it'll make it easier and less error prone. Although the plugins
written without the macros will have more flexibility (like variable
number of arguments).

Here the example plugin `print_node` is written without the help of
`nadi_plugin` macros and others with it. You can look at the functions
in `print_node2` anf `print_node` for comparision of difference in
abilities.

Use the command `cargo build --release` to compile all the plugins
into dynamic library and then move them to a folder the nadi-system
reads the plugins from. Or load them manually from the program.

# Available functions



One example Function is shown below:
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
