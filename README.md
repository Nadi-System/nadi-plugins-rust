# Rust Plugins for Nadi System

The plugins here are provided as an example on how to write user
plugins, as well as to showcase the capabilitities of the plugins on
the nadi system.

The examples are written in `Rust`, and are compiled into shared
libraries (`.so` in linux, `.dll` in windows and `.dynlib` in OSX).

Load [Nadi Core](https://github.com/Nadi-System/nadi_core) for data
types and the plugin structure. Use the macros from
`nadi_core::nadi_plugin` to export the given functions.

Although the examples are written in two ways, with and without using
the `nadi_plugin` macros, please use the macros for plugin development
as it'll make it easier and less error prone.

For example, look at the sample plugins about how to use the macros
and how to write plugin functions. The example `fancy-print` can be
looked at as an example to see how a complete plugin library without
the macros can be written. Please do not write plugins this way. If
you need to look at how the macros are generating the boilerplate use
`cargo expand` command.

Use the command `cargo build --release` to compile all the plugins
into dynamic library and then move them to a folder the nadi-system
reads the plugins from. Or load them manually from the program. You
can optionally use the command `strip` to decrease the size of shared
library produced by rust.
