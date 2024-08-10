use std::ffi::{c_char, CString};

use nadi_core::attributes::AsValue;
use nadi_core::plugins::FunctionCtx;
use nadi_core::{Network, NodeInner};

// Plugins should provide "node_functions" and "network_functions"
// functions that return comma separated list of functions to load.
// Use available macros from nadi_plugin and follow the examples there
// for easier plugin development which'll generate these functions for
// you.

#[no_mangle]
extern "C" fn node_functions() -> *const c_char {
    CString::new("print_name,print_attrs").unwrap().into_raw()
}

#[no_mangle]
extern "C" fn network_functions() -> *const c_char {
    CString::new("print_attr_csv").unwrap().into_raw()
}

#[no_mangle]
extern "C" fn print_name(node: &mut NodeInner, _ctx: &mut FunctionCtx) {
    let inputs: String = node
        .inputs()
        .map(|i| i.borrow().name().to_string())
        .collect::<Vec<String>>()
        .join(",");
    if !inputs.is_empty() {
        print!("{{ {} }} -> ", inputs);
    }
    print!("{}", node.name());
    if let Some(out) = node.output() {
        println!(" -> {}", out.borrow().name())
    } else {
        println!()
    }
}

#[no_mangle]
extern "C" fn print_name_help() -> *const c_char {
    CString::new("Print the node with its inputs and outputs")
        .unwrap()
        .into_raw()
}

#[no_mangle]
extern "C" fn print_attrs(node: &mut NodeInner, _ctx: &mut FunctionCtx) {
    println!("* NODE:: {}\n* ATTRS::", node.name());
    node.print_attrs();
    print!("* TIMESERIES::");
    println!("{}", node.list_timeseries().join(","));
}

#[no_mangle]
extern "C" fn print_attrs_help() -> *const c_char {
    CString::new("Print the node with its attributes")
        .unwrap()
        .into_raw()
}

#[no_mangle]
extern "C" fn print_attr_csv(net: &mut Network, ctx: &mut FunctionCtx) {
    let n = ctx.args_count();
    let mut attrs = Vec::with_capacity(n);
    let args_n: Vec<String> = (0..n)
        .map(|i| ctx.arg(i).cloned().into_string().unwrap())
        .collect();
    println!("name,{}", args_n.join(","));
    for node in net.nodes() {
        let node = node.borrow();
        for i in 0..n {
            let var = ctx.arg(i).cloned().into_string().unwrap();
            attrs.push(
                node.attr(&var)
                    .map(|v| v.to_string())
                    .unwrap_or("".to_string()),
            );
        }
        println!("{},{}", node.name(), attrs.join(","));
    }
}

#[no_mangle]
extern "C" fn print_attr_csv_help() -> *const c_char {
    CString::new("Print the given attributes in csv format with first column with node name")
        .unwrap()
        .into_raw()
}
