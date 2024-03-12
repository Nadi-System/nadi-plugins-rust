use std::ffi::{c_char, CString};

use nadi_core::node::NodeInner;
use nadi_core::plugins::FunctionCtx;

// Plugins should provide "node_functions" and "network_functions"
// functions that return comma separated list of functions to load

#[no_mangle]
extern "C" fn node_functions() -> *const c_char {
    CString::new("print_name,print_attrs").unwrap().into_raw()
}

#[no_mangle]
extern "C" fn network_functions() -> *const c_char {
    CString::new("").unwrap().into_raw()
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
