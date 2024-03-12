use std::ffi::{c_char, CString};

use nadi_core::attributes::AsValue;
use nadi_core::node::NodeInner;
use nadi_core::plugins::FunctionCtx;
use nadi_core::timeseries::TimeSeries;

// Plugins should provide "node_functions" and "network_functions"
// functions that return comma separated list of functions to load

#[no_mangle]
extern "C" fn node_functions() -> *const c_char {
    CString::new("check_sf").unwrap().into_raw()
}

#[no_mangle]
extern "C" fn network_functions() -> *const c_char {
    CString::new("").unwrap().into_raw()
}

#[no_mangle]
extern "C" fn check_sf(node: &mut NodeInner, ctx: &mut FunctionCtx) {
    if let Some(ts) = ctx.arg(0).cloned().into_string() {
        match node.timeseries(&ts) {
            Ok(ts) => {
                if let Some(vals) = ts.values_float() {
                    let negs = vals.into_iter().filter(|v| **v < 0.0).count();
                    if negs > 0 {
                        println!("{} {negs} Negative values in streamflow", node.name());
                    }
                } else {
                    ctx.set_error(anyhow::Error::msg(
                        "Timeseries named {ts} is not float values",
                    ));
                }
            }
            Err(e) => {
                ctx.set_error(e);
            }
        }
    }
}

#[no_mangle]
extern "C" fn check_sf_help() -> *const c_char {
    CString::new("Checks the given streamflow timeseries for negative values")
        .unwrap()
        .into_raw()
}
