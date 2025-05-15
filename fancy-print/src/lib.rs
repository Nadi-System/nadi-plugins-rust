/// Nadi plugin that prints the network with some colors. This plugin
/// is written showing all the boilerplate that macros from
/// nadi_plugin will generate for you.
///
/// DO NOT WRITE PLUGINS like this, use the macros to generate the
/// boilerplace while you only have to write the code logic of the
/// plugin functions. Which will ensure:
/// - all functions are registered correctly,
/// - all the boilerplace code are correctly written,
/// - the shared library exports the root module correctly so nadi system can read it,
/// - it is easier to maintain and upgrade on nadi_core version change.
use abi_stable::{
    export_root_module,
    prefix_type::PrefixTypeTrait,
    sabi_extern_fn,
    sabi_trait::prelude::TD_Opaque,
    std_types::{ROption::RSome, RString, RVec},
};
use colored::Colorize;
use nadi_core::{
    functions::{
        FuncArg, FunctionCtx, FunctionRet, NadiFunctions, NetworkFunction, NetworkFunction_TO,
    },
    network::Network,
    plugins::{NadiExternalPlugin, NadiExternalPlugin_Ref},
};

#[export_root_module]
pub fn get_library() -> NadiExternalPlugin_Ref {
    NadiExternalPlugin {
        register_functions,
        plugin_name,
    }
    .leak_into_prefix()
}

#[sabi_extern_fn]
fn plugin_name() -> RString {
    "fancy_print".into()
}

#[sabi_extern_fn]
fn register_functions(funcs: &mut NadiFunctions) {
    funcs.register_network_function(
        "fancy_print",
        NetworkFunction_TO::from_value(FancyPrint, TD_Opaque),
    )
}

#[derive(Debug, Clone)]
pub struct FancyPrint;

impl NetworkFunction for FancyPrint {
    fn name(&self) -> RString {
        "fancy_print".into()
    }

    fn help(&self) -> RString {
        "Fancy print a network
"
        .into()
    }

    fn args(&self) -> RVec<FuncArg> {
        vec![].into()
    }

    fn code(&self) -> RString where {
        "
        for node in network.nodes() {
            let n = node.lock();
            print!(\"[{}] {}\", n.index(), n.name().blue());
            if let RSome(o) = n.output() {
                println!(\" {} {}\", \"->\".red(), o.lock().name().yellow());
            } else {
                println!();
            }
        }
        ROk(())
"
        .into()
    }

    fn call(&self, network: &Network, _ctx: &FunctionCtx) -> FunctionRet {
        for node in network.nodes() {
            let n = node.lock();
            print!("[{}] {}", n.index(), n.name().blue());
            if let RSome(o) = n.output() {
                println!(" {} {}", "->".red(), o.lock().name().yellow());
            } else {
                println!();
            }
        }
        FunctionRet::None
    }
}
