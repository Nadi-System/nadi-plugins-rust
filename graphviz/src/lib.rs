use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod graphviz {
    use abi_stable::std_types::ROption::RSome;
    use nadi_core::attrs::HasAttributes;
    use nadi_core::functions::FunctionRet;
    use nadi_core::nadi_plugin::network_func;
    use nadi_core::string_template::Template;
    use nadi_core::{network::Network, return_on_err};
    use std::{fs::File, io::Write, path::Path};

    /// Save the network as a graphviz file
    /// # Arguments:
    /// - `outfile` - Path to the output file
    /// - `name` - Name of the graph
    #[network_func(name = "network", global_attrs = "")]
    fn save_graphviz(
        net: &mut Network,
        outfile: &Path,
        name: &str,
        global_attrs: &str,
        node_attr: Option<&Template>,
        edge_attr: Option<&Template>,
    ) -> FunctionRet {
        let mut file = return_on_err!(File::create(outfile));
        return_on_err!(writeln!(file, "digraph {} {{", name));
        return_on_err!(writeln!(file, "{}", global_attrs));
        for node in net.nodes() {
            let node = node.lock();
            if let Some(templ) = &node_attr {
                let attr = return_on_err!(node.render(templ));
                return_on_err!(writeln!(file, "\"{}\" [{attr}]", node.name()));
            }
            if let RSome(out) = node.output() {
                if let Some(templ) = &edge_attr {
                    let attr = return_on_err!(node.render(templ));
                    return_on_err!(writeln!(
                        file,
                        "\"{}\" -> \"{}\" [{attr}]",
                        node.name(),
                        out.lock().name()
                    ));
                } else {
                    return_on_err!(writeln!(
                        file,
                        "\"{}\" -> \"{}\"",
                        node.name(),
                        out.lock().name()
                    ));
                }
            }
        }
        return_on_err!(writeln!(file, "}}"));
        FunctionRet::None
    }
}
